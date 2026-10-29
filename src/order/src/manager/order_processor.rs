// SPDX-License-Identifier: GPL-3.0-only
//! At-most-once submission; uncertain outcomes can only enter reconciliation.
use super::{
    order_events::OrderEventWriter,
    order_reservation::OrderReservation,
    risk::{OrderRiskRejection, RiskPolicy},
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use sigbot_core::modules::order::store::postgres::OrderPostgresRepository;
use sigbot_exchange::client::execution_adapter::{IExecutionAdapter, PreparedSubmission};
use sigbot_types::modules::{
    messager::{envelope::DurableMessage, topic::MessageScope},
    order::{
        execution::{OrderReceipt, OrderState},
        intent::TradeIntent,
    },
    wallet::reservation::ReservationAction,
};
use std::sync::Arc;

pub struct OrderProcessor {
    repository: OrderPostgresRepository,
    reservation: Arc<OrderReservation>,
    adapter: Arc<dyn IExecutionAdapter>,
    policy: RiskPolicy,
    node_id: i64,
    scope: MessageScope,
    workflow_id: i64,
}

impl OrderProcessor {
    pub fn new(
        repository: OrderPostgresRepository,
        reservation: Arc<OrderReservation>,
        adapter: Arc<dyn IExecutionAdapter>,
        policy: RiskPolicy,
        node_id: i64,
        scope: MessageScope,
        workflow_id: i64,
    ) -> Result<Self> {
        ensure!(node_id > 0 && workflow_id > 0, "order workflow/node identity required");
        scope.validate()?;
        adapter.validate_instrument(&policy.instrument)?;
        Ok(Self {
            repository,
            reservation,
            adapter,
            policy,
            node_id,
            scope,
            workflow_id,
        })
    }

    pub async fn process(&self, message: &DurableMessage<TradeIntent>, now: DateTime<Utc>) -> Result<OrderState> {
        message.validate()?;
        ensure!(
            message.scope == self.scope && message.workflow_id == Some(self.workflow_id),
            "order tenant/environment/space/workflow mismatch"
        );
        ensure!(
            message.account_id == Some(message.payload.account_id),
            "intent account mismatch"
        );
        let mut existing = self.repository.find(message).await?;
        match existing.as_deref() {
            Some("submitting" | "unknown") => return self.reconcile(message, now).await,
            Some("acknowledged" | "filled" | "partially_filled" | "cancelled") => return Ok(OrderState::Acknowledged),
            Some("rejected") => return Ok(OrderState::Rejected),
            _ => {}
        }
        if existing.is_none() {
            let mut tx = self.repository.pool().begin().await?;
            let exposure = OrderPostgresRepository::exposure(&mut tx, message).await?;
            // Another caller may have accepted this identity while we waited for
            // the wallet lock. Do not count that accepted intent against itself.
            existing = OrderPostgresRepository::find_in(&mut tx, message).await?;
            if existing.is_none() {
                let reservation = self
                    .policy
                    .check(
                        &message.payload,
                        now,
                        exposure.position,
                        exposure.pending_quantity,
                        exposure
                            .position
                            .abs()
                            .checked_mul(message.payload.market.price)
                            .and_then(|value| value.checked_add(exposure.pending_notional))
                            .context("portfolio valuation overflow")?,
                    )
                    .map_err(|error| OrderRiskRejection(error.to_string()))?;
                OrderPostgresRepository::insert(&mut tx, message, &reservation.0, reservation.1).await?;
            }
            tx.commit().await?;
        } else {
            // A restart after risk acceptance resumes the same terms; fresh market evidence
            // is still required before the first submission.
            if let Err(error) = message.payload.validate(now, self.policy.limits.max_market_age_ms) {
                self.persist(message, OrderState::Rejected, None, now).await?;
                tracing::warn!(run_id=%message.run_id,%error,"order rejected because accepted market evidence expired");
                return Ok(OrderState::Rejected);
            }
        }
        match existing.as_deref() {
            Some("submitting" | "unknown") => return self.reconcile(message, now).await,
            Some("acknowledged" | "filled" | "partially_filled" | "cancelled") => return Ok(OrderState::Acknowledged),
            Some("rejected") => return Ok(OrderState::Rejected),
            _ => {}
        }
        if self.repository.find(message).await?.as_deref() != Some("reserved") {
            let (asset, amount) = self.repository.reservation(message).await?;
            let mut tx = self.repository.pool().begin().await?;
            self.reservation
                .request(&mut tx, message, ReservationAction::Reserve { asset, amount })
                .await?;
            tx.commit().await?;
            return Ok(OrderState::Pending);
        }
        let prepared = match self.repository.prepared(message).await? {
            Some(value) => Some(serde_json::from_value::<PreparedSubmission>(value)?),
            None => match self.adapter.prepare(message).await? {
                Some(candidate) => Some(serde_json::from_value::<PreparedSubmission>(
                    self.repository
                        .persist_prepared(message, &serde_json::to_value(candidate)?)
                        .await?,
                )?),
                None => None,
            },
        };
        if !self.repository.claim_submission(message).await? {
            return self.reconcile(message, now).await;
        }
        // Network I/O is outside the database transaction. A crash leaves submitting,
        // which is reconciled on redelivery and is never submitted again.
        let exchange_span = MessageTrace::child("order.exchange.submit", &message.traceparent);
        let mut request = message.clone();
        request.traceparent = exchange_span.traceparent();
        let outcome = match prepared {
            Some(prepared) => self.adapter.submit_prepared(&request, &prepared).await,
            None => self.adapter.submit(&request).await,
        };
        match outcome {
            Ok(receipt) => self.accept_receipt(message, receipt, now).await,
            Err(error) => {
                self.persist(message, OrderState::Unknown, None, now).await?;
                Err(error.context("order outcome unknown; funds stay reserved until reconciliation"))
            }
        }
    }

    pub async fn reconcile(&self, message: &DurableMessage<TradeIntent>, now: DateTime<Utc>) -> Result<OrderState> {
        message.validate()?;
        ensure!(
            message.scope == self.scope && message.workflow_id == Some(self.workflow_id),
            "order reconciliation workflow mismatch"
        );
        let exchange_span = MessageTrace::child("order.exchange.reconcile", &message.traceparent);
        let mut request = message.clone();
        request.traceparent = exchange_span.traceparent();
        let receipt = match self.repository.prepared(message).await? {
            Some(prepared) => {
                self.adapter
                    .reconcile_prepared(&request, &serde_json::from_value(prepared)?)
                    .await?
            }
            None => self.adapter.reconcile(&request).await?,
        };
        match receipt {
            Some(receipt) => self.accept_receipt(message, receipt, now).await,
            None => {
                self.persist(message, OrderState::Unknown, None, now).await?;
                Ok(OrderState::Unknown)
            }
        }
    }

    async fn accept_receipt(
        &self,
        message: &DurableMessage<TradeIntent>,
        receipt: OrderReceipt,
        now: DateTime<Utc>,
    ) -> Result<OrderState> {
        ensure!(!receipt.exchange_order_id.is_empty(), "venue receipt lacks an order ID");
        self.persist(message, receipt.state, Some(receipt.exchange_order_id), now)
            .await?;
        // Filled/partial receipts are still not execution facts. Wallet only consumes
        // the separate execution stream carrying execution price, quantity and fees.
        Ok(receipt.state)
    }

    async fn persist(
        &self,
        input: &DurableMessage<TradeIntent>,
        state: OrderState,
        exchange_order_id: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self.repository.pool().begin().await?;
        if !OrderPostgresRepository::record(&mut tx, input, state.as_str(), exchange_order_id.as_deref()).await? {
            tx.commit().await?;
            return Ok(());
        }
        OrderEventWriter::enqueue(
            &mut tx,
            &self.producer(),
            self.node_id,
            input,
            state,
            exchange_order_id.as_deref(),
            now,
        )
        .await?;
        if state == OrderState::Rejected || state == OrderState::Cancelled {
            self.reservation
                .request(&mut tx, input, ReservationAction::Release)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub fn producer(&self) -> String {
        OrderEventWriter::owner(&self.scope, self.workflow_id, self.node_id)
    }
}
