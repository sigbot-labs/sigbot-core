// SPDX-License-Identifier: GPL-3.0-only
//! At-most-once submission; uncertain outcomes can only enter reconciliation.
use crate::execution::events::OrderEventWriter;
use crate::processing::reservation::OrderReservation;
use crate::processing::risk::OrderRiskRejection;
use crate::processing::risk::RiskPolicy;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use sigbot_core::modules::order::handler::order::OrderRecordHandler;
use sigbot_exchange::exchange::IExecutionAdapter;
use sigbot_exchange::exchange::PreparedSubmission;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::OrderReceipt;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::wallet::reservation::ReservationAction;
use sigbot_types::Uuid;
use std::sync::Arc;

pub struct OrderProcessor {
    handler: OrderRecordHandler,
    reservation: Arc<OrderReservation>,
    adapter: Arc<dyn IExecutionAdapter>,
    policy: RiskPolicy,
    node_id: i64,
    scope: MessageScope,
    workflow_id: Uuid,
}

impl OrderProcessor {
    pub fn new(
        handler: OrderRecordHandler,
        reservation: Arc<OrderReservation>,
        adapter: Arc<dyn IExecutionAdapter>,
        policy: RiskPolicy,
        node_id: i64,
        scope: MessageScope,
        workflow_id: Uuid,
    ) -> Result<Self> {
        ensure!(
            node_id > 0 && workflow_id.get_version_num() == 7,
            "order workflow/node identity required"
        );
        scope.validate()?;
        adapter.validate_instrument(&policy.instrument)?;
        Ok(Self {
            handler,
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
            "order tenant/environment/workflow mismatch"
        );
        ensure!(
            message.account_id == Some(message.payload.account_id),
            "intent account mismatch"
        );
        let mut existing = self.handler.find(message).await?;
        if existing.is_some() {
            self.require_mode(message).await?;
        }

        match existing.as_deref() {
            Some("submitting" | "unknown") => return self.reconcile(message, now).await,
            Some("acknowledged" | "filled" | "partially_filled" | "cancelled") => return Ok(OrderState::Acknowledged),
            Some("rejected") => return Ok(OrderState::Rejected),
            _ => {}
        }
        if existing.is_none() {
            existing = self
                .handler
                .accept_with(message, |exposure| {
                    RiskPolicy::check_mode(exposure.mode, self.adapter.mode())?;
                    self.policy
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
                        .map_err(|error| OrderRiskRejection(error.to_string()).into())
                })
                .await?;
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
        if self.handler.find(message).await?.as_deref() != Some("reserved") {
            let (asset, amount) = self.handler.reservation(message).await?;
            self.reservation
                .request(message, ReservationAction::Reserve { asset, amount })
                .await?;
            return Ok(OrderState::Pending);
        }
        let prepared = match self.handler.prepared(message).await? {
            Some(value) => Some(serde_json::from_value::<PreparedSubmission>(value)?),
            None => match self.adapter.prepare(message).await? {
                Some(candidate) => Some(serde_json::from_value::<PreparedSubmission>(
                    self.handler
                        .persist_prepared(message, &serde_json::to_value(candidate)?)
                        .await?,
                )?),
                None => None,
            },
        };
        if !self.handler.claim_submission(message).await? {
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
        self.require_mode(message).await?;
        let exchange_span = MessageTrace::child("order.exchange.reconcile", &message.traceparent);
        let mut request = message.clone();
        request.traceparent = exchange_span.traceparent();
        let receipt = match self.handler.prepared(message).await? {
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

    async fn require_mode(&self, message: &DurableMessage<TradeIntent>) -> Result<()> {
        RiskPolicy::check_mode(self.handler.wallet_mode(message).await?, self.adapter.mode())
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
        self.handler
            .persist_transition(
                &self.reservation,
                &self.producer(),
                self.node_id,
                input,
                state,
                exchange_order_id.as_deref(),
                now,
            )
            .await?;
        Ok(())
    }

    pub fn producer(&self) -> String {
        OrderEventWriter::owner(&self.scope, self.workflow_id, self.node_id)
    }
}
