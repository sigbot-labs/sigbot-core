// SPDX-License-Identifier: GPL-3.0-only
//! Read venue facts after acceptance. Durable cursors never depend on broker delivery.
use crate::execution::events::OrderEventWriter;
use crate::execution::tracker::OrderExecutionTracker;
use crate::processing::risk::RiskPolicy;
use anyhow::ensure;
use anyhow::Result;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use sigbot_core::modules::order::handler::order::{
    OrderHandler, OrderRecordHandler, OrderVenueHandler, VenueObservation, VenuePoll,
};
use sigbot_exchange::exchange::IExecutionAdapter;
use sigbot_exchange::exchange::PreparedSubmission;
use sigbot_messager::messager::IMessager;
use sigbot_types::exchange::instrument::InstrumentSpec;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::execution::OrderStatusFact;
use sigbot_types::Uuid;
use std::sync::Arc;

pub struct VenueExecutionPoller {
    handler: OrderVenueHandler,
    orders: OrderRecordHandler,
    scope: MessageScope,
    workflow_id: Uuid,
    account_id: Uuid,
    node_id: i64,
    adapter: Arc<dyn IExecutionAdapter>,
    instrument: InstrumentSpec,
}
impl VenueExecutionPoller {
    pub fn new(
        storage: OrderHandler,
        scope: MessageScope,
        workflow_id: Uuid,
        account_id: Uuid,
        node_id: i64,
        consumer: String,
        adapter: Arc<dyn IExecutionAdapter>,
        instrument: InstrumentSpec,
    ) -> Self {
        let handler = OrderVenueHandler::new(
            storage.clone(),
            scope.clone(),
            workflow_id,
            account_id,
            node_id,
            consumer.clone(),
        );
        let orders = OrderRecordHandler::new(&storage);
        Self {
            handler,
            orders,
            scope,
            workflow_id,
            account_id,
            node_id,
            adapter,
            instrument,
        }
    }

    pub async fn tick(&self, tracker: &OrderExecutionTracker, messager: &dyn IMessager) -> Result<()> {
        self.drain(tracker).await?;
        let result = self.poll().await;
        sigbot_messager::messager::OutboxRelay::dispatch_pending(
            &self.handler.messager(),
            self.handler.producer(),
            messager,
            32,
            &|parent| Ok(MessageTrace::child("order.venue.retry", parent).traceparent()),
        )
        .await?;
        result
    }

    pub async fn poll(&self) -> Result<()> {
        let Some(poll) = self.handler.claim().await? else {
            return Ok(());
        };
        let wallet_mode = self.orders.wallet_mode(&poll.input).await?;
        if let Err(error) = RiskPolicy::check_mode(wallet_mode, self.adapter.mode()) {
            self.handler.retry(&poll, Utc::now() + Duration::seconds(10)).await?;
            return Err(error);
        }
        let prepared: Option<PreparedSubmission> = poll.prepared.clone().map(serde_json::from_value).transpose()?;
        let page = match self
            .adapter
            .read_executions(
                &poll.input,
                &poll.receipt,
                prepared.as_ref(),
                &self.instrument,
                poll.cursor.as_deref(),
                1000,
            )
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.handler.retry(&poll, Utc::now() + Duration::seconds(10)).await?;
                return Err(error);
            }
        };
        ensure!(page.facts.len() <= 1000, "venue execution page exceeds requested bound");
        ensure!(
            !page.has_more || (page.next_cursor.is_some() && page.next_cursor != poll.cursor),
            "venue page did not advance its cursor"
        );
        let observed = Utc::now();
        let mut observations = Vec::new();
        for fact in &page.facts {
            self.validate_fact(&poll, fact, observed)?;
            let payload = serde_json::to_value(fact)?;
            observations.push(VenueObservation {
                kind: "execution",
                reference: fact.exchange_trade_id.clone(),
                topic: AccountTopic::ExecutionEvent,
                event_time: fact.executed_at,
                available_time: observed,
                payload,
            });
        }
        if let Some(status) = &page.status {
            status.validate()?;
            ensure!(
                status.receipt.state != OrderState::Filled || status.executed_quantity == poll.input.payload.quantity,
                "filled venue status lacks the full intended execution quantity"
            );
            ensure!(
                status.wallet_id == poll.input.payload.wallet_id
                    && status.account_id == self.account_id
                    && status.exchange == self.instrument.exchange
                    && status.receipt.exchange_order_id == poll.receipt.exchange_order_id
                    && status.reservation_node_id == poll.input.node_id
                    && status.reservation_seq == poll.input.seq
                    && status.executed_quantity <= poll.input.payload.quantity
                    && status.occurred_at <= observed,
                "venue status differs from the owned order"
            );
            let reference = format!(
                "{}:{}:{}",
                status.receipt.exchange_order_id,
                status.receipt.state.as_str(),
                status.executed_quantity.normalize()
            );
            let payload = serde_json::to_value(status)?;
            observations.push(VenueObservation {
                kind: "status",
                reference,
                topic: AccountTopic::OrderEvent,
                event_time: status.occurred_at,
                available_time: observed,
                payload,
            });
        }
        let terminal = page
            .status
            .as_ref()
            .map(|status| (status.receipt.state, status.executed_quantity))
            .or_else(|| {
                (poll.receipt.state == OrderState::Filled).then_some((OrderState::Filled, poll.input.payload.quantity))
            });
        self.handler
            .persist(
                &poll,
                observations,
                page.next_cursor.as_deref(),
                page.has_more,
                terminal,
            )
            .await
    }

    fn validate_fact(&self, poll: &VenuePoll, fact: &ExecutionFact, observed: DateTime<Utc>) -> Result<()> {
        fact.validate()?;
        ensure!(
            fact.wallet_id == poll.input.payload.wallet_id
                && fact.account_id == self.account_id
                && fact.exchange == self.instrument.exchange
                && fact.symbol == self.instrument.symbol
                && fact.base_asset == self.instrument.base_asset
                && fact.quote_asset == self.instrument.quote_asset
                && fact.side == poll.input.payload.side
                && fact.exchange_order_id == poll.receipt.exchange_order_id
                && fact.reservation_node_id == poll.input.node_id
                && fact.reservation_seq == poll.input.seq
                && fact.executed_at <= observed
                && !fact.order_complete,
            "venue execution differs from configured instrument or owned order"
        );
        Ok(())
    }

    /// The host consumes its durable source partition directly; subscribing to
    /// its own derived order events would create a feedback loop.
    async fn drain(&self, tracker: &OrderExecutionTracker) -> Result<()> {
        let rows = self
            .handler
            .pending_publications(
                &OrderEventWriter::fact_owner("execution", &self.scope, self.workflow_id, self.node_id),
                &OrderEventWriter::fact_owner("status", &self.scope, self.workflow_id, self.node_id),
            )
            .await?;
        for row in rows {
            if row.topic == AccountTopic::ExecutionEvent.topic(&self.scope, self.account_id)? {
                let fact: DurableMessage<ExecutionFact> = serde_json::from_value(row.payload)?;
                tracker.process(&MessageEnvelope::from_durable(fact)?).await?;
            } else {
                let status: DurableMessage<OrderStatusFact> = serde_json::from_value(row.payload)?;
                tracker.process_status(&MessageEnvelope::from_durable(status)?).await?;
            }
        }
        Ok(())
    }
}
