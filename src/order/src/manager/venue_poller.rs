// SPDX-License-Identifier: GPL-3.0-only
//! Read venue facts after acceptance. Durable cursors never depend on broker delivery.
use super::{execution_tracker::OrderExecutionTracker, order_events::OrderEventWriter};
use anyhow::{ensure, Result};
use chrono::{DateTime, Duration, Utc};
use common_telemetry::tracing_context::MessageTrace;
use serde_json::Value;
use sigbot_core::modules::order::store::venue_postgres::{VenuePoll, VenuePostgresRepository};
use sigbot_exchange::client::execution_adapter::{IExecutionAdapter, PreparedSubmission};
use sigbot_messager::{client::messager_factory::ISigbotMessagerClient, storage::postgres::PostgresMessageStore};
use sigbot_types::modules::{
    exchange::instrument::InstrumentSpec,
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{AccountTopic, MessageScope},
    },
    order::execution::{ExecutionFact, OrderState, OrderStatusFact},
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::sync::Arc;

pub struct VenueExecutionPoller {
    pool: PgPool,
    scope: MessageScope,
    workflow_id: i64,
    account_id: i64,
    node_id: i64,
    consumer: String,
    producer: String,
    adapter: Arc<dyn IExecutionAdapter>,
    instrument: InstrumentSpec,
}
impl VenueExecutionPoller {
    pub fn new(
        pool: PgPool,
        scope: MessageScope,
        workflow_id: i64,
        account_id: i64,
        node_id: i64,
        consumer: String,
        adapter: Arc<dyn IExecutionAdapter>,
        instrument: InstrumentSpec,
    ) -> Self {
        let producer = format!("{consumer}:venue");
        Self {
            pool,
            scope,
            workflow_id,
            account_id,
            node_id,
            consumer,
            producer,
            adapter,
            instrument,
        }
    }

    pub async fn tick(&self, tracker: &OrderExecutionTracker, messager: &dyn ISigbotMessagerClient) -> Result<()> {
        self.drain(tracker).await?;
        let result = self.poll().await;
        PostgresMessageStore::new(self.pool.clone())
            .dispatch_pending(&self.producer, messager, 32, &|parent| {
                Ok(MessageTrace::child("order.venue.retry", parent).traceparent())
            })
            .await?;
        result
    }

    pub async fn poll(&self) -> Result<()> {
        let Some(poll) = VenuePostgresRepository::claim(
            &self.pool,
            &self.scope,
            self.workflow_id,
            self.account_id,
            &self.consumer,
        )
        .await?
        else {
            return Ok(());
        };
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
                VenuePostgresRepository::retry(&self.pool, &poll, Utc::now() + Duration::seconds(10)).await?;
                return Err(error);
            }
        };
        ensure!(page.facts.len() <= 1000, "venue execution page exceeds requested bound");
        ensure!(
            !page.has_more || (page.next_cursor.is_some() && page.next_cursor != poll.cursor),
            "venue page did not advance its cursor"
        );
        let observed = Utc::now();
        let mut tx = self.pool.begin().await?;
        if !VenuePostgresRepository::owns(&mut tx, &poll).await? {
            tx.commit().await?;
            return Ok(());
        }
        for fact in &page.facts {
            self.validate_fact(&poll, fact, observed)?;
            let payload = serde_json::to_value(fact)?;
            if VenuePostgresRepository::observe(&mut tx, &poll, "execution", &fact.exchange_trade_id, &payload).await? {
                self.publish(
                    &mut tx,
                    &poll,
                    AccountTopic::ExecutionEvent,
                    fact.executed_at,
                    observed,
                    payload,
                )
                .await?;
            }
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
            if VenuePostgresRepository::observe(&mut tx, &poll, "status", &reference, &payload).await? {
                self.publish(
                    &mut tx,
                    &poll,
                    AccountTopic::OrderEvent,
                    status.occurred_at,
                    observed,
                    payload,
                )
                .await?;
            }
        }
        let terminal = page
            .status
            .as_ref()
            .map(|status| (status.receipt.state, status.executed_quantity))
            .or_else(|| {
                (poll.receipt.state == OrderState::Filled).then_some((OrderState::Filled, poll.input.payload.quantity))
            });
        VenuePostgresRepository::finish(&mut tx, &poll, page.next_cursor.as_deref(), page.has_more, terminal).await?;
        tx.commit().await?;
        Ok(())
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

    async fn publish(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        poll: &VenuePoll,
        topic: AccountTopic,
        event_time: DateTime<Utc>,
        available_time: DateTime<Utc>,
        payload: Value,
    ) -> Result<()> {
        let seq = PostgresMessageStore::next_sequence(tx, &self.scope, self.node_id, &poll.input.run_id).await?;
        let output = DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: Some(self.workflow_id),
            account_id: Some(self.account_id),
            node_id: self.node_id,
            target_node_id: None,
            run_id: poll.input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time,
            available_time,
            produced_at: available_time,
            traceparent: MessageTrace::child("order.venue.observe", &poll.input.traceparent).traceparent(),
            payload,
        };
        PostgresMessageStore::enqueue(tx, &self.producer, &topic.topic(&self.scope, self.account_id)?, &output).await?;
        Ok(())
    }

    /// The host consumes its durable source partition directly; subscribing to
    /// its own derived order events would create a feedback loop.
    async fn drain(&self, tracker: &OrderExecutionTracker) -> Result<()> {
        let rows = sqlx::query(
            "SELECT o.topic,o.payload FROM messager_outbox o WHERE o.producer=$1
            AND (o.payload->>'workflow_id')::bigint=$2 AND NOT EXISTS(SELECT 1 FROM messager_inbox i
                WHERE i.env=o.env AND i.tenant_id=o.tenant_id AND i.space_id=o.space_id AND i.node_id=o.node_id
                AND i.run_id=o.run_id AND i.seq=o.seq AND i.state='done' AND i.consumer=CASE
                    WHEN o.topic LIKE '%/execution/event' THEN $3 ELSE $4 END)
            ORDER BY o.created_at,o.seq LIMIT 32",
        )
        .bind(&self.producer)
        .bind(self.workflow_id)
        .bind(OrderEventWriter::fact_owner(
            "execution",
            &self.scope,
            self.workflow_id,
            self.node_id,
        ))
        .bind(OrderEventWriter::fact_owner(
            "status",
            &self.scope,
            self.workflow_id,
            self.node_id,
        ))
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let payload: Value = row.get("payload");
            if row.get::<String, _>("topic") == AccountTopic::ExecutionEvent.topic(&self.scope, self.account_id)? {
                let fact: DurableMessage<ExecutionFact> = serde_json::from_value(payload)?;
                tracker.process(&MessageEnvelope::from_durable(fact)?).await?;
            } else {
                let status: DurableMessage<OrderStatusFact> = serde_json::from_value(payload)?;
                tracker.process_status(&MessageEnvelope::from_durable(status)?).await?;
            }
        }
        Ok(())
    }
}
