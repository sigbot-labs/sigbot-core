// SPDX-License-Identifier: GPL-3.0-only
//! Execution facts update order progress independently of acceptance receipts.
use super::order_events::OrderEventWriter;
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_messager::storage::postgres::PostgresMessageStore;
use sigbot_types::modules::{
    exchange::instrument::InstrumentSpec,
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{AccountTopic, MessageScope},
    },
    order::{
        execution::{ExecutionFact, OrderReceipt, OrderState, OrderStatusFact},
        intent::TradeIntent,
    },
};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub struct OrderExecutionTracker {
    pool: PgPool,
    scope: MessageScope,
    node_id: i64,
    source_node_ids: Vec<i64>,
    instrument: Option<InstrumentSpec>,
    workflow_id: i64,
}

impl OrderExecutionTracker {
    pub fn new(
        pool: PgPool,
        scope: MessageScope,
        node_id: i64,
        source_node_ids: Vec<i64>,
        workflow_id: i64,
    ) -> Result<Self> {
        scope.validate()?;
        ensure!(
            node_id > 0
                && workflow_id > 0
                && !source_node_ids.is_empty()
                && source_node_ids.iter().all(|node| *node > 0),
            "explicit exchange execution source nodes required"
        );
        Ok(Self {
            pool,
            scope,
            node_id,
            source_node_ids,
            instrument: None,
            workflow_id,
        })
    }

    pub fn with_instrument(mut self, instrument: InstrumentSpec) -> Result<Self> {
        instrument.validate()?;
        self.instrument = Some(instrument);
        Ok(self)
    }

    pub async fn process(&self, message: &MessageEnvelope<ExecutionFact>) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        self.validate_source(input, fact.account_id)?;
        ensure!(input.event_time == fact.executed_at, "execution event time mismatch");
        let topic = AccountTopic::ExecutionEvent.topic(&self.scope, fact.account_id)?;
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        let consumer = OrderEventWriter::fact_owner("execution", &self.scope, self.workflow_id, self.node_id);
        let mut tx = self.pool.begin().await?;
        let order = self
            .lock_order(
                &mut tx,
                fact.wallet_id,
                fact.account_id,
                fact.reservation_node_id,
                &input.run_id,
                fact.reservation_seq,
            )
            .await?;
        let intent: TradeIntent = serde_json::from_value(order.get("intent"))?;
        ensure!(
            intent.exchange == fact.exchange && intent.symbol == fact.symbol && intent.side == fact.side,
            "execution differs from persisted intent"
        );
        let exchange_order_id: Option<String> = order.get("exchange_order_id");
        ensure!(
            exchange_order_id
                .as_deref()
                .is_none_or(|id| id == fact.exchange_order_id),
            "execution order ID changed"
        );
        if !PostgresMessageStore::accept(&mut tx, &consumer, &topic, &wire).await? {
            tx.commit().await?;
            return Ok(false);
        }
        fact.validate()?;
        if let Some(spec) = &self.instrument {
            ensure!(
                fact.exchange == spec.exchange
                    && fact.symbol == spec.symbol
                    && fact.base_asset == spec.base_asset
                    && fact.quote_asset == spec.quote_asset,
                "execution assets differ from the configured instrument"
            );
        }
        let payload = serde_json::to_value(fact)?;
        let inserted = sqlx::query("INSERT INTO order_execution(wallet_id,exchange,exchange_trade_id,exchange_order_id,run_id,seq,
            symbol,side,price,quantity,fee,fee_asset,executed_at,producer_node_id,reservation_node_id,reservation_seq,order_complete,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT DO NOTHING")
            .bind(fact.wallet_id).bind(&fact.exchange).bind(&fact.exchange_trade_id).bind(&fact.exchange_order_id)
            .bind(&input.run_id).bind(input.seq).bind(&fact.symbol).bind(if fact.side == sigbot_types::modules::order::intent::OrderSide::Buy { "BUY" } else { "SELL" })
            .bind(fact.price).bind(fact.quantity).bind(fact.fee).bind(&fact.fee_asset).bind(fact.executed_at)
            .bind(input.node_id).bind(fact.reservation_node_id).bind(fact.reservation_seq).bind(fact.order_complete).bind(&payload)
            .execute(&mut *tx).await?.rows_affected();
        if inserted == 0 {
            let previous: Option<Value> = sqlx::query_scalar(
                "SELECT payload FROM order_execution WHERE wallet_id=$1 AND exchange=$2 AND exchange_trade_id=$3",
            )
            .bind(fact.wallet_id)
            .bind(&fact.exchange)
            .bind(&fact.exchange_trade_id)
            .fetch_one(&mut *tx)
            .await?;
            ensure!(
                previous == Some(payload),
                "exchange execution ID reused with different facts"
            );
            PostgresMessageStore::complete(&mut tx, &consumer, &wire.durable).await?;
            tx.commit().await?;
            return Ok(false);
        }
        let filled: Decimal = sqlx::query_scalar(
            "SELECT COALESCE(SUM(quantity),0) FROM order_execution
            WHERE wallet_id=$1 AND exchange=$2 AND exchange_order_id=$3",
        )
        .bind(fact.wallet_id)
        .bind(&fact.exchange)
        .bind(&fact.exchange_order_id)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(filled <= intent.quantity, "exchange overfill requires reconciliation");
        let previous: String = order.get("state");
        let state = if filled == intent.quantity {
            OrderState::Filled
        } else if previous == "cancelled" {
            OrderState::Cancelled
        } else {
            OrderState::PartiallyFilled
        };
        self.record(
            &mut tx,
            &wire.durable,
            OrderStatusFact {
                reservation_node_id: fact.reservation_node_id,
                reservation_seq: fact.reservation_seq,
                wallet_id: fact.wallet_id,
                account_id: fact.account_id,
                exchange: fact.exchange.clone(),
                receipt: OrderReceipt {
                    exchange_order_id: fact.exchange_order_id.clone(),
                    state,
                },
                executed_quantity: filled,
                occurred_at: fact.executed_at,
            },
        )
        .await?;
        PostgresMessageStore::complete(&mut tx, &consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn process_status(&self, message: &MessageEnvelope<OrderStatusFact>) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        fact.validate()?;
        self.validate_source(input, fact.account_id)?;
        ensure!(input.event_time == fact.occurred_at, "status event time mismatch");
        let topic = AccountTopic::OrderEvent.topic(&self.scope, fact.account_id)?;
        let consumer = OrderEventWriter::fact_owner("status", &self.scope, self.workflow_id, self.node_id);
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        let mut tx = self.pool.begin().await?;
        let order = self
            .lock_order(
                &mut tx,
                fact.wallet_id,
                fact.account_id,
                fact.reservation_node_id,
                &input.run_id,
                fact.reservation_seq,
            )
            .await?;
        let intent: TradeIntent = serde_json::from_value(order.get("intent"))?;
        ensure!(
            intent.exchange == fact.exchange && fact.executed_quantity <= intent.quantity,
            "venue cumulative quantity exceeds intent"
        );
        let exchange_order_id: Option<String> = order.get("exchange_order_id");
        ensure!(
            exchange_order_id
                .as_deref()
                .is_none_or(|id| id == fact.receipt.exchange_order_id),
            "venue order identity changed"
        );
        if !PostgresMessageStore::accept(&mut tx, &consumer, &topic, &wire).await? {
            tx.commit().await?;
            return Ok(false);
        }
        let previous: String = order.get("state");
        let mut status = fact.clone();
        if previous == "filled" {
            status.receipt.state = OrderState::Filled;
            status.executed_quantity = intent.quantity;
        } else if previous == "cancelled" {
            status.receipt.state = OrderState::Cancelled;
        } else if previous == "partially_filled" && status.receipt.state == OrderState::Acknowledged {
            status.receipt.state = OrderState::PartiallyFilled;
        }
        // Status alone does not fabricate order_execution rows or wallet postings.
        self.record(&mut tx, &wire.durable, status).await?;
        PostgresMessageStore::complete(&mut tx, &consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(true)
    }

    fn validate_source<T>(&self, input: &DurableMessage<T>, account_id: i64) -> Result<()> {
        ensure!(
            input.scope == self.scope
                && input.workflow_id == Some(self.workflow_id)
                && input.account_id == Some(account_id)
                && input.kind == MessageKind::Event
                && self.source_node_ids.contains(&input.node_id)
                && input.available_time <= Utc::now(),
            "untrusted or unavailable venue fact"
        );
        Ok(())
    }

    async fn lock_order(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: i64,
        account_id: i64,
        producer_node_id: i64,
        run_id: &str,
        seq: i64,
    ) -> Result<sqlx::postgres::PgRow> {
        sqlx::query("SELECT o.intent,o.state,o.exchange_order_id FROM order_record o JOIN s_wallets w ON w.id=o.wallet_id
            JOIN messager_inbox i ON i.env=o.env AND i.tenant_id::text=o.tenant_id AND i.space_id=o.space_id
                AND i.node_id=o.producer_node_id AND i.run_id=o.run_id AND i.seq=o.seq AND i.consumer=$9
            WHERE o.env=$1 AND o.tenant_id=$2 AND o.space_id=$3 AND o.wallet_id=$4 AND o.producer_node_id=$5
            AND o.run_id=$6 AND o.seq=$7 AND w.account_id=$8 AND w.env=$1 AND w.tenant_id=$2 AND w.space_id=$3 FOR UPDATE OF o")
            .bind(&self.scope.env).bind(self.scope.tenant_id.to_string()).bind(self.scope.space_id).bind(wallet_id)
            .bind(producer_node_id).bind(run_id).bind(seq).bind(account_id)
            .bind(OrderEventWriter::owner(&self.scope, self.workflow_id, self.node_id))
            .fetch_one(&mut **tx).await.context("venue fact has no owned workflow intent")
    }

    async fn record(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<Value>,
        status: OrderStatusFact,
    ) -> Result<()> {
        sqlx::query("UPDATE order_record SET state=$8,exchange_order_id=$9,updated_at=NOW()
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND wallet_id=$4 AND producer_node_id=$5 AND run_id=$6 AND seq=$7")
            .bind(&self.scope.env).bind(self.scope.tenant_id.to_string()).bind(self.scope.space_id).bind(status.wallet_id)
            .bind(status.reservation_node_id).bind(&input.run_id).bind(status.reservation_seq).bind(status.receipt.state.as_str())
            .bind(&status.receipt.exchange_order_id).execute(&mut **tx).await?;
        let seq = PostgresMessageStore::next_sequence(tx, &self.scope, self.node_id, &input.run_id).await?;
        let trace = MessageTrace::child("order.execution.publish", &input.traceparent);
        let output = DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: Some(status.account_id),
            node_id: self.node_id,
            target_node_id: None,
            run_id: input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: input.event_time,
            available_time: input.available_time,
            produced_at: Utc::now(),
            traceparent: trace.traceparent(),
            payload: serde_json::to_value(&status)?,
        };
        let producer = OrderEventWriter::owner(&self.scope, self.workflow_id, self.node_id);
        PostgresMessageStore::enqueue(
            tx,
            &producer,
            &AccountTopic::OrderEvent.topic(&self.scope, status.account_id)?,
            &output,
        )
        .await?;
        Ok(())
    }
}
