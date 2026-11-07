// SPDX-License-Identifier: GPL-3.0-only
//! Execution facts update order progress independently of acceptance receipts.

use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::order::storage::postgres::OrderPostgresRepository;
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::exchange::instrument::InstrumentSpec;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope, MessageKind};
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::execution::OrderReceipt;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::execution::OrderStatusFact;
use sigbot_types::order::intent::TradeIntent;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

pub struct OrderExecutionPostgresRepository {
    pub(crate) pool: PgPool,
    pub(crate) scope: MessageScope,
    pub(crate) node_id: i64,
    pub(crate) source_node_ids: Vec<i64>,
    instrument: Option<InstrumentSpec>,
    pub(crate) workflow_id: Uuid,
}

impl OrderExecutionPostgresRepository {
    pub fn new(
        storage: &OrderPostgresRepository,
        scope: MessageScope,
        node_id: i64,
        source_node_ids: Vec<i64>,
        workflow_id: Uuid,
    ) -> Result<Self> {
        scope.validate()?;
        ensure!(
            node_id > 0
                && workflow_id.get_version_num() == 7
                && !source_node_ids.is_empty()
                && source_node_ids.iter().all(|node| *node > 0),
            "explicit exchange execution source nodes required"
        );
        Ok(Self {
            pool: storage.pool.clone(),
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
        let consumer = OrderPostgresRepository::fact_owner("execution", &self.scope, self.workflow_id, self.node_id);
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
        if !MessagerPostgresRepository::accept(&mut tx, &consumer, &topic, &wire).await? {
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
        let inserted = sqlx::query("INSERT INTO ord_execution(wallet_id,exchange,exchange_trade_id,exchange_order_id,run_id,seq,
            symbol,side,price,quantity,fee,fee_asset,executed_at,producer_node_id,reservation_node_id,reservation_seq,order_complete,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT DO NOTHING")
            .bind(fact.wallet_id).bind(&fact.exchange).bind(&fact.exchange_trade_id).bind(&fact.exchange_order_id)
            .bind(&input.run_id).bind(input.seq).bind(&fact.symbol).bind(if fact.side == sigbot_types::order::intent::OrderSide::Buy { "BUY" } else { "SELL" })
            .bind(fact.price).bind(fact.quantity).bind(fact.fee).bind(&fact.fee_asset).bind(fact.executed_at)
            .bind(input.node_id).bind(fact.reservation_node_id).bind(fact.reservation_seq).bind(fact.order_complete).bind(&payload)
            .execute(&mut *tx).await?.rows_affected();
        if inserted == 0 {
            let previous: Option<Value> = sqlx::query_scalar(
                "SELECT payload FROM ord_execution WHERE wallet_id=$1 AND exchange=$2 AND exchange_trade_id=$3",
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
            MessagerPostgresRepository::complete(&mut tx, &consumer, &wire.durable).await?;
            tx.commit().await?;
            return Ok(false);
        }
        let filled: Decimal = sqlx::query_scalar(
            "SELECT COALESCE(SUM(quantity),0) FROM ord_execution
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
        MessagerPostgresRepository::complete(&mut tx, &consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(true)
    }

    pub(crate) fn validate_source<T>(&self, input: &DurableMessage<T>, account_id: Uuid) -> Result<()> {
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

    pub(crate) async fn lock_order(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        wallet_id: Uuid,
        account_id: Uuid,
        producer_node_id: i64,
        run_id: &Uuid,
        sequence: i64,
    ) -> Result<sqlx::postgres::PgRow> {
        sqlx::query(
            "SELECT o.intent,o.state,o.exchange_order_id FROM order_info o JOIN ledger_info w ON w.id=o.wallet_id
            JOIN msg_inbox i ON i.env=o.env AND i.tenant_id=o.tenant_id
                AND i.node_id=o.producer_node_id AND i.run_id=o.run_id AND i.seq=o.seq AND i.consumer=$8
            WHERE o.env=$1 AND o.tenant_id=$2 AND o.wallet_id=$3 AND o.producer_node_id=$4
            AND o.run_id=$5 AND o.seq=$6 AND w.account_id=$7 AND w.env=$1 AND w.tenant_id=$2 FOR UPDATE OF o",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(wallet_id)
        .bind(producer_node_id)
        .bind(run_id)
        .bind(sequence)
        .bind(account_id)
        .bind(OrderPostgresRepository::owner(
            &self.scope,
            self.workflow_id,
            self.node_id,
        ))
        .fetch_one(&mut **transaction)
        .await
        .context("venue fact has no owned workflow intent")
    }
}
