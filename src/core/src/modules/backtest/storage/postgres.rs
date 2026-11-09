// SPDX-License-Identifier: GPL-3.0-only

pub(crate) mod clock;
pub(crate) mod history;
pub(crate) mod session;

use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::datafeed::event::DatafeedEvent;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::intent::TradeIntent;
use sqlx::Row;
use uuid::Uuid;

pub struct RestingOrderCursor {
    pub accepted_at: DateTime<Utc>,
    pub producer_node_id: i64,
    pub sequence: i64,
    pub account_id: Uuid,
}

pub struct RestingOrder {
    pub cursor: RestingOrderCursor,
    pub exchange_order_id: String,
    pub message: DurableMessage<TradeIntent>,
    pub matching_configuration: Value,
}

pub struct BacktestTransaction {
    pub(crate) inner: sqlx::Transaction<'static, sqlx::Postgres>,
}

impl BacktestTransaction {
    pub(crate) async fn begin(pool: &sqlx::PgPool) -> Result<Self> {
        Ok(Self {
            inner: pool.begin().await?,
        })
    }

    pub async fn commit(self) -> Result<()> {
        self.inner.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<()> {
        self.inner.rollback().await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct BacktestPostgresRepository {
    pool: sqlx::PgPool,
    scope: MessageScope,
    node_id: i64,
}

impl BacktestPostgresRepository {
    pub async fn connect(database_url: &str, scope: MessageScope, node_id: i64) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(database_url)
                .await?,
            scope,
            node_id,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::new(self.pool.clone())
    }

    pub async fn begin(&self) -> Result<BacktestTransaction> {
        BacktestTransaction::begin(&self.pool).await
    }

    pub async fn virtual_time(&self, run_id: &Uuid) -> Result<DateTime<Utc>> {
        Ok(sqlx::query_scalar(
            "SELECT virtual_time FROM bt_checkpoint WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND state='running'",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn quarantine_route(&self, consumer: &str, topic: &str, message: &MessageEnvelope<Value>) -> Result<()> {
        let mut transaction = self.begin().await?;
        if MessagerPostgresRepository::accept(&mut transaction.inner, consumer, topic, message).await? {
            MessagerPostgresRepository::complete(&mut transaction.inner, consumer, &message.durable).await?;
        }
        transaction.commit().await
    }

    pub async fn reconcile(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT exchange_order_id,intent FROM bt_order WHERE env=$1 AND tenant_id=$2
            AND account_id=$3 AND run_id=$4 AND seq=$5 AND producer_node_id=$6",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(message.payload.account_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .bind(message.node_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            ensure!(
                row.get::<Value, _>("intent") == serde_json::to_value(&message.payload)?,
                "replay order identity collision"
            );
            Ok(row.get("exchange_order_id"))
        })
        .transpose()
    }

    pub async fn insert_order(
        &self,
        transaction: &mut BacktestTransaction,
        input: &DurableMessage<TradeIntent>,
        exchange_order_id: &str,
        accepted_at: DateTime<Utc>,
        matching_configuration: &Value,
    ) -> Result<bool> {
        Ok(sqlx::query("INSERT INTO bt_order(env,tenant_id,account_id,run_id,seq,
            exchange_order_id,intent,state,accepted_at,producer_node_id,original_message,matching_node_id,matching_configuration)
            VALUES($1,$2,$3,$4,$5,$6,$7,'accepted',$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.payload.account_id)
            .bind(&input.run_id).bind(input.seq).bind(exchange_order_id).bind(serde_json::to_value(&input.payload)?)
            .bind(accepted_at).bind(input.node_id).bind(serde_json::to_value(input)?).bind(self.node_id)
            .bind(matching_configuration).execute(&mut *transaction.inner).await?.rows_affected() == 1)
    }

    pub async fn accept_market(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<Option<BacktestTransaction>> {
        let mut transaction = self.begin().await?;
        if !MessagerPostgresRepository::accept(&mut transaction.inner, consumer, topic, message).await? {
            transaction.commit().await?;
            return Ok(None);
        }
        Ok(Some(transaction))
    }

    pub async fn verify_tick(
        &self,
        transaction: &mut BacktestTransaction,
        input: &DurableMessage<DatafeedEvent>,
    ) -> Result<()> {
        let fact = &input.payload;
        let workflow = input.workflow_id.context("tick workflow required")?;
        let stored = sqlx::query(
            "SELECT payload,event_at,available_at,seq FROM mkt_event m WHERE env=$1 AND tenant_id=$2
            AND source=$3 AND source_event_id=$4 AND revision=$5 AND symbol=$6 AND event_type='market'
            AND (run_id=$7 OR EXISTS(SELECT 1 FROM mkt_delivery d WHERE d.env=m.env AND d.tenant_id=m.tenant_id
            AND d.source=m.source AND d.source_event_id=m.source_event_id AND d.revision=m.revision
            AND d.run_id=$7 AND d.workflow_id=$8))",
        )
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(&fact.source)
        .bind(&fact.source_event_id)
        .bind(fact.revision)
        .bind(&fact.symbol)
        .bind(&input.run_id)
        .bind(workflow)
        .fetch_one(&mut *transaction.inner)
        .await
        .context("tick must first be persisted by Datafeed")?;
        ensure!(
            stored.get::<Value, _>("payload") == fact.raw
                && stored.get::<DateTime<Utc>, _>("event_at").timestamp_micros() == fact.event_time.timestamp_micros()
                && stored.get::<DateTime<Utc>, _>("available_at").timestamp_micros()
                    == fact.available_time.timestamp_micros()
                && stored.get::<i64, _>("seq") == fact.seq,
            "tick differs from its durable source fact"
        );
        Ok(())
    }

    pub async fn resting_orders(
        &self,
        transaction: &mut BacktestTransaction,
        input: &DurableMessage<DatafeedEvent>,
        cursor: Option<&RestingOrderCursor>,
        exchange: &str,
        symbol: &str,
    ) -> Result<Vec<RestingOrder>> {
        let fact = &input.payload;
        let rows = sqlx::query("SELECT accepted_at,producer_node_id,seq,account_id,exchange_order_id,original_message,matching_configuration
            FROM bt_order WHERE env=$1 AND tenant_id=$2 AND matching_node_id=$3 AND run_id=$4
            AND state='accepted' AND accepted_at<=$5 AND intent->>'order_type'=$6 AND intent->>'exchange'=$7 AND intent->>'symbol'=$8
            AND intent->'market'->>'source'=$9 AND (original_message->>'workflow_id')::uuid=$10
            AND ($11::timestamptz IS NULL OR (accepted_at,producer_node_id,seq,account_id)>($11,$12,$13,$14))
            ORDER BY accepted_at,producer_node_id,seq,account_id LIMIT 128 FOR UPDATE")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(self.node_id)
            .bind(&input.run_id).bind(fact.event_time).bind("LIMIT")
            .bind(exchange).bind(symbol).bind(&fact.source).bind(input.workflow_id)
            .bind(cursor.map(|value| value.accepted_at)).bind(cursor.map(|value| value.producer_node_id))
            .bind(cursor.map(|value| value.sequence)).bind(cursor.map(|value| value.account_id))
            .fetch_all(&mut *transaction.inner).await?;
        rows.into_iter()
            .map(|row| {
                Ok(RestingOrder {
                    cursor: RestingOrderCursor {
                        accepted_at: row.get("accepted_at"),
                        producer_node_id: row.get("producer_node_id"),
                        sequence: row.get("seq"),
                        account_id: row.get("account_id"),
                    },
                    exchange_order_id: row.get("exchange_order_id"),
                    message: serde_json::from_value(row.get("original_message"))?,
                    matching_configuration: row.get("matching_configuration"),
                })
            })
            .collect()
    }

    pub async fn fill<T: serde::Serialize>(
        &self,
        transaction: &mut BacktestTransaction,
        producer: &str,
        topic: &str,
        order: &DurableMessage<TradeIntent>,
        event_time: DateTime<Utc>,
        output: &DurableMessage<T>,
    ) -> Result<()> {
        let output = serde_json::from_value(serde_json::to_value(output)?)?;
        MessagerPostgresRepository::enqueue(&mut transaction.inner, producer, topic, &output).await?;
        let changed = sqlx::query(
            "UPDATE bt_order SET state='filled',matched_at=$6 WHERE env=$1 AND tenant_id=$2
            AND account_id=$3 AND run_id=$4 AND seq=$5 AND producer_node_id=$7 AND state='accepted'",
        )
        .bind(&order.scope.env)
        .bind(order.scope.tenant_id)
        .bind(order.payload.account_id)
        .bind(&order.run_id)
        .bind(order.seq)
        .bind(event_time)
        .bind(order.node_id)
        .execute(&mut *transaction.inner)
        .await?
        .rows_affected();
        ensure!(changed == 1, "backtest order no longer owns its pending fill");
        Ok(())
    }

    pub async fn next_sequence(&self, transaction: &mut BacktestTransaction, run_id: &Uuid) -> Result<i64> {
        MessagerPostgresRepository::next_sequence(&mut transaction.inner, &self.scope, self.node_id, run_id).await
    }

    pub async fn complete_market(
        &self,
        mut transaction: BacktestTransaction,
        consumer: &str,
        input: &DurableMessage<Value>,
    ) -> Result<()> {
        MessagerPostgresRepository::complete(&mut transaction.inner, consumer, input).await?;
        transaction.commit().await
    }
}
