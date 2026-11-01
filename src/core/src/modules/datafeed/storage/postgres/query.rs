// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::datafeed::storage::postgres::DatafeedPostgresRepository;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::datafeed::event::{DatafeedEvent, DatafeedPublication};
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::strategy::execution::{
    ComputePlan, SnapshotMode, SnapshotPoint, SnapshotQuery, SnapshotResult, StrategyEventData,
};
use sqlx::Row;
use std::str::FromStr;
use uuid::Uuid;

mod aggregate;

#[derive(Clone)]
pub struct MarketQueryPostgresRepository {
    events: DatafeedPostgresRepository,
}

pub struct MarketQueryPostgresSession(sqlx::Transaction<'static, sqlx::Postgres>);

impl MarketQueryPostgresRepository {
    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self {
            events: DatafeedPostgresRepository::from_pool(pool),
        }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            events: DatafeedPostgresRepository::connect(database_url).await?,
        })
    }

    pub fn message_store(&self) -> MessagerPostgresRepository {
        self.events.message_store()
    }

    pub async fn admit(&self, owner: &str, topic: &str, message: &MessageEnvelope<Value>) -> Result<bool> {
        let mut transaction = self.events.pool().begin().await?;
        let accepted = MessagerPostgresRepository::accept(&mut transaction, owner, topic, message).await?;
        transaction.commit().await?;
        Ok(accepted)
    }

    pub async fn begin(
        &self,
        owner: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<Option<MarketQueryPostgresSession>> {
        let mut transaction = self.events.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await?;
        if !MessagerPostgresRepository::accept(&mut transaction, owner, topic, message).await? {
            transaction.commit().await?;
            return Ok(None);
        }
        Ok(Some(MarketQueryPostgresSession(transaction)))
    }

    pub async fn query(&self) -> Result<MarketQueryPostgresSession> {
        let mut transaction = self.events.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        Ok(MarketQueryPostgresSession(transaction))
    }
}

impl MarketQueryPostgresSession {
    pub async fn virtual_time(&mut self, scope: &MessageScope, run_id: &Uuid) -> Result<DateTime<Utc>> {
        Ok(sqlx::query_scalar(
            "SELECT virtual_time FROM bt_checkpoint WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND state='running'",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(run_id)
        .fetch_one(&mut *self.0)
        .await?)
    }

    pub async fn persist(
        &mut self,
        publication: &DatafeedPublication<'_>,
        event: DatafeedEvent,
    ) -> Result<DatafeedEvent> {
        DatafeedPostgresRepository::persist_in(&mut self.0, publication, event).await
    }

    pub async fn snapshot(
        &mut self,
        scope: &MessageScope,
        run_id: &Uuid,
        query: &SnapshotQuery,
        now: DateTime<Utc>,
    ) -> Result<SnapshotResult> {
        let as_of = match query.mode {
            SnapshotMode::Latest => now,
            SnapshotMode::AsOf(value) => value,
        };
        let rows = sqlx::query("SELECT DISTINCT ON(source,symbol) source,symbol,event_at,available_at,payload FROM mkt_event
            WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND event_type='market' AND source=ANY($4) AND symbol=ANY($5)
            AND event_at<=$6 AND available_at<=$6 ORDER BY source,symbol,event_at DESC,available_at DESC,revision DESC,source_event_id DESC LIMIT $7")
            .bind(&scope.env).bind(scope.tenant_id).bind(run_id).bind(&query.sources).bind(&query.symbols)
            .bind(as_of).bind(i64::from(query.max_rows)).fetch_all(&mut *self.0).await?;
        Ok(SnapshotResult {
            points: rows.into_iter().map(Self::point).collect::<Result<_>>()?,
        })
    }

    pub async fn history(
        &mut self,
        scope: &MessageScope,
        run_id: &Uuid,
        query: &SnapshotQuery,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SnapshotPoint>> {
        let rows = sqlx::query("SELECT source,symbol,event_at,available_at,payload FROM mkt_event
            WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND event_type='market' AND source=ANY($4) AND symbol=ANY($5)
            AND event_at>=$6 AND event_at<=$7 AND available_at<=$8 ORDER BY event_at,source,symbol,seq,revision LIMIT $9")
            .bind(&scope.env).bind(scope.tenant_id).bind(run_id).bind(&query.sources).bind(&query.symbols)
            .bind(start).bind(end).bind(as_of).bind(i64::from(query.max_rows)).fetch_all(&mut *self.0).await?;
        rows.into_iter().map(Self::point).collect()
    }

    fn point(row: sqlx::postgres::PgRow) -> Result<SnapshotPoint> {
        let payload: Value = row.try_get("payload")?;
        let price = payload.get("price").context("market fact has no normalized price")?;
        let price = Decimal::from_str(&price.as_str().map(str::to_owned).unwrap_or_else(|| price.to_string()))?;
        ensure!(price > Decimal::ZERO, "market fact price must be positive");
        Ok(SnapshotPoint {
            source: row.try_get("source")?,
            symbol: row.try_get("symbol")?,
            event_time: row.try_get("event_at")?,
            available_time: row.try_get("available_at")?,
            price,
        })
    }

    pub async fn compute(
        &mut self,
        scope: &MessageScope,
        run_id: &Uuid,
        plan: &ComputePlan,
    ) -> Result<StrategyEventData> {
        sqlx::query("SET LOCAL statement_timeout='5s'")
            .execute(&mut *self.0)
            .await?;
        sqlx::query("SET LOCAL work_mem='4MB'").execute(&mut *self.0).await?;
        let timescale: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='timescaledb')")
                .fetch_one(&mut *self.0)
                .await?;
        let rows: Vec<Value> = sqlx::query_scalar(&Self::statement(plan, timescale))
            .bind(&scope.env)
            .bind(scope.tenant_id)
            .bind(run_id)
            .bind(&plan.sources)
            .bind(&plan.symbols)
            .bind(plan.history_from)
            .bind(plan.as_of)
            .bind(plan.window_ms as f64 / 1000.0)
            .bind(i64::from(plan.max_rows) + 1)
            .fetch_all(&mut *self.0)
            .await?;
        ensure!(
            rows.len() <= plan.max_rows as usize,
            "compute result row bound exceeded; narrow history or widen window"
        );
        ensure!(
            serde_json::to_vec(&rows)?.len() <= plan.max_bytes as usize,
            "compute result byte bound exceeded"
        );
        let watermark: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT MAX(event_at) FROM mkt_event WHERE env=$1 AND tenant_id=$2 AND run_id=$3
            AND event_type='market' AND source=ANY($4) AND symbol=ANY($5) AND event_at>=$6 AND event_at<=$7 AND available_at<=$7")
            .bind(&scope.env).bind(scope.tenant_id).bind(run_id).bind(&plan.sources).bind(&plan.symbols)
            .bind(plan.history_from).bind(plan.as_of).fetch_one(&mut *self.0).await?;
        let revision: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(revision),0) FROM mkt_event WHERE env=$1 AND tenant_id=$2 AND run_id=$3
            AND event_type='market' AND source=ANY($4) AND symbol=ANY($5) AND event_at>=$6 AND event_at<=$7 AND available_at<=$7")
            .bind(&scope.env).bind(scope.tenant_id).bind(run_id).bind(&plan.sources).bind(&plan.symbols)
            .bind(plan.history_from).bind(plan.as_of).fetch_one(&mut *self.0).await?;
        Ok(StrategyEventData::Compute {
            rows,
            watermark: watermark.unwrap_or(plan.history_from),
            revision: revision.try_into()?,
        })
    }

    pub async fn next_sequence(&mut self, scope: &MessageScope, node_id: i64, run_id: &Uuid) -> Result<i64> {
        MessagerPostgresRepository::next_sequence(&mut self.0, scope, node_id, run_id).await
    }

    pub async fn enqueue<T: serde::Serialize>(
        &mut self,
        owner: &str,
        topic: &str,
        message: &DurableMessage<T>,
    ) -> Result<()> {
        let message = serde_json::from_value(serde_json::to_value(message)?)?;
        MessagerPostgresRepository::enqueue(&mut self.0, owner, topic, &message).await?;
        Ok(())
    }

    pub async fn checkpoint_compute(
        &mut self,
        scope: &MessageScope,
        workflow_id: Uuid,
        node_id: i64,
        input: &DurableMessage<Value>,
        result: &DurableMessage<Value>,
        watermark: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query("INSERT INTO wf_checkpoint(env,tenant_id,workflow_id,node_id,run_id,partition_key,seq,state,watermark,version)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,1) ON CONFLICT(env,tenant_id,workflow_id,node_id,run_id,partition_key)
            DO UPDATE SET seq=EXCLUDED.seq,state=EXCLUDED.state,watermark=GREATEST(wf_checkpoint.watermark,EXCLUDED.watermark),
            version=wf_checkpoint.version+1,updated_at=NOW()")
            .bind(&scope.env).bind(scope.tenant_id).bind(workflow_id).bind(node_id)
            .bind(&input.run_id).bind(format!("compute:{}", input.node_id)).bind(input.seq)
            .bind(serde_json::json!({"plan":input.payload,"result":result.payload})).bind(watermark)
            .execute(&mut *self.0).await?;
        Ok(())
    }

    pub async fn complete(mut self, owner: &str, input: &DurableMessage<Value>) -> Result<()> {
        MessagerPostgresRepository::complete(&mut self.0, owner, input).await?;
        Ok(self.0.commit().await?)
    }

    pub async fn rollback(self) -> Result<()> {
        Ok(self.0.rollback().await?)
    }
}
