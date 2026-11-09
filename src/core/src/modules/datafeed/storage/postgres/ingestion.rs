// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use serde_json::Value;
use sigbot_types::datafeed::event::DatafeedEvent;
use sigbot_types::datafeed::event::{DatafeedPublication, HistoricalImport};
use sigbot_types::exchange::mode::ExecutionMode;
use sigbot_types::messager::envelope::{DurableMessage, MessageKind, TraceContext};
use sigbot_types::messager::topic::WorkflowTopic;
use sqlx::Row;
use uuid::Uuid;

#[derive(Clone)]
pub struct DatafeedPostgresRepository {
    pool: sqlx::PgPool,
}

impl DatafeedPostgresRepository {
    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(database_url)
                .await?,
        })
    }

    pub fn message_store(&self) -> MessagerPostgresRepository {
        MessagerPostgresRepository::new(self.pool.clone())
    }

    pub async fn ingest(&self, publication: &DatafeedPublication<'_>, event: DatafeedEvent) -> Result<DatafeedEvent> {
        let mut transaction = self.pool.begin().await?;
        let event = Self::persist_in(&mut transaction, publication, event).await?;
        transaction.commit().await?;
        Ok(event)
    }

    pub(crate) async fn persist_in(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        publication: &DatafeedPublication<'_>,
        mut event: DatafeedEvent,
    ) -> Result<DatafeedEvent> {
        ensure!(event.revision >= 0, "negative source revision");
        ensure!(
            event.event_time <= event.available_time,
            "source fact available before event time"
        );
        let raw = event.raw.clone();
        sqlx::query("INSERT INTO mkt_event(env,tenant_id,source,source_event_id,symbol,event_type,event_at,available_at,seq,payload,run_id,revision)
            VALUES($1,$2,$3,$4,$5,$11,$6,$7,$8,$9,$10,$12) ON CONFLICT DO NOTHING")
            .bind(&publication.scope.env).bind(publication.scope.tenant_id)
            .bind(&event.source).bind(&event.source_event_id).bind(&event.symbol).bind(event.event_time)
            .bind(event.available_time).bind(event.seq).bind(&raw).bind(publication.run_id).bind(&event.event_type)
            .bind(event.revision).execute(&mut **transaction).await?;
        let existing = sqlx::query(
            "SELECT payload,available_at,event_at,symbol,event_type,seq FROM mkt_event WHERE env=$1 AND tenant_id=$2
            AND run_id=$3 AND source=$4 AND source_event_id=$5 AND revision=$6 FOR UPDATE",
        )
        .bind(&publication.scope.env)
        .bind(publication.scope.tenant_id)
        .bind(publication.run_id)
        .bind(&event.source)
        .bind(&event.source_event_id)
        .bind(event.revision)
        .fetch_one(&mut **transaction)
        .await?;
        ensure!(
            existing.get::<Value, _>("payload") == raw
                && existing.get::<String, _>("symbol") == event.symbol
                && existing.get::<String, _>("event_type") == event.event_type,
            "source event identity reused with changed payload; explicit revision required"
        );
        event.available_time = existing.get("available_at");
        event.event_time = existing.get("event_at");
        event.seq = existing.get("seq");
        let delivered: Option<Value> = sqlx::query_scalar(
            "SELECT payload FROM mkt_delivery WHERE env=$1 AND tenant_id=$2
            AND node_id=$3 AND run_id=$4 AND source=$5 AND source_event_id=$6 AND revision=$7",
        )
        .bind(&publication.scope.env)
        .bind(publication.scope.tenant_id)
        .bind(publication.node_id)
        .bind(publication.run_id)
        .bind(&event.source)
        .bind(&event.source_event_id)
        .bind(event.revision)
        .fetch_optional(&mut **transaction)
        .await?;
        let durable = if let Some(payload) = delivered {
            let saved: DurableMessage<Value> = serde_json::from_value(payload)?;
            ensure!(
                saved.workflow_id == Some(publication.workflow_id),
                "market delivery identity belongs to another Workflow"
            );
            saved
        } else {
            TraceContext::parse(publication.traceparent)?;
            DurableMessage {
                version: 2,
                scope: publication.scope.clone(),
                workflow_id: Some(publication.workflow_id),
                account_id: None,
                node_id: publication.node_id,
                target_node_id: None,
                run_id: publication.run_id.to_owned(),
                seq: MessagerPostgresRepository::next_sequence(
                    transaction,
                    publication.scope,
                    publication.node_id,
                    publication.run_id,
                )
                .await?,
                attempt_no: 0,
                kind: MessageKind::Event,
                event_time: event.event_time,
                available_time: event.available_time,
                produced_at: Utc::now(),
                traceparent: MessageTrace::child("datafeed.publish", publication.traceparent).traceparent(),
                payload: serde_json::to_value(&event)?,
            }
        };
        sqlx::query("INSERT INTO mkt_delivery(env,tenant_id,workflow_id,node_id,run_id,source,source_event_id,revision,message_seq,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING")
            .bind(&publication.scope.env).bind(publication.scope.tenant_id)
            .bind(publication.workflow_id).bind(publication.node_id).bind(publication.run_id).bind(&event.source)
            .bind(&event.source_event_id).bind(event.revision).bind(durable.seq).bind(serde_json::to_value(&durable)?)
            .execute(&mut **transaction).await?;
        let topic = match event.event_type.as_str() {
            "news" => WorkflowTopic::FeedNews,
            "market" => WorkflowTopic::FeedMarket,
            _ => anyhow::bail!("unsupported source event type"),
        }
        .topic(publication.scope, publication.workflow_id)?;
        MessagerPostgresRepository::enqueue(transaction, publication.producer, &topic, &durable).await?;
        let partition_key = if event.event_type == "market" {
            let native = event.raw.get("price_type").and_then(Value::as_str) == Some("last_trade")
                && matches!(
                    event.raw.get("provider").and_then(Value::as_str),
                    Some("binance_spot" | "hyperliquid_spot" | "hyperliquid_perpetual")
                );
            format!(
                "market:{}:{}{}",
                event.source,
                event.symbol,
                if native { ":last_trade" } else { "" }
            )
        } else {
            event.source.clone()
        };
        sqlx::query("INSERT INTO wf_checkpoint(env,tenant_id,workflow_id,node_id,run_id,partition_key,seq,state,watermark,version)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,1) ON CONFLICT(env,tenant_id,workflow_id,node_id,run_id,partition_key) DO UPDATE
            SET seq=GREATEST(wf_checkpoint.seq,EXCLUDED.seq),watermark=GREATEST(wf_checkpoint.watermark,EXCLUDED.watermark),
            state=CASE WHEN EXCLUDED.seq>wf_checkpoint.seq THEN EXCLUDED.state ELSE wf_checkpoint.state END,
            version=wf_checkpoint.version+CASE WHEN EXCLUDED.seq>wf_checkpoint.seq THEN 1 ELSE 0 END,updated_at=NOW()")
            .bind(&publication.scope.env).bind(publication.scope.tenant_id)
            .bind(publication.workflow_id).bind(publication.node_id).bind(publication.run_id).bind(partition_key)
            .bind(event.seq).bind(serde_json::json!({"source_event_id":event.source_event_id})).bind(event.event_time)
            .execute(&mut **transaction).await?;
        Ok(event)
    }

    pub(crate) async fn replay_in(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        input: &HistoricalImport<'_>,
    ) -> Result<()> {
        input.source_scope.validate()?;
        input.target_scope.validate()?;
        ensure!(
            input.mode == ExecutionMode::Backtest && input.source_run != input.target_run,
            "historical import requires backtest mode and a separate run"
        );
        ensure!(
            input.clock <= input.frozen_at && input.revision >= 0 && input.revision <= i32::MAX as i64,
            "invalid frozen historical boundary"
        );
        let source = sqlx::query("SELECT symbol,event_type,event_at,available_at,seq,payload FROM mkt_event
            WHERE env=$1 AND tenant_id=$2 AND (run_id=$3 OR EXISTS(SELECT 1 FROM mkt_delivery d WHERE d.env=mkt_event.env
            AND d.tenant_id=mkt_event.tenant_id AND d.run_id=$3 AND d.source=mkt_event.source
            AND d.source_event_id=mkt_event.source_event_id AND d.revision=mkt_event.revision AND d.created_at<=$7)) AND source=$4
            AND source_event_id=$5 AND revision=$6 AND recorded_at<=$7 AND available_at<=$8")
            .bind(&input.source_scope.env).bind(input.source_scope.tenant_id)
            .bind(input.source_run).bind(input.source).bind(input.source_event_id).bind(input.revision as i32)
            .bind(input.frozen_at).bind(input.clock).fetch_one(&mut **transaction).await?;
        let payload: Value = source.get("payload");
        sqlx::query("INSERT INTO mkt_event(env,tenant_id,run_id,source,source_event_id,revision,symbol,event_type,event_at,available_at,seq,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING")
            .bind(&input.target_scope.env).bind(input.target_scope.tenant_id)
            .bind(input.target_run).bind(input.source).bind(input.source_event_id).bind(input.revision as i32)
            .bind(source.get::<String,_>("symbol")).bind(source.get::<String,_>("event_type"))
            .bind(source.get::<DateTime<Utc>,_>("event_at")).bind(source.get::<DateTime<Utc>,_>("available_at"))
            .bind(source.get::<i64,_>("seq")).bind(&payload).execute(&mut **transaction).await?;
        let stored = sqlx::query(
            "SELECT run_id,payload,event_at,available_at FROM mkt_event WHERE env=$1 AND tenant_id=$2
            AND run_id=$3 AND source=$4 AND source_event_id=$5 AND revision=$6",
        )
        .bind(&input.target_scope.env)
        .bind(input.target_scope.tenant_id)
        .bind(input.target_run)
        .bind(input.source)
        .bind(input.source_event_id)
        .bind(input.revision as i32)
        .fetch_one(&mut **transaction)
        .await?;
        ensure!(
            stored.get::<Uuid, _>("run_id") == *input.target_run
                && stored.get::<Value, _>("payload") == payload
                && stored.get::<DateTime<Utc>, _>("event_at") == source.get::<DateTime<Utc>, _>("event_at")
                && stored.get::<DateTime<Utc>, _>("available_at") == source.get::<DateTime<Utc>, _>("available_at"),
            "replay fact identity collision"
        );
        Ok(())
    }

    pub(crate) fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}
