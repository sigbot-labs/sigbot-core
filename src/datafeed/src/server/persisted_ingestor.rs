// SPDX-License-Identifier: GPL-3.0-only
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use serde_json::Value;
use sigbot_messager::{
    client::{messager_factory::ISigbotMessagerClient, outbox_relay::OutboxRelay},
    storage::postgres::PostgresMessageStore,
};
use sigbot_types::modules::datafeed::event::DatafeedEvent;
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageKind, TraceContext},
    topic::{MessageScope, WorkflowTopic},
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::{collections::HashMap, sync::Arc};

/// Datafeed publication identity; shared by polling and active query transactions.
pub(crate) struct DatafeedPublication<'a> {
    pub scope: &'a MessageScope,
    pub workflow_id: i64,
    pub node_id: i64,
    pub run_id: &'a str,
    pub producer: &'a str,
    pub traceparent: &'a str,
}

#[derive(Clone)]
pub struct PersistedDatafeedIngestor {
    pool: PgPool,
    scope: MessageScope,
    workflow_id: i64,
    node_id: i64,
    run_id: String,
    traceparent: String,
    producer: String,
    relay: Arc<tokio::sync::Mutex<Option<OutboxRelay>>>,
}

impl PersistedDatafeedIngestor {
    pub async fn from_properties(properties: &HashMap<String, String>, provider: &str) -> Result<Self> {
        let get = |key: &str| {
            properties
                .get(&format!("{provider}_{key}"))
                .or_else(|| properties.get(key))
                .cloned()
                .with_context(|| format!("missing datafeed property {key}"))
        };
        let scope = MessageScope {
            env: get("env")?,
            tenant_id: get("tenant_id")?.parse()?,
            space_id: get("space_id")?.parse()?,
        };
        scope.validate()?;
        let database_url = std::env::var(get("database_url_env")?)
            .context("datafeed database credential environment variable missing")?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await?;
        let traceparent = get("traceparent")?;
        TraceContext::parse(&traceparent)?;
        let producer = format!(
            "datafeed:{}:{}:{}:{}:{}:{provider}",
            scope.env,
            scope.tenant_id,
            scope.space_id,
            get("workflow_id")?,
            get("node_id")?
        );
        let instance = Self {
            pool,
            scope,
            workflow_id: get("workflow_id")?.parse()?,
            node_id: get("node_id")?.parse()?,
            run_id: get("run_id")?,
            traceparent,
            producer,
            relay: Arc::new(tokio::sync::Mutex::new(None)),
        };
        ensure!(
            instance.workflow_id > 0 && instance.node_id > 0 && !instance.run_id.is_empty(),
            "invalid feed identity"
        );
        Ok(instance)
    }

    pub async fn ingest(&self, bytes: &[u8]) -> Result<()> {
        let event: DatafeedEvent = serde_json::from_slice(bytes)?;
        let mut tx = self.pool.begin().await?;
        let publication = DatafeedPublication {
            scope: &self.scope,
            workflow_id: self.workflow_id,
            node_id: self.node_id,
            run_id: &self.run_id,
            producer: &self.producer,
            traceparent: &self.traceparent,
        };
        Self::persist_in(&mut tx, &publication, event).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Caller commits the source fact, delivery mapping, feed Outbox and its own state together.
    pub(crate) async fn persist_in(
        tx: &mut Transaction<'_, Postgres>,
        publication: &DatafeedPublication<'_>,
        mut event: DatafeedEvent,
    ) -> Result<DatafeedEvent> {
        ensure!(event.revision >= 0, "negative source revision");
        ensure!(
            event.event_time <= event.available_time,
            "source fact available before event time"
        );
        let raw = event.raw.clone();
        sqlx::query("INSERT INTO market_event(env,tenant_id,space_id,source,source_event_id,symbol,event_type,event_at,available_at,seq,payload,run_id,revision)
            VALUES($1,$2,$3,$4,$5,$6,$12,$7,$8,$9,$10,$11,$13) ON CONFLICT DO NOTHING")
            .bind(&publication.scope.env).bind(publication.scope.tenant_id).bind(publication.scope.space_id)
            .bind(&event.source).bind(&event.source_event_id).bind(&event.symbol)
            .bind(event.event_time).bind(event.available_time).bind(event.seq).bind(&raw).bind(publication.run_id).bind(&event.event_type).bind(event.revision)
            .execute(&mut **tx).await?;
        let existing = sqlx::query(
            "SELECT payload,available_at,event_at,symbol,event_type,seq FROM market_event WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND source=$4 AND source_event_id=$5 AND revision=$6 FOR UPDATE",
        )
        .bind(&publication.scope.env)
        .bind(publication.scope.tenant_id)
        .bind(publication.scope.space_id)
        .bind(&event.source)
        .bind(&event.source_event_id)
        .bind(event.revision)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            existing.get::<Value, _>("payload") == raw
                && existing.get::<String, _>("symbol") == event.symbol
                && existing.get::<String, _>("event_type") == event.event_type,
            "source event identity reused with changed payload; explicit revision required"
        );
        event.available_time = existing.get::<DateTime<Utc>, _>("available_at");
        event.event_time = existing.get::<DateTime<Utc>, _>("event_at");
        event.seq = existing.get("seq");
        let delivered: Option<Value> = sqlx::query_scalar(
            "SELECT payload FROM market_event_delivery WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND node_id=$4 AND run_id=$5 AND source=$6 AND source_event_id=$7 AND revision=$8",
        )
        .bind(&publication.scope.env)
        .bind(publication.scope.tenant_id)
        .bind(publication.scope.space_id)
        .bind(publication.node_id)
        .bind(publication.run_id)
        .bind(&event.source)
        .bind(&event.source_event_id)
        .bind(event.revision)
        .fetch_optional(&mut **tx)
        .await?;
        let durable = if let Some(payload) = delivered {
            let saved: DurableMessage<Value> = serde_json::from_value(payload)?;
            ensure!(
                saved.workflow_id == Some(publication.workflow_id),
                "market delivery identity belongs to another Workflow"
            );
            saved
        } else {
            DurableMessage {
                version: 2,
                scope: publication.scope.clone(),
                workflow_id: Some(publication.workflow_id),
                account_id: None,
                node_id: publication.node_id,
                target_node_id: None,
                run_id: publication.run_id.to_owned(),
                seq: PostgresMessageStore::next_sequence(
                    tx,
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
                traceparent: Self::next_traceparent(publication.traceparent)?,
                payload: serde_json::to_value(&event)?,
            }
        };
        sqlx::query("INSERT INTO market_event_delivery(env,tenant_id,space_id,workflow_id,node_id,run_id,source,source_event_id,revision,message_seq,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING")
            .bind(&publication.scope.env).bind(publication.scope.tenant_id).bind(publication.scope.space_id).bind(publication.workflow_id).bind(publication.node_id).bind(publication.run_id)
            .bind(&event.source).bind(&event.source_event_id).bind(event.revision).bind(durable.seq).bind(serde_json::to_value(&durable)?).execute(&mut **tx).await?;
        let kind = match event.event_type.as_str() {
            "news" => WorkflowTopic::FeedNews,
            "market" => WorkflowTopic::FeedMarket,
            _ => anyhow::bail!("unsupported source event type"),
        };
        let topic = kind.topic(publication.scope, publication.workflow_id)?;
        PostgresMessageStore::enqueue(tx, publication.producer, &topic, &durable).await?;
        // Venue trade sequences are instrument-local. Existing source-only checkpoints
        // remain historical projections; news keeps its established source partition.
        let partition_key = if event.event_type == "market" {
            let native_trades = event.raw.get("price_type").and_then(Value::as_str) == Some("last_trade")
                && matches!(
                    event.raw.get("provider").and_then(Value::as_str),
                    Some("binance_spot" | "hyperliquid_spot" | "hyperliquid_perpetual")
                );
            format!(
                "market:{}:{}{}",
                event.source,
                event.symbol,
                if native_trades { ":last_trade" } else { "" }
            )
        } else {
            event.source.clone()
        };
        sqlx::query("INSERT INTO workflow_checkpoint(env,tenant_id,space_id,workflow_id,node_id,run_id,partition_key,seq,state,watermark,version)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,1)
            ON CONFLICT(env,tenant_id,space_id,workflow_id,node_id,run_id,partition_key) DO UPDATE
            SET seq=GREATEST(workflow_checkpoint.seq,EXCLUDED.seq),watermark=GREATEST(workflow_checkpoint.watermark,EXCLUDED.watermark),
                state=CASE WHEN EXCLUDED.seq>workflow_checkpoint.seq THEN EXCLUDED.state ELSE workflow_checkpoint.state END,
                version=workflow_checkpoint.version+CASE WHEN EXCLUDED.seq>workflow_checkpoint.seq THEN 1 ELSE 0 END,updated_at=NOW()")
            .bind(&publication.scope.env).bind(publication.scope.tenant_id).bind(publication.scope.space_id)
            .bind(publication.workflow_id).bind(publication.node_id).bind(publication.run_id).bind(&partition_key)
            .bind(event.seq).bind(serde_json::json!({"source_event_id": event.source_event_id}))
            .bind(event.event_time).execute(&mut **tx).await?;
        Ok(event)
    }

    pub async fn start_relay(&self, messager: Arc<dyn ISigbotMessagerClient>) -> Result<()> {
        let mut relay = self.relay.lock().await;
        ensure!(relay.is_none(), "datafeed outbox relay already started");
        *relay = Some(OutboxRelay::start(
            PostgresMessageStore::new(self.pool.clone()),
            self.producer.clone(),
            messager,
        ));
        Ok(())
    }

    fn next_traceparent(previous: &str) -> Result<String> {
        TraceContext::parse(previous)?;
        Ok(MessageTrace::child("datafeed.publish", previous).traceparent())
    }
}
