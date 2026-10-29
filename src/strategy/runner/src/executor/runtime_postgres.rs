// SPDX-License-Identifier: GPL-3.0-only
//! Atomic inbox/checkpoint/effects persistence for the SDK's serialized runtime.

use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::{json, Value};
use sigbot_logservice::manager::archive::{LogAppend, LogArchive, LogLine};
use sigbot_messager::storage::postgres::PostgresMessageStore;
use sigbot_strategy_sdk::sdk::runtime::{StrategyCheckpoint, StrategyIdentity, StrategyLog, StrategyTransaction};
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{AccountTopic, WorkflowEdge, WorkflowTopic},
    },
    strategy::runtime::{Effects, StrategyRequest},
};
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;

pub type ProducerTrace = Arc<dyn Fn(&str) -> Result<String> + Send + Sync>;

pub struct PostgresStrategyTransaction {
    pool: PgPool,
    edges: Vec<WorkflowEdge>,
    producer_trace: ProducerTrace,
    log_archive: Option<Arc<LogArchive>>,
    virtual_clock: bool,
}

impl PostgresStrategyTransaction {
    pub fn producer(identity: &StrategyIdentity) -> String {
        format!(
            "strategy:{}:{}:{}:{}:{}",
            identity.scope.env,
            identity.scope.tenant_id,
            identity.scope.space_id,
            identity.workflow_id,
            identity.node_id
        )
    }
    pub fn new(pool: PgPool, edges: Vec<WorkflowEdge>, producer_trace: ProducerTrace) -> Self {
        Self {
            pool,
            edges,
            producer_trace,
            log_archive: None,
            virtual_clock: false,
        }
    }

    pub fn with_log_archive(mut self, archive: Arc<LogArchive>) -> Self {
        self.log_archive = Some(archive);
        self
    }

    pub fn with_replay_clock(mut self) -> Self {
        self.virtual_clock = true;
        self
    }

    fn input_topic(&self, identity: &StrategyIdentity, input: &DurableMessage<Value>) -> Result<String> {
        let kind = if input.payload.get("probabilities").is_some() {
            WorkflowTopic::EvaluationSnapshot
        } else {
            match input
                .payload
                .pointer("/data/kind")
                .and_then(Value::as_str)
                .context("typed strategy event required")?
            {
                "market" => WorkflowTopic::FeedMarket,
                "news" => WorkflowTopic::FeedNews,
                "snapshot" => WorkflowTopic::FeedSnapshotResult,
                "compute" => WorkflowTopic::ComputeResult,
                "query_result" => match input.payload.pointer("/data/value/operation").and_then(Value::as_str) {
                    Some("notification") => WorkflowTopic::NotificationResult,
                    Some("export") => WorkflowTopic::ExportResult,
                    _ => anyhow::bail!("unsupported external result operation"),
                },
                "query_failure" => match input.payload.pointer("/data/value/operation").and_then(Value::as_str) {
                    Some("snapshot") => WorkflowTopic::FeedSnapshotResult,
                    Some("compute") => WorkflowTopic::ComputeResult,
                    _ => anyhow::bail!("unknown failed query operation"),
                },
                "timer" => WorkflowTopic::RuntimeEvent,
                "execution" => {
                    return AccountTopic::ExecutionEvent
                        .topic(&input.scope, input.account_id.context("execution account required")?)
                }
                "portfolio" => {
                    return AccountTopic::PortfolioSnapshot
                        .topic(&input.scope, input.account_id.context("portfolio account required")?)
                }
                "order_status" => {
                    return AccountTopic::OrderEvent
                        .topic(&input.scope, input.account_id.context("order account required")?)
                }
                _ => anyhow::bail!("unsupported strategy event"),
            }
        };
        let self_timer = kind == WorkflowTopic::RuntimeEvent && input.node_id == identity.node_id;
        ensure!(
            self_timer
                || self
                    .edges
                    .iter()
                    .any(|edge| edge.permits(input.node_id, identity.node_id, kind)),
            "workflow edge does not authorize this delivery"
        );
        kind.topic(&input.scope, identity.workflow_id)
    }

    async fn output(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        identity: &StrategyIdentity,
        input: &DurableMessage<Value>,
        kind: WorkflowTopic,
        payload: Value,
        account_id: Option<i64>,
    ) -> Result<i64> {
        let seq = PostgresMessageStore::next_sequence(tx, &identity.scope, identity.node_id, &identity.run_id).await?;
        let outgoing = DurableMessage {
            version: 2,
            scope: identity.scope.clone(),
            workflow_id: Some(identity.workflow_id),
            account_id,
            node_id: identity.node_id,
            target_node_id: None,
            run_id: identity.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: if matches!(
                kind,
                WorkflowTopic::FeedSnapshotRequest
                    | WorkflowTopic::ComputeRequest
                    | WorkflowTopic::NotificationRequest
                    | WorkflowTopic::ExportRequest
            ) {
                MessageKind::Request
            } else {
                MessageKind::Event
            },
            event_time: input.event_time,
            available_time: input.available_time,
            produced_at: if self.virtual_clock {
                input.available_time
            } else {
                Utc::now()
            },
            traceparent: (self.producer_trace)(&input.traceparent)?,
            payload,
        };
        PostgresMessageStore::enqueue(
            tx,
            &Self::producer(identity),
            &kind.topic(&identity.scope, identity.workflow_id)?,
            &outgoing,
        )
        .await?;
        Ok(seq)
    }
}

#[async_trait]
impl StrategyTransaction for PostgresStrategyTransaction {
    async fn record_logs(
        &self,
        identity: &StrategyIdentity,
        input: &DurableMessage<Value>,
        logs: Vec<StrategyLog>,
    ) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let archive = self
            .log_archive
            .as_ref()
            .context("durable log_archive required for Python output or exceptions")?;
        for log in logs {
            let message = DurableMessage {
                version: 2,
                scope: identity.scope.clone(),
                workflow_id: Some(identity.workflow_id),
                account_id: input.account_id,
                node_id: identity.node_id,
                target_node_id: None,
                run_id: identity.run_id.clone(),
                seq: 0, // Assigned atomically with the log's SQLite WAL record.
                attempt_no: input.attempt_no,
                kind: MessageKind::Event,
                event_time: input.event_time,
                available_time: input.available_time,
                produced_at: Utc::now(),
                traceparent: (self.producer_trace)(&input.traceparent)?,
                payload: LogLine {
                    level: log.level,
                    source: log.source,
                    content: log.content,
                },
            };
            archive.append_generated(&mut LogAppend::new(message)).await?;
        }
        Ok(())
    }
    async fn load(&self, identity: &StrategyIdentity) -> Result<StrategyCheckpoint> {
        let state: Option<Value> = sqlx::query_scalar(
            "SELECT state FROM workflow_checkpoint WHERE env=$1 AND tenant_id=$2
            AND space_id=$3 AND workflow_id=$4 AND node_id=$5 AND run_id=$6 AND partition_key='decision'",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.scope.space_id)
        .bind(identity.workflow_id)
        .bind(identity.node_id)
        .bind(&identity.run_id)
        .fetch_optional(&self.pool)
        .await?;
        state
            .map(serde_json::from_value)
            .transpose()
            .map(|state| state.unwrap_or_default())
            .map_err(Into::into)
    }

    async fn processed(&self, identity: &StrategyIdentity, input: &DurableMessage<Value>) -> Result<bool> {
        let existing: Option<Value> = sqlx::query_scalar(
            "SELECT payload FROM messager_inbox WHERE env=$1 AND tenant_id=$2
            AND space_id=$3 AND consumer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7 AND state='done'",
        )
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(input.scope.space_id)
        .bind(format!("strategy.{}", identity.node_id))
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(mut existing) = existing {
            let mut proposed = serde_json::to_value(input)?;
            for field in ["attempt_no", "traceparent", "produced_at"] {
                existing
                    .as_object_mut()
                    .context("invalid persisted strategy input")?
                    .remove(field);
                proposed
                    .as_object_mut()
                    .context("invalid strategy input")?
                    .remove(field);
            }
            ensure!(
                existing == proposed,
                "processed strategy identity reused with different input"
            );
            return Ok(true);
        }
        Ok(false)
    }

    async fn commit(
        &self,
        identity: &StrategyIdentity,
        input: &DurableMessage<Value>,
        expected_version: i64,
        checkpoint: StrategyCheckpoint,
        effects: Effects,
    ) -> Result<bool> {
        let topic = self.input_topic(identity, input)?;
        let mut tx = self.pool.begin().await?;
        let consumer = format!("strategy.{}", identity.node_id);
        if !PostgresMessageStore::accept(
            &mut tx,
            &consumer,
            &topic,
            &MessageEnvelope::from_durable(input.clone())?,
        )
        .await?
        {
            tx.commit().await?;
            return Ok(false);
        }
        ensure!(
            checkpoint.version == expected_version + 1,
            "checkpoint version must advance once"
        );
        let changed = sqlx::query("INSERT INTO workflow_checkpoint(env,tenant_id,space_id,workflow_id,node_id,run_id,
            partition_key,seq,state,watermark,version) VALUES($1,$2,$3,$4,$5,$6,'decision',$7,$8,$9,$10)
            ON CONFLICT(env,tenant_id,space_id,workflow_id,node_id,run_id,partition_key)
            DO UPDATE SET seq=EXCLUDED.seq,state=EXCLUDED.state,watermark=GREATEST(workflow_checkpoint.watermark,EXCLUDED.watermark),
                version=EXCLUDED.version,updated_at=NOW() WHERE workflow_checkpoint.version=$11")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id)
            .bind(identity.workflow_id).bind(identity.node_id).bind(&identity.run_id).bind(input.seq)
            .bind(serde_json::to_value(&checkpoint)?).bind(input.available_time).bind(checkpoint.version).bind(expected_version)
            .execute(&mut *tx).await?.rows_affected();
        ensure!(
            changed == 1,
            "strategy owner/checkpoint changed; retry from durable state"
        );
        for intent in effects.intents {
            self.output(
                &mut tx,
                identity,
                input,
                WorkflowTopic::StrategySignal,
                serde_json::to_value(&intent)?,
                Some(intent.account_id),
            )
            .await?;
        }
        for request in effects.requests {
            let (kind, payload) = match request {
                StrategyRequest::Snapshot(query) => (WorkflowTopic::FeedSnapshotRequest, serde_json::to_value(query)?),
                StrategyRequest::Compute(plan) => (WorkflowTopic::ComputeRequest, serde_json::to_value(plan)?),
                StrategyRequest::Notification(request) => {
                    (WorkflowTopic::NotificationRequest, serde_json::to_value(request)?)
                }
                StrategyRequest::Export(batch) => (WorkflowTopic::ExportRequest, serde_json::to_value(batch)?),
            };
            self.output(&mut tx, identity, input, kind, payload, input.account_id)
                .await?;
        }
        for timer in effects.timers {
            if self.virtual_clock {
                let seq =
                    PostgresMessageStore::next_sequence(&mut tx, &identity.scope, identity.node_id, &identity.run_id)
                        .await?;
                sqlx::query("INSERT INTO workflow_timer(env,tenant_id,space_id,workflow_id,node_id,run_id,seq,scheduled_at,payload,traceparent)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
                    .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(identity.workflow_id)
                    .bind(identity.node_id).bind(&identity.run_id).bind(seq).bind(timer.at).bind(timer.payload).bind(&input.traceparent)
                    .execute(&mut *tx).await?;
                continue;
            }
            let seq = self
                .output(
                    &mut tx,
                    identity,
                    input,
                    WorkflowTopic::RuntimeEvent,
                    json!({
                        "source":"timer","symbol":null,"event_time":timer.at,"available_time":timer.at,
                        "data":{"kind":"timer","value":{"scheduled_at":timer.at,"payload":timer.payload}}
                    }),
                    input.account_id,
                )
                .await?;
            sqlx::query(
                "UPDATE messager_outbox SET available_at=$7 WHERE env=$1 AND tenant_id=$2 AND space_id=$3
                AND producer=$8 AND node_id=$4 AND run_id=$5 AND seq=$6",
            )
            .bind(&identity.scope.env)
            .bind(identity.scope.tenant_id)
            .bind(identity.scope.space_id)
            .bind(identity.node_id)
            .bind(&identity.run_id)
            .bind(seq)
            .bind(timer.at)
            .bind(Self::producer(identity))
            .execute(&mut *tx)
            .await?;
        }
        PostgresMessageStore::complete(&mut tx, &consumer, input).await?;
        tx.commit().await?;
        Ok(true)
    }
}
