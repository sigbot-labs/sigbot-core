// SPDX-License-Identifier: GPL-3.0-only

use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_core::config::config::PostgresStorageProperties;
use sigbot_core::modules::messager::handler::messager::MessagerHandler;
pub use sigbot_core::modules::strategy::handler::strategy::ProducerTrace;
use sigbot_core::modules::strategy::handler::strategy::{StrategyHandler, StrategyStorageIdentity};
use sigbot_logservice::s3::s3::{LogAppend, LogLine, S3Logservice};
use sigbot_strategy_sdk::sdk::runtime::{StrategyCheckpoint, StrategyIdentity, StrategyLog, StrategyTransaction};
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope, MessageKind};
use sigbot_types::messager::topic::{AccountTopic, MessageScope, WorkflowEdge, WorkflowTopic};
use sigbot_types::strategy::execution::Effects;
use sigbot_types::Uuid;
use std::sync::Arc;

pub struct CheckpointStrategyTransaction {
    handler: StrategyHandler,
    edges: Vec<WorkflowEdge>,
    producer_trace: ProducerTrace,
    log_archive: Option<Arc<S3Logservice>>,
}

impl CheckpointStrategyTransaction {
    pub fn producer(identity: &StrategyIdentity) -> String {
        StrategyHandler::producer(Self::identity(identity))
    }

    pub async fn connect(
        storage: &PostgresStorageProperties,
        edges: Vec<WorkflowEdge>,
        producer_trace: ProducerTrace,
        virtual_clock: bool,
    ) -> Result<Self> {
        let mut handler = StrategyHandler::connect(storage, producer_trace.clone()).await?;
        if virtual_clock {
            handler = handler.with_replay_clock();
        }
        Ok(Self {
            handler,
            edges,
            producer_trace,
            log_archive: None,
        })
    }

    pub fn with_log_archive(mut self, archive: Arc<S3Logservice>) -> Self {
        self.log_archive = Some(archive);
        self
    }

    pub fn messager(&self) -> MessagerHandler {
        self.handler.messager()
    }

    pub async fn replay_clock(
        &self,
        identity: &StrategyIdentity,
        input: &DurableMessage<Value>,
    ) -> Result<DateTime<Utc>> {
        self.handler.replay_clock(Self::identity(identity), input).await
    }

    pub async fn quarantine(
        &self,
        consumer: &str,
        topic: &str,
        wire: &MessageEnvelope<Value>,
        reason: &str,
    ) -> Result<bool> {
        self.handler.quarantine(consumer, topic, wire, reason).await
    }

    pub async fn portfolio(&self, scope: &MessageScope, account_id: Uuid) -> Result<Value> {
        self.handler.portfolio(scope, account_id).await
    }

    fn identity(identity: &StrategyIdentity) -> StrategyStorageIdentity<'_> {
        StrategyStorageIdentity {
            scope: &identity.scope,
            workflow_id: identity.workflow_id,
            node_id: identity.node_id,
            run_id: &identity.run_id,
        }
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
}

#[async_trait]
impl StrategyTransaction for CheckpointStrategyTransaction {
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
                seq: 0,
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
        self.handler
            .load(Self::identity(identity))
            .await?
            .map(serde_json::from_value)
            .transpose()
            .map(|value| value.unwrap_or_default())
            .map_err(Into::into)
    }

    async fn processed(&self, identity: &StrategyIdentity, input: &DurableMessage<Value>) -> Result<bool> {
        self.handler.processed(Self::identity(identity), input).await
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
        self.handler
            .commit(
                Self::identity(identity),
                &topic,
                input,
                expected_version,
                checkpoint.version,
                serde_json::to_value(checkpoint)?,
                effects,
            )
            .await
    }
}
