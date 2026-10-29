// SPDX-License-Identifier: GPL-3.0-only
use crate::manager::sink::{ExportBatch, ExportSinkConfig, ExportSinkFactory, IExportSink};
use anyhow::{ensure, Context, Result};
use common_telemetry::tracing_context::MessageTrace;
use serde::Deserialize;
use serde_json::Value;
use sigbot_messager::{
    client::{messager_factory::ISigbotMessagerClient, outbox_relay::OutboxRelay},
    storage::postgres::PostgresMessageStore,
};
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageEnvelope, MessageKind},
    topic::{MessageScope, WorkflowTopic},
};
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportNodeConfig {
    pub scope: MessageScope,
    pub workflow_id: i64,
    pub node_id: i64,
    pub input_node_ids: Vec<i64>,
    pub connection_id: i64,
    pub database_url_env: String,
    pub sink: ExportSinkConfig,
}

pub struct ExportRuntime {
    pool: PgPool,
    config: ExportNodeConfig,
    sink: Arc<dyn IExportSink>,
    relay: Mutex<Option<OutboxRelay>>,
}

impl ExportRuntime {
    pub async fn from_properties(properties: &HashMap<String, String>) -> Result<Arc<Self>> {
        let config: ExportNodeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("export runtime_config required")?,
        )?;
        config.scope.validate()?;
        ensure!(
            config.workflow_id > 0
                && config.node_id > 0
                && config.connection_id > 0
                && !config.input_node_ids.is_empty(),
            "export identity/edges required"
        );
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&std::env::var(&config.database_url_env)?)
            .await?;
        let connection: Option<i64> =
            sqlx::query_scalar("SELECT id FROM connection WHERE id=$1 AND env=$2 AND tenant_id=$3 AND space_id=$4")
                .bind(config.connection_id)
                .bind(&config.scope.env)
                .bind(config.scope.tenant_id)
                .bind(config.scope.space_id)
                .fetch_optional(&pool)
                .await?;
        ensure!(connection.is_some(), "export connection is outside this scope");
        Ok(Arc::new(Self {
            pool,
            sink: ExportSinkFactory::create(&config.sink)?,
            config,
            relay: Mutex::new(None),
        }))
    }

    pub async fn start(self: &Arc<Self>, messager: Arc<dyn ISigbotMessagerClient>) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("exporter", false);
        let topic = WorkflowTopic::ExportRequest.topic(&self.config.scope, self.config.workflow_id)?;
        let runtime = self.clone();
        messager
            .subscribe(
                &topic,
                Arc::new(move |bytes| {
                    let runtime = runtime.clone();
                    Box::pin(async move {
                        runtime.consume(&bytes).await?;
                        Ok("committed".into())
                    })
                }),
            )
            .await?;
        *self.relay.lock().await = Some(OutboxRelay::start(
            PostgresMessageStore::new(self.pool.clone()),
            self.producer(),
            messager,
        ));
        common_telemetry::readiness::RuntimeReadiness::business("exporter", true);
        Ok(())
    }

    pub async fn consume(&self, bytes: &[u8]) -> Result<()> {
        let mut value: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        PostgresMessageStore::new(self.pool.clone())
            .restore_legacy_workflow(&mut value)
            .await?;
        let wire: MessageEnvelope<ExportBatch> = serde_json::from_value(serde_json::to_value(&value)?)?;
        wire.validate()?;
        let input = &wire.durable;
        ensure!(input.kind == MessageKind::Request, "export expects a request envelope");
        ensure!(
            input.scope == self.config.scope && input.workflow_id == Some(self.config.workflow_id),
            "export scope mismatch"
        );
        if !self.config.input_node_ids.contains(&input.node_id)
            || input.target_node_id.is_some_and(|id| id != self.config.node_id)
        {
            let topic = WorkflowTopic::ExportRequest.topic(&self.config.scope, self.config.workflow_id)?;
            let consumer = format!("{}:routing", self.producer());
            let mut tx = self.pool.begin().await?;
            if PostgresMessageStore::accept(&mut tx, &consumer, &topic, &value).await? {
                sqlx::query("UPDATE messager_inbox SET state='quarantined',completed_at=NOW(),quarantine_reason='outside_export_route'
                    WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND consumer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7")
                    .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.scope.space_id).bind(consumer)
                    .bind(input.node_id).bind(&input.run_id).bind(input.seq).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            return Ok(());
        }
        let mut batch = input.payload.clone();
        batch.key = format!(
            "{}/{}/{}/{}/{}/{}",
            input.scope.env, input.scope.tenant_id, input.scope.space_id, input.run_id, input.node_id, input.seq
        );
        batch.validate()?;
        let mut tx = self.pool.begin().await?;
        let consumer = self.producer();
        let topic = WorkflowTopic::ExportRequest.topic(&self.config.scope, self.config.workflow_id)?;
        if !PostgresMessageStore::accept(&mut tx, &consumer, &topic, &value).await? {
            tx.commit().await?;
            return Ok(());
        }
        // Hold this scoped Inbox row while writing. Concurrent duplicates cannot race.
        // Crash after delivery reuses the same Kafka key / deterministic Sheets range.
        self.sink.write(&batch).await?;
        let partition = input.node_id.to_string();
        sqlx::query("INSERT INTO export_checkpoint(env,tenant_id,space_id,connection_id,run_id,partition_key,seq,cursor)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(env,tenant_id,space_id,connection_id,run_id,partition_key)
            DO UPDATE SET seq=GREATEST(export_checkpoint.seq,EXCLUDED.seq),
            cursor=CASE WHEN EXCLUDED.seq>export_checkpoint.seq THEN EXCLUDED.cursor ELSE export_checkpoint.cursor END,updated_at=NOW()")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.scope.space_id).bind(self.config.connection_id)
            .bind(&input.run_id).bind(partition).bind(input.seq).bind(serde_json::json!({"seq":input.seq,"range":batch.range}))
            .execute(&mut *tx).await?;
        let now = chrono::Utc::now();
        let seq =
            PostgresMessageStore::next_sequence(&mut tx, &input.scope, self.config.node_id, &input.run_id).await?;
        let output = DurableMessage {
            version: 2,
            scope: input.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: input.account_id,
            node_id: self.config.node_id,
            target_node_id: Some(input.node_id),
            run_id: input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Result,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: MessageTrace::child("export.publish", &input.traceparent).traceparent(),
            payload: serde_json::json!({"rows":batch.rows.len(),"source_node_id":input.node_id,"source_seq":input.seq}),
        };
        if let Some(span) = wire.request_span_id.as_deref() {
            sigbot_messager::client::reply_pairing::ReplyPairingCache::shared().remember(&output, span)?;
        }
        PostgresMessageStore::enqueue(
            &mut tx,
            &consumer,
            &WorkflowTopic::ExportResult.topic(&input.scope, self.config.workflow_id)?,
            &output,
        )
        .await?;
        PostgresMessageStore::complete(&mut tx, &consumer, &value.durable).await?;
        tx.commit().await?;
        Ok(())
    }

    fn producer(&self) -> String {
        format!(
            "export:{}:{}:{}:{}:{}",
            self.config.scope.env,
            self.config.scope.tenant_id,
            self.config.scope.space_id,
            self.config.workflow_id,
            self.config.node_id
        )
    }
}
