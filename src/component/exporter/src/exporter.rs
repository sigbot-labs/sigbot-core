// SPDX-License-Identifier: GPL-3.0-only

use crate::google::google_sheets::GoogleSheetsExporter;
use crate::kafka::kafka::KafkaExporter;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use common_context::ApplicationContext;
use common_context::IProvider;
use common_telemetry::tracing_context::MessageTrace;
use serde::Deserialize;
use serde_json::Value;
use sigbot_core::config::config::get_config;
use sigbot_core::modules::exporter::handler::exporter::ExporterHandler;
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::OutboxRelay;
use sigbot_messager::messager::SubscriptionSet;
pub use sigbot_types::exporter::request::ExportBatch;
use sigbot_types::exporter::ExporterProvider;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::Uuid;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait]
pub trait IExporter: IProvider {
    fn provider(&self) -> ExporterProvider;
    async fn write(&self, batch: &ExportBatch) -> Result<()>;
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportNodeConfig {
    pub scope: MessageScope,
    pub workflow_id: Uuid,
    pub node_id: i64,
    pub input_node_ids: Vec<i64>,
    pub connection_id: Uuid,
    pub sink: ExportSinkConfig,
}

pub struct ExportService {
    handler: ExporterHandler,
    config: ExportNodeConfig,
    sink: Arc<dyn IExporter>,
    relay: Mutex<Option<OutboxRelay>>,
    subscriptions: SubscriptionSet,
}

impl ExportService {
    pub async fn from_properties(properties: &HashMap<String, String>) -> Result<Arc<Self>> {
        let config: ExportNodeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("export runtime_config required")?,
        )?;
        config.scope.validate()?;
        ensure!(
            config.workflow_id.get_version_num() == 7
                && config.node_id > 0
                && config.connection_id.get_version_num() == 7
                && !config.input_node_ids.is_empty(),
            "export identity/edges required"
        );
        let database_url = get_config().storage.postgres.connection_url()?;
        let handler = ExporterHandler::connect(&database_url).await?;
        ensure!(
            handler.connection_exists(config.connection_id, &config.scope).await?,
            "export connection is outside this scope"
        );
        let key = sigbot_utils::providers::ProviderInstanceId::configured("export_destination", &config.sink)?;
        let sink_config = config.sink.clone();
        let context = ApplicationContext::global();
        context.register_if_absent::<dyn IExporter, _>(&key, move || {
            let config = sink_config.clone();
            async move { config.connect() }
        })?;
        let sink = context.get::<dyn IExporter>(&key).await?;
        Ok(Arc::new(Self {
            handler,
            sink,
            config,
            relay: Mutex::new(None),
            subscriptions: SubscriptionSet::default(),
        }))
    }

    pub async fn start(self: &Arc<Self>, messager: Arc<dyn IMessager>) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("exporter", false);
        self.subscriptions.bind(messager.clone())?;
        let topic = WorkflowTopic::ExportRequest.topic(&self.config.scope, self.config.workflow_id)?;
        let runtime = self.clone();
        self.subscriptions
            .subscribe(
                topic,
                format!("{}:request", self.producer()),
                Arc::new(move |bytes| {
                    let runtime = runtime.clone();
                    Box::pin(async move {
                        runtime.consume(&bytes).await?;
                        Ok("committed".into())
                    })
                }),
            )
            .await?;
        *self.relay.lock().await = Some(OutboxRelay::start(self.handler.messager(), self.producer(), messager));
        common_telemetry::readiness::RuntimeReadiness::business("exporter", true);
        Ok(())
    }

    pub async fn consume(&self, bytes: &[u8]) -> Result<()> {
        let value: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
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
            self.handler
                .quarantine(&consumer, &topic, &value, "outside_export_route")
                .await?;
            return Ok(());
        }
        let mut batch = input.payload.clone();
        batch.key = format!(
            "{}/{}/{}/{}/{}",
            input.scope.env, input.scope.tenant_id, input.run_id, input.node_id, input.seq
        );
        batch.validate()?;
        let consumer = self.producer();
        let topic = WorkflowTopic::ExportRequest.topic(&self.config.scope, self.config.workflow_id)?;
        let Some(mut claim) = self.handler.claim(&consumer, &topic, &value).await? else {
            return Ok(());
        };
        // Hold this scoped Inbox row while writing. Concurrent duplicates cannot race.
        // Crash after delivery reuses the same Kafka key / deterministic Sheets range.
        self.sink.write(&batch).await?;
        let partition = input.node_id.to_string();
        let now = chrono::Utc::now();
        let seq = claim
            .next_sequence(&input.scope, self.config.node_id, &input.run_id)
            .await?;
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
            sigbot_messager::messager::ReplyPairingCache::shared().remember(&output, span)?;
        }
        claim
            .complete(
                self.config.connection_id,
                &partition,
                serde_json::json!({"seq":input.seq,"range":batch.range}),
                &WorkflowTopic::ExportResult.topic(&input.scope, self.config.workflow_id)?,
                &output,
            )
            .await
    }

    fn producer(&self) -> String {
        format!(
            "export:{}:{}:{}:{}",
            self.config.scope.env, self.config.scope.tenant_id, self.config.workflow_id, self.config.node_id
        )
    }
}

#[derive(Clone, serde::Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExportSinkConfig {
    Kafka {
        brokers: String,
        topic: String,
        properties: BTreeMap<String, String>,
        #[serde(default)]
        secret_keys: BTreeMap<String, String>,
    },
    GoogleSheets {
        spreadsheet_id: String,
        token_secret: String,
        #[serde(default)]
        proxy_url: Option<String>,
    },
}

impl ExportSinkConfig {
    pub fn connect(&self) -> Result<Arc<dyn IExporter>> {
        match self {
            Self::Kafka {
                brokers,
                topic,
                properties,
                secret_keys,
            } => Ok(KafkaExporter::new(brokers, topic, properties, secret_keys)?),
            Self::GoogleSheets {
                spreadsheet_id,
                token_secret,
                proxy_url,
            } => Ok(GoogleSheetsExporter::new(
                spreadsheet_id,
                token_secret,
                proxy_url.as_deref(),
            )?),
        }
    }
}

impl ExportService {
    pub async fn close(&self) {
        common_telemetry::readiness::RuntimeReadiness::business("exporter", false);
        if let Some(relay) = self.relay.lock().await.take() {
            relay.close();
        }
        if let Err(error) = self.subscriptions.close().await {
            common_telemetry::error!(%error, "export subscription close failed");
        }
    }
}
#[async_trait::async_trait]
impl IProvider for ExportService {
    async fn close(&self) -> anyhow::Result<()> {
        ExportService::close(self).await;
        Ok(())
    }
}
