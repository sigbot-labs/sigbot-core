// SPDX-License-Identifier: GPL-3.0-only

use crate::logservice::ILogservice;
use crate::s3::s3::LogArchiveConfig;
use crate::s3::s3::LogLine;
use crate::s3::s3::S3Logservice;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use common_context::ApplicationContext;
use common_context::IProvider;
use serde::Deserialize;
use sigbot_core::config::config::{get_config, PostgresStorageProperties};
use sigbot_core::modules::logservice::handler::logservice::LogHandler;
use sigbot_messager::messager::IMessager;
use sigbot_messager::server::MessagerServer;
use sigbot_types::logservice::LogserviceArgument;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::ArgumentConfig;
use sigbot_types::Uuid;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogserviceNodeConfig {
    scope: MessageScope,
    workflow_id: Uuid,
    node_id: i64,
    run_id: Uuid,
    input_node_ids: Vec<i64>,
    archive: LogArchiveConfig,
    archive_interval_ms: u64,
}

impl LogserviceNodeConfig {
    fn from_properties(properties: &HashMap<String, String>) -> Result<Self> {
        let config: Self = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("logservice runtime_config required")?,
        )?;
        config.scope.validate()?;
        ensure!(
            config.workflow_id.get_version_num() == 7
                && config.node_id > 0
                && config.run_id.get_version_num() == 7
                && !config.input_node_ids.is_empty()
                && config
                    .input_node_ids
                    .iter()
                    .all(|source| *source > 0 && *source != config.node_id),
            "logservice identity and incoming Workflow edges are required"
        );
        ensure!(
            (100..=60_000).contains(&config.archive_interval_ms),
            "logservice archive interval must be between 100ms and 60s"
        );
        Ok(config)
    }
}

pub struct SigbotLogServer {
    archive: Arc<dyn ILogservice>,
    archiver: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl SigbotLogServer {
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let encoded = matches
            .get_one::<String>("LOG_MANAGER_CONFIGURATION")
            .expect("logservice configuration required");
        let argument = Arc::new(
            LogserviceArgument::from_json(
                &ArgumentConfig::decode(encoded).expect("invalid logservice configuration encoding"),
            )
            .expect("invalid logservice configuration"),
        );
        let properties = argument
            .properties
            .as_ref()
            .expect("logservice runtime properties required");
        let config = LogserviceNodeConfig::from_properties(properties).expect("invalid logservice runtime");
        let handler = LogHandler::configured(
            &get_config().storage.postgres,
            &config.archive.spool_path,
            config.archive.max_buffer_bytes,
        )
        .await
        .expect("logservice index connection failed");
        let key = sigbot_utils::providers::ProviderInstanceId::configured("logservice", &*argument)
            .expect("invalid logservice identity");
        Self::archive_with_key(&key, &config.archive, handler)
            .await
            .expect("logservice archive initialization failed");
        let context = ApplicationContext::global();
        context
            .register_with::<dyn ILogservice, S3Logservice>(&key, |archive| Ok(archive as Arc<dyn ILogservice>))
            .expect("logservice interface registration failed");
        let service = context
            .get::<dyn ILogservice>(&key)
            .await
            .expect("logservice interface initialization failed");
        let messager = MessagerServer::connect(matches, argument.messager_config.clone())
            .await
            .expect("logservice messager initialization failed");
        let owner_key = key.clone();
        context
            .register_with_shutdown_phase::<Self, _>(&owner_key, 20, move || {
                let service = service.clone();
                let messager = messager.clone();
                let config = config.clone();
                async move { Self::start(service, messager, config).await }
            })
            .expect("logservice worker registration failed");
        context
            .get::<Self>(&key)
            .await
            .expect("logservice worker initialization failed");
    }

    async fn start(
        archive: Arc<dyn ILogservice>,
        messager: Arc<dyn IMessager>,
        config: LogserviceNodeConfig,
    ) -> Result<Arc<Self>> {
        common_telemetry::readiness::RuntimeReadiness::business("logservice", false);
        let topic = WorkflowTopic::LogEvent.topic(&config.scope, config.workflow_id)?;
        let expected_topic = topic.clone();
        let input_node_ids = config.input_node_ids.clone();
        let scope = config.scope.clone();
        let run_id = config.run_id.clone();
        let workflow_id = config.workflow_id;
        let node_id = config.node_id;
        let writer = archive.clone();
        messager
            .subscribe(
                &topic,
                Arc::new(move |bytes| {
                    let writer = writer.clone();
                    let expected_topic = expected_topic.clone();
                    let input_node_ids = input_node_ids.clone();
                    let scope = scope.clone();
                    let run_id = run_id.clone();
                    Box::pin(async move {
                        let message: DurableMessage<LogLine> = serde_json::from_slice(&bytes)?;
                        message.validate_topic(&expected_topic)?;
                        ensure!(
                            message.kind == MessageKind::Event
                                && message.scope == scope
                                && message.workflow_id == Some(workflow_id)
                                && message.run_id == run_id,
                            "log event isolation mismatch"
                        );
                        if !input_node_ids.contains(&message.node_id)
                            || message.target_node_id.is_some_and(|target| target != node_id)
                        {
                            return Ok("edge ignored".to_owned());
                        }
                        writer.append(&message).await?;
                        Ok("durable".to_owned())
                    })
                }),
            )
            .await?;
        let interval = Duration::from_millis(config.archive_interval_ms);
        let worker = archive.clone();
        let archiver = tokio::spawn(async move {
            loop {
                match worker.archive_pending(64).await {
                    Ok(0) => tokio::time::sleep(interval).await,
                    Ok(_) => {}
                    Err(error) => {
                        common_telemetry::warn!(%error, "log archive copy delayed");
                        tokio::time::sleep(interval).await;
                    }
                }
            }
        });
        common_telemetry::readiness::RuntimeReadiness::business("logservice", true);
        Ok(Arc::new(Self {
            archive,
            archiver: Mutex::new(Some(archiver)),
        }))
    }

    pub async fn archive(config: &LogArchiveConfig, storage: &PostgresStorageProperties) -> Result<Arc<S3Logservice>> {
        let key = sigbot_utils::providers::ProviderInstanceId::configured("s3_logs", config)?;
        let handler = LogHandler::configured(storage, &config.spool_path, config.max_buffer_bytes).await?;
        Self::archive_with_key(&key, config, handler).await
    }

    async fn archive_with_key(key: &str, config: &LogArchiveConfig, handler: LogHandler) -> Result<Arc<S3Logservice>> {
        let config = config.clone();
        let context = ApplicationContext::global();
        context.register_if_absent::<S3Logservice, _>(key, move || {
            let config = config.clone();
            let handler = handler.clone();
            async move { Ok(Arc::new(S3Logservice::new(&config, handler).await?)) }
        })?;
        context.get::<S3Logservice>(key).await
    }
}

#[async_trait::async_trait]
impl IProvider for SigbotLogServer {
    async fn close(&self) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("logservice", false);
        if let Some(task) = self.archiver.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.archive.archive_pending(64).await? == 0 {
                    return Ok::<_, anyhow::Error>(());
                }
            }
        })
        .await;
        Ok(())
    }
}
