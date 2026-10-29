// SPDX-License-Identifier: GPL-3.0-only
use super::strategy_node::{StrategyNode, StrategyNodeConfig};
use anyhow::{Context, Result};
use common_telemetry::tracing_context::MessageTrace;
use sigbot_core::{
    config::config::get_config,
    mgmt::apm::logging::{LogRouteGuard, LogRoutes},
    store::postgres::PostgresRepository,
};
use sigbot_logservice::manager::{
    archive::LogArchiveFactory,
    tracing_archive::{ArchiveCapture, LogCaptureOptions, LogStreamIdentity},
};
use sigbot_messager::{
    client::{messager_factory::ISigbotMessagerClient, outbox_relay::OutboxRelay},
    storage::postgres::PostgresMessageStore,
};
use sigbot_types::modules::strategy::SigbotStrategyArgument;
use std::sync::{Arc, Mutex};
use tracing_subscriber::filter::{LevelFilter, Targets};

pub struct StrategyRuntime {
    relay: OutboxRelay,
    archive_task: Option<tokio::task::JoinHandle<()>>,
    capture: Option<Arc<ArchiveCapture>>,
    route: Mutex<Option<LogRouteGuard>>,
    messager: Arc<dyn ISigbotMessagerClient>,
}

impl StrategyRuntime {
    pub async fn start(
        argument: &SigbotStrategyArgument,
        messager: Arc<dyn ISigbotMessagerClient>,
    ) -> Result<Arc<Self>> {
        common_telemetry::readiness::RuntimeReadiness::business("strategy", false);
        let properties = argument
            .properties
            .as_ref()
            .context("strategy runtime properties required")?;
        let config: StrategyNodeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("typed strategy runtime_config required")?,
        )?;
        let repository = PostgresRepository::<()>::get_or_init(&get_config().appdb.postgres).await?;
        let pool = repository.get_pool().clone();
        let archive = match &config.log_archive {
            Some(config) => Some(Arc::new(LogArchiveFactory::create(config, pool.clone()).await?)),
            None => None,
        };
        let (capture, route) = if let Some(archive) = &archive {
            let (layer, capture) = ArchiveCapture::start(
                archive.clone(),
                LogStreamIdentity {
                    scope: config.identity.scope.clone(),
                    workflow_id: config.identity.workflow_id,
                    node_id: config.identity.node_id,
                    run_id: config.identity.run_id.clone(),
                },
                LogCaptureOptions::default(),
            )?;
            let targets = Targets::new()
                .with_target("sigbot_", LevelFilter::INFO)
                .with_target("common_telemetry", LevelFilter::INFO);
            (Some(capture), Some(LogRoutes::install(Box::new(layer), targets)?))
        } else {
            (None, None)
        };
        let node = Arc::new(StrategyNode::with_log_archive(
            pool.clone(),
            config,
            Arc::new(|parent| Ok(MessageTrace::child("strategy.publish", parent).traceparent())),
            archive.clone(),
        )?);
        let producer = node.producer();
        let observed_messager = capture
            .as_ref()
            .map(|capture| capture.wrap_messager(messager.clone()))
            .unwrap_or_else(|| messager.clone());
        node.start(observed_messager).await?;
        let relay = OutboxRelay::start(PostgresMessageStore::new(pool), producer, messager.clone());
        let archive_task = archive.map(|archive| {
            let readiness = common_telemetry::readiness::RuntimeReadiness::task("strategy.archive");
            tokio::spawn(async move {
                readiness.set(true);
                loop {
                    if let Err(error) = archive.archive_pending(64).await {
                        common_telemetry::warn!(target:"sigbot_archive_internal", "strategy log archive retry: {}", error);
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            })
        });
        tracing::info!("Strategy runtime subscriptions and archive are ready");
        if let Some(capture) = &capture {
            capture.flush_durable().await?;
        }
        common_telemetry::readiness::RuntimeReadiness::business("strategy", true);
        Ok(Arc::new(Self {
            relay,
            archive_task,
            capture,
            route: Mutex::new(route),
            messager,
        }))
    }

    pub async fn close(&self) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("strategy", false);
        self.relay.close();
        self.messager.close().await; // Stop callbacks before the final durable barrier.
        let result = if let Some(capture) = &self.capture {
            capture.shutdown().await
        } else {
            Ok(())
        };
        self.route.lock().unwrap_or_else(|error| error.into_inner()).take();
        if let Some(task) = &self.archive_task {
            task.abort();
        }
        result
    }
}
