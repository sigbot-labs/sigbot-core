// SPDX-License-Identifier: GPL-3.0-only

use crate::history::virtual_clock::PostgresReplayBarrier;
use crate::history::virtual_clock::ReplayBarrierConfig;
use crate::history::virtual_clock::ReplayCursor;
use crate::history::virtual_clock::ReplayDecision;
use crate::history::virtual_clock::ReplayEngine;
use crate::history::virtual_clock::ReplayProgress;
use crate::history::virtual_clock::ReplayRecord;
use crate::history::virtual_clock::ReplaySource;
use crate::trades::trades::BacktestExchange;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use sigbot_core::config::config::get_config;
use sigbot_core::modules::backtest::handler::backtest::{
    BacktestReplayHandler, HistoricalRecord, HistoryCursor, ReplayCheckpoint, ReplayHistory, ReplayIdentity,
};
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::OutboxRelay;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::Uuid;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayNodeIdentity {
    pub scope: MessageScope,
    pub workflow_id: Uuid,
    pub node_id: i64,
    pub run_id: Uuid,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayRuntimeConfig {
    pub identity: ReplayNodeIdentity,
    pub history: ReplayHistoryConfig,
    pub data_node_id: i64,
    pub evaluation_node_id: i64,
    pub barrier: ReplayBarrierConfig,
    pub page_size: u32,
    pub max_steps: u64,
    #[serde(default)]
    pub warmup_from: Option<DateTime<Utc>>,
    #[serde(default = "ReplayRuntimeConfig::default_warmup_limit")]
    pub warmup_limit: u32,
}

impl ReplayRuntimeConfig {
    fn default_warmup_limit() -> u32 {
        10000
    }
}

pub struct HistoryBacktest {
    job: tokio::task::JoinHandle<()>,
    relays: Vec<OutboxRelay>,
}

struct HistoryCoordinator {
    handler: Arc<BacktestReplayHandler>,
    config: ReplayRuntimeConfig,
    barrier: PostgresReplayBarrier,
    matching: BacktestExchange,
}

impl HistoryBacktest {
    pub async fn start(properties: &HashMap<String, String>, messager: Arc<dyn IMessager>) -> Result<Arc<Self>> {
        common_telemetry::readiness::RuntimeReadiness::business("backtest.replay", false);
        let config: ReplayRuntimeConfig = serde_json::from_str(
            properties
                .get("replay_config")
                .context("typed replay_config required")?,
        )?;
        let identity = &config.identity;
        identity.scope.validate()?;
        let workflow_id = identity.workflow_id;
        let run_id = identity.run_id;
        ensure!(
            workflow_id.get_version_num() == 7 && run_id.get_version_num() == 7,
            "replay identity must use UUIDv7"
        );
        let matching: crate::trades::trades::MatchingConfig = serde_json::from_str(
            properties
                .get("matching_config")
                .context("replay requires explicit backtest matching_config")?,
        )?;
        ensure!(
            matching.virtual_clock && matching.scope == identity.scope && matching.node_id == identity.node_id,
            "replay matching must use the same isolated scope, owner node and persisted clock"
        );
        ensure!(run_id != config.history.run_id, "replay requires a separate run");
        ensure!(
            config.data_node_id > 0
                && config.evaluation_node_id > 0
                && config.data_node_id != config.evaluation_node_id
                && config.barrier.scope == identity.scope
                && config.barrier.run_id == run_id,
            "replay participants differ from run isolation"
        );
        let database_url = get_config().storage.postgres.connection_url()?;
        let handler = Arc::new(BacktestReplayHandler::connect(&database_url).await?);
        handler
            .acquire_owner(
                ReplayIdentity {
                    scope: &identity.scope,
                    workflow_id,
                    run_id: &run_id,
                },
                &config.history.run_id,
            )
            .await?;
        let source = Arc::new(PostgresReplaySource::new(handler.clone(), config.history.clone())?);
        let barrier = PostgresReplayBarrier::connect(&database_url, config.barrier.clone()).await?;
        let host = Arc::new(HistoryCoordinator {
            matching: BacktestExchange::connect(&database_url, matching).await?,
            handler: handler.clone(),
            config,
            barrier,
        });
        let progress = host.restore().await?;
        host.warmup().await?;
        let engine = ReplayEngine::new(
            source,
            host.clone(),
            host.config.history.until,
            host.config.page_size,
            host.config.max_steps,
        )?;
        let relays = [host.config.data_node_id, host.config.evaluation_node_id]
            .into_iter()
            .map(|node| OutboxRelay::start(handler.messager(), host.producer(node), messager.clone()))
            .collect();
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("backtest.replay.owner");
        let job = tokio::spawn(async move {
            readiness.set(true);
            let result = engine.run(progress).await;
            let completed = result.is_ok();
            if let Err(error) = result {
                tracing::error!(%error,run_id=%host.config.identity.run_id,"historical replay stopped without advancing past an unsettled step");
            }
            host.handler.release_owner(&host.config.identity.run_id).await;
            if completed {
                readiness.complete();
            }
        });
        common_telemetry::readiness::RuntimeReadiness::business("backtest.replay", true);
        Ok(Arc::new(Self { job, relays }))
    }
    pub fn close(&self) {
        common_telemetry::readiness::RuntimeReadiness::business("backtest.replay", false);
        self.job.abort();
        for relay in &self.relays {
            relay.close();
        }
    }
}

impl Drop for HistoryBacktest {
    fn drop(&mut self) {
        self.close();
    }
}

impl HistoryCoordinator {
    fn identity(&self) -> ReplayIdentity<'_> {
        ReplayIdentity {
            scope: &self.config.identity.scope,
            workflow_id: self.config.identity.workflow_id,
            run_id: &self.config.identity.run_id,
        }
    }

    fn history(&self) -> ReplayHistory<'_> {
        ReplayHistory {
            scope: &self.config.history.scope,
            run_id: &self.config.history.run_id,
            workflow_id: self.config.history.workflow_id,
            sources: &self.config.history.sources,
            from: self.config.history.from,
            until: self.config.history.until,
            frozen_at: self.config.history.frozen_at,
        }
    }

    fn producer(&self, node: i64) -> String {
        let scope = &self.config.identity.scope;
        format!("backtest.feed:{}:{}:{node}", scope.env, scope.tenant_id)
    }

    async fn warmup(&self) -> Result<()> {
        let Some(from) = self.config.warmup_from else {
            return Ok(());
        };
        ensure!(
            from < self.config.history.from && (1..=1_000_000).contains(&self.config.warmup_limit),
            "invalid historical warmup bound"
        );
        let mut configuration = self.config.history.clone();
        configuration.until = configuration.from;
        configuration.from = from;
        let source = PostgresReplaySource::new(self.handler.clone(), configuration.clone())?;
        let mut cursor = None;
        let mut count = 0_u32;
        loop {
            let page = source.storage_page(cursor.as_ref(), self.config.page_size).await?;
            if page.is_empty() {
                break;
            }
            count += page.len() as u32;
            ensure!(
                count <= self.config.warmup_limit,
                "historical warmup exceeded record bound"
            );
            self.handler
                .import_records(
                    self.identity(),
                    ReplayHistory {
                        scope: &configuration.scope,
                        run_id: &configuration.run_id,
                        workflow_id: configuration.workflow_id,
                        sources: &configuration.sources,
                        from: configuration.from,
                        until: configuration.until,
                        frozen_at: configuration.frozen_at,
                    },
                    &page,
                    self.config.history.from,
                )
                .await?;
            cursor = page.last().map(|record| Self::from_storage_cursor(&record.cursor));
        }
        Ok(())
    }
    async fn restore(&self) -> Result<ReplayProgress> {
        let configuration = serde_json::to_value(&self.config)?;
        let checkpoint = self
            .handler
            .restore(
                self.identity(),
                &configuration,
                self.config.history.frozen_at,
                self.config.history.from,
            )
            .await?;
        Ok(ReplayProgress {
            cursor: checkpoint.cursor.as_ref().map(Self::from_storage_cursor),
            virtual_time: checkpoint.virtual_time,
            steps: checkpoint.steps,
            timer_firings: checkpoint.timer_firings,
        })
    }

    fn to_storage_cursor(cursor: &ReplayCursor) -> HistoryCursor {
        HistoryCursor {
            available_at: cursor.available_at,
            priority: cursor.priority,
            source: cursor.source.clone(),
            source_event_id: cursor.source_event_id.clone(),
            revision: cursor.revision,
        }
    }

    fn from_storage_cursor(cursor: &HistoryCursor) -> ReplayCursor {
        ReplayCursor {
            available_at: cursor.available_at,
            priority: cursor.priority,
            source: cursor.source.clone(),
            source_event_id: cursor.source_event_id.clone(),
            revision: cursor.revision,
        }
    }
}

#[async_trait]
impl ReplayDecision for HistoryCoordinator {
    async fn deliver(&self, record: &ReplayRecord, _sequence: u64, clock: DateTime<Utc>) -> Result<()> {
        let node = if record.topic == WorkflowTopic::EvaluationSnapshot {
            self.config.evaluation_node_id
        } else {
            self.config.data_node_id
        };
        let stored = HistoricalRecord {
            cursor: Self::to_storage_cursor(&record.cursor),
            event_time: record.event_time,
            topic: record.topic,
            payload: record.payload.clone(),
        };
        let mut delivery = self
            .handler
            .begin_delivery(
                self.identity(),
                self.history(),
                &stored,
                node,
                &self.producer(node),
                clock,
            )
            .await?;
        if record.topic == WorkflowTopic::FeedMarket {
            let tick = serde_json::from_value(serde_json::to_value(&delivery.message)?)?;
            self.matching
                .match_tick_in(&mut delivery.transaction, &tick, clock)
                .await?;
        }
        delivery.transaction.commit().await
    }

    async fn next_timer(&self) -> Result<Option<DateTime<Utc>>> {
        self.handler.next_timer(self.identity()).await
    }

    async fn fire_timers(&self, clock: DateTime<Utc>, limit: u32) -> Result<u32> {
        self.handler.fire_timers(self.identity(), clock, limit).await
    }

    async fn settle(&self, _: DateTime<Utc>) -> Result<()> {
        self.handler.assert_owner().await?;
        self.barrier.wait().await
    }
    async fn checkpoint(&self, progress: &ReplayProgress, complete: bool) -> Result<()> {
        self.handler
            .checkpoint(
                self.identity(),
                ReplayCheckpoint {
                    cursor: progress.cursor.as_ref().map(Self::to_storage_cursor),
                    virtual_time: progress.virtual_time,
                    steps: progress.steps,
                    timer_firings: progress.timer_firings,
                },
                complete,
            )
            .await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayHistoryConfig {
    pub scope: MessageScope,
    pub run_id: Uuid,
    pub workflow_id: Uuid,
    pub sources: Vec<String>,
    pub from: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub frozen_at: DateTime<Utc>,
}

pub struct PostgresReplaySource {
    handler: Arc<BacktestReplayHandler>,
    config: ReplayHistoryConfig,
}

impl PostgresReplaySource {
    pub fn new(handler: Arc<BacktestReplayHandler>, config: ReplayHistoryConfig) -> Result<Self> {
        config.scope.validate()?;
        ensure!(
            config.run_id.get_version_num() == 7
                && config.workflow_id.get_version_num() == 7
                && !config.sources.is_empty()
                && config.sources.len() <= 32,
            "historical run, workflow and bounded sources are required"
        );
        ensure!(
            config.from < config.until && config.until <= config.frozen_at && config.frozen_at <= Utc::now(),
            "invalid frozen historical interval"
        );
        Ok(Self { handler, config })
    }

    fn history(&self) -> ReplayHistory<'_> {
        ReplayHistory {
            scope: &self.config.scope,
            run_id: &self.config.run_id,
            workflow_id: self.config.workflow_id,
            sources: &self.config.sources,
            from: self.config.from,
            until: self.config.until,
            frozen_at: self.config.frozen_at,
        }
    }

    async fn storage_page(&self, after: Option<&ReplayCursor>, limit: u32) -> Result<Vec<HistoricalRecord>> {
        let cursor = after.map(HistoryCoordinator::to_storage_cursor);
        self.handler.page(self.history(), cursor.as_ref(), limit).await
    }
}

#[async_trait]
impl ReplaySource for PostgresReplaySource {
    async fn page(&self, after: Option<&ReplayCursor>, limit: u32) -> Result<Vec<ReplayRecord>> {
        Ok(self
            .storage_page(after, limit)
            .await?
            .into_iter()
            .map(|record| ReplayRecord {
                cursor: HistoryCoordinator::from_storage_cursor(&record.cursor),
                event_time: record.event_time,
                topic: record.topic,
                payload: record.payload,
            })
            .collect())
    }
}
