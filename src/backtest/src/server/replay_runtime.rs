// SPDX-License-Identifier: GPL-3.0-only
//! Historical data still crosses the actual broker and the normal decision/order/wallet runtimes.
use crate::manager::{
    replay::{ReplayDecision, ReplayEngine, ReplayProgress, ReplayRecord, ReplaySource},
    replay_barrier::{PostgresReplayBarrier, ReplayBarrierConfig},
    replay_postgres::{PostgresReplaySource, ReplayHistoryConfig},
    trades::simulated_exchange::SimulatedExchange,
};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sigbot_datafeed::server::replay_ingestor::{ReplayImport, ReplayIngestor};
use sigbot_messager::{
    client::{messager_factory::ISigbotMessagerClient, outbox_relay::OutboxRelay},
    storage::postgres::PostgresMessageStore,
};
use sigbot_strategy_sdk::sdk::runtime::StrategyIdentity;
use sigbot_types::modules::messager::{
    envelope::{DurableMessage, MessageKind},
    topic::WorkflowTopic,
};
use sqlx::{PgPool, Row};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayRuntimeConfig {
    pub identity: StrategyIdentity,
    pub history: ReplayHistoryConfig,
    pub data_node_id: i64,
    pub evaluation_node_id: i64,
    pub barrier: ReplayBarrierConfig,
    pub page_size: u32,
    pub max_steps: u64,
    pub database_url_env: String,
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

pub struct ReplayRuntime {
    job: tokio::task::JoinHandle<()>,
    relays: Vec<OutboxRelay>,
}

struct ReplayHost {
    pool: PgPool,
    config: ReplayRuntimeConfig,
    barrier: PostgresReplayBarrier,
    owner: tokio::sync::Mutex<sqlx::PgConnection>,
    matching: SimulatedExchange,
}

impl ReplayRuntime {
    pub async fn start(
        properties: &HashMap<String, String>,
        messager: Arc<dyn ISigbotMessagerClient>,
    ) -> Result<Arc<Self>> {
        common_telemetry::readiness::RuntimeReadiness::business("backtest.replay", false);
        let config: ReplayRuntimeConfig = serde_json::from_str(
            properties
                .get("replay_config")
                .context("typed replay_config required")?,
        )?;
        let identity = &config.identity;
        identity.scope.validate()?;
        let matching: crate::manager::trades::simulated_exchange::MatchingConfig = serde_json::from_str(
            properties
                .get("matching_config")
                .context("replay requires explicit simulated matching_config")?,
        )?;
        ensure!(
            matching.virtual_clock && matching.scope == identity.scope && matching.node_id == identity.node_id,
            "replay matching must use the same isolated scope, owner node and persisted clock"
        );
        ensure!(
            identity.scope != config.history.scope
                && identity.run_id != config.history.run_id
                && (matches!(identity.scope.env.as_str(), "replay" | "backtest")
                    || identity.scope.env.starts_with("replay-")),
            "replay requires a separate simulation scope and run"
        );
        ensure!(
            config.data_node_id > 0
                && config.evaluation_node_id > 0
                && config.data_node_id != config.evaluation_node_id
                && config.barrier.scope == identity.scope
                && config.barrier.run_id == identity.run_id,
            "replay participants differ from run isolation"
        );
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&std::env::var(&config.database_url_env)?)
            .await?;
        // A detached connection closes on task cancellation; returning a session
        // advisory lock to the general pool would leave an owner lock behind.
        let mut owner = pool.acquire().await?.detach();
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1,0))")
            .bind(&identity.run_id)
            .fetch_one(&mut owner)
            .await?;
        ensure!(locked, "another replay owner already holds this run");
        let binding:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_run WHERE run_id=$1 AND workflow_id=$2 AND env=$3 AND tenant_id=$4 AND space_id=$5 AND replay_of=$6)")
            .bind(&identity.run_id).bind(identity.workflow_id).bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id)
            .bind(&config.history.run_id).fetch_one(&pool).await?;
        ensure!(
            binding,
            "replay run must be explicitly bound to its historical source before startup"
        );
        let source = Arc::new(PostgresReplaySource::new(pool.clone(), config.history.clone())?);
        // Keep the original checkpoint configuration immutable. Only the runtime
        // view of legacy transport owner names changes after migration 17.
        let mut barrier_config = config.barrier.clone();
        barrier_config.migrate_legacy_owners(identity.workflow_id)?;
        let barrier = PostgresReplayBarrier::new(pool.clone(), barrier_config)?;
        let host = Arc::new(ReplayHost {
            matching: SimulatedExchange::new(pool.clone(), matching)?,
            pool: pool.clone(),
            config,
            barrier,
            owner: tokio::sync::Mutex::new(owner),
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
            .map(|node| {
                OutboxRelay::start(
                    PostgresMessageStore::new(pool.clone()),
                    host.producer(node),
                    messager.clone(),
                )
            })
            .collect();
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("backtest.replay.owner");
        let job = tokio::spawn(async move {
            readiness.set(true);
            let result = engine.run(progress).await;
            let completed = result.is_ok();
            if let Err(error) = result {
                tracing::error!(%error,run_id=%host.config.identity.run_id,"historical replay stopped without advancing past an unsettled step");
            }
            // Keep the session lock for exactly the owner task's lifetime.
            let _ = sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1,0))")
                .bind(&host.config.identity.run_id)
                .execute(&mut *host.owner.lock().await)
                .await;
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

impl Drop for ReplayRuntime {
    fn drop(&mut self) {
        self.close();
    }
}

impl ReplayHost {
    fn producer(&self, node: i64) -> String {
        let scope = &self.config.identity.scope;
        format!(
            "backtest.feed:{}:{}:{}:{node}",
            scope.env, scope.tenant_id, scope.space_id
        )
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
        let source = PostgresReplaySource::new(self.pool.clone(), configuration)?;
        let mut cursor = None;
        let mut count = 0_u32;
        loop {
            let page = source.page(cursor.as_ref(), self.config.page_size).await?;
            if page.is_empty() {
                break;
            }
            count += page.len() as u32;
            ensure!(
                count <= self.config.warmup_limit,
                "historical warmup exceeded record bound"
            );
            let mut tx = self.pool.begin().await?;
            for record in &page {
                if record.topic == WorkflowTopic::FeedMarket {
                    ReplayIngestor::import(
                        &mut tx,
                        &ReplayImport {
                            source_scope: &self.config.history.scope,
                            source_run: &self.config.history.run_id,
                            target_scope: &self.config.identity.scope,
                            target_run: &self.config.identity.run_id,
                            source: &record.cursor.source,
                            source_event_id: &record.cursor.source_event_id,
                            revision: record.cursor.revision,
                            frozen_at: self.config.history.frozen_at,
                            clock: self.config.history.from,
                        },
                    )
                    .await?;
                }
            }
            tx.commit().await?;
            cursor = page.last().map(|record| record.cursor.clone());
        }
        Ok(())
    }
    async fn restore(&self) -> Result<ReplayProgress> {
        let identity = &self.config.identity;
        let configuration = serde_json::to_value(&self.config)?;
        sqlx::query(
            "INSERT INTO backtest_checkpoint(env,tenant_id,space_id,run_id,configuration,frozen_at,virtual_time)
            VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",
        )
        .bind(&identity.scope.env)
        .bind(identity.scope.tenant_id)
        .bind(identity.scope.space_id)
        .bind(&identity.run_id)
        .bind(&configuration)
        .bind(self.config.history.frozen_at)
        .bind(self.config.history.from)
        .execute(&self.pool)
        .await?;
        let row=sqlx::query("SELECT configuration,cursor,virtual_time,steps,state FROM backtest_checkpoint WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id).fetch_one(&self.pool).await?;
        ensure!(
            row.get::<Value, _>("configuration") == configuration,
            "replay restart changed its frozen configuration"
        );
        let cursor: Option<Value> = row.get("cursor");
        let timers:i64=sqlx::query_scalar("SELECT COUNT(*) FROM workflow_timer WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND state='delivered'")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id).fetch_one(&self.pool).await?;
        Ok(ReplayProgress {
            cursor: cursor.map(serde_json::from_value).transpose()?,
            virtual_time: row.get("virtual_time"),
            steps: u64::try_from(row.get::<i64, _>("steps"))?,
            timer_firings: timers.try_into()?,
        })
    }
}

#[async_trait]
impl ReplayDecision for ReplayHost {
    async fn deliver(&self, record: &ReplayRecord, _sequence: u64, clock: DateTime<Utc>) -> Result<()> {
        let identity = &self.config.identity;
        let mut tx = self.pool.begin().await?;
        let pending:Option<Value>=sqlx::query_scalar("SELECT pending_message FROM backtest_checkpoint WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND state='running' FOR UPDATE")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id).fetch_one(&mut *tx).await?;
        if matches!(record.topic, WorkflowTopic::FeedMarket | WorkflowTopic::FeedNews) {
            ReplayIngestor::import(
                &mut tx,
                &ReplayImport {
                    source_scope: &self.config.history.scope,
                    source_run: &self.config.history.run_id,
                    target_scope: &identity.scope,
                    target_run: &identity.run_id,
                    source: &record.cursor.source,
                    source_event_id: &record.cursor.source_event_id,
                    revision: record.cursor.revision,
                    frozen_at: self.config.history.frozen_at,
                    clock,
                },
            )
            .await?;
        }
        sqlx::query("UPDATE backtest_checkpoint SET virtual_time=$5 WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND virtual_time<=$5 AND state='running'")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id).bind(clock).execute(&mut *tx).await?;
        let node = if record.topic == WorkflowTopic::EvaluationSnapshot {
            self.config.evaluation_node_id
        } else {
            self.config.data_node_id
        };
        let trace = MessageTrace::child("backtest.history", "");
        let message = if let Some(pending) = pending {
            ensure!(
                pending.get("cursor") == Some(&serde_json::to_value(&record.cursor)?),
                "replay admission differs from its pending historical fact"
            );
            serde_json::from_value::<DurableMessage<Value>>(
                pending
                    .get("message")
                    .cloned()
                    .context("pending replay message missing")?,
            )?
        } else {
            DurableMessage {
                version: 2,
                scope: identity.scope.clone(),
                workflow_id: Some(identity.workflow_id),
                account_id: None,
                node_id: node,
                target_node_id: None,
                run_id: identity.run_id.clone(),
                seq: PostgresMessageStore::next_sequence(&mut tx, &identity.scope, node, &identity.run_id).await?,
                attempt_no: 0,
                kind: MessageKind::Event,
                event_time: record.event_time,
                available_time: clock,
                produced_at: clock,
                traceparent: trace.traceparent(),
                payload: record.payload.clone(),
            }
        };
        if record.topic == WorkflowTopic::FeedMarket {
            let tick = serde_json::from_value(serde_json::to_value(&message)?)?;
            self.matching.match_tick_in(&mut tx, &tick, clock).await?;
        }
        sqlx::query("UPDATE backtest_checkpoint SET pending_message=$5 WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id)
            .bind(json!({"cursor":record.cursor,"message":message})).execute(&mut *tx).await?;
        PostgresMessageStore::enqueue(
            &mut tx,
            &self.producer(node),
            &record.topic.topic(&identity.scope, identity.workflow_id)?,
            &message,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn next_timer(&self) -> Result<Option<DateTime<Utc>>> {
        let identity = &self.config.identity;
        Ok(sqlx::query_scalar("SELECT MIN(scheduled_at) FROM workflow_timer WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND state='pending'")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id).fetch_one(&self.pool).await?)
    }

    async fn fire_timers(&self, clock: DateTime<Utc>, limit: u32) -> Result<u32> {
        let identity = &self.config.identity;
        let mut tx = self.pool.begin().await?;
        let timers=sqlx::query("SELECT node_id,seq,scheduled_at,payload,traceparent FROM workflow_timer WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4
            AND state='pending' AND scheduled_at<=$5 ORDER BY scheduled_at,node_id,seq LIMIT $6 FOR UPDATE SKIP LOCKED")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id).bind(clock).bind(i64::from(limit)).fetch_all(&mut *tx).await?;
        sqlx::query("UPDATE backtest_checkpoint SET virtual_time=$5 WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND virtual_time<=$5 AND state='running'")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id).bind(clock).execute(&mut *tx).await?;
        for timer in &timers {
            let node: i64 = timer.get("node_id");
            let seq: i64 = timer.get("seq");
            let at: DateTime<Utc> = timer.get("scheduled_at");
            let trace = MessageTrace::child("backtest.timer", timer.get::<String, _>("traceparent").as_str());
            let message = DurableMessage {
                version: 2,
                scope: identity.scope.clone(),
                workflow_id: Some(identity.workflow_id),
                account_id: None,
                node_id: node,
                target_node_id: Some(node),
                run_id: identity.run_id.clone(),
                seq,
                attempt_no: 0,
                kind: MessageKind::Event,
                event_time: at,
                available_time: at,
                produced_at: clock,
                traceparent: trace.traceparent(),
                payload: json!({"source":"timer","symbol":null,"event_time":at,"available_time":at,
                    "data":{"kind":"timer","value":{"scheduled_at":at,"payload":timer.get::<Value,_>("payload")}}}),
            };
            let producer = format!(
                "strategy:{}:{}:{}:{}:{node}",
                identity.scope.env, identity.scope.tenant_id, identity.scope.space_id, identity.workflow_id
            );
            PostgresMessageStore::enqueue(
                &mut tx,
                &producer,
                &WorkflowTopic::RuntimeEvent.topic(&identity.scope, identity.workflow_id)?,
                &message,
            )
            .await?;
            sqlx::query("UPDATE workflow_timer SET state='delivered' WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND node_id=$4 AND run_id=$5 AND seq=$6")
                .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(node).bind(&identity.run_id).bind(seq).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(timers.len().try_into()?)
    }

    async fn settle(&self, _: DateTime<Utc>) -> Result<()> {
        // A lost owner connection never silently reacquires a different session.
        sqlx::query("SELECT 1").execute(&mut *self.owner.lock().await).await?;
        self.barrier.wait().await
    }
    async fn checkpoint(&self, progress: &ReplayProgress, complete: bool) -> Result<()> {
        let identity = &self.config.identity;
        sqlx::query("UPDATE backtest_checkpoint SET cursor=$5,virtual_time=$6,steps=$7,state=$8,pending_message=NULL WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id)
            .bind(progress.cursor.as_ref().map(serde_json::to_value).transpose()?).bind(progress.virtual_time).bind(i64::try_from(progress.steps)?)
            .bind(if complete {"complete"}else {"running"}).execute(&self.pool).await?;
        Ok(())
    }
}
