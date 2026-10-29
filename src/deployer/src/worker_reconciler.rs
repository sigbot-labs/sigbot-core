//! One scoped controller owns worker deployment and run lifecycle. PostgreSQL row
//! locks prevent duplicate controllers; service recovery never allocates a new run.
use crate::worker_plan::WorkerPlan;
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use sigbot_core::{
    config::{authguard::AuthguardProperties, config::WorkerDeployProperties},
    context::state::SigbotState,
};
use sigbot_types::modules::messager::topic::MessageScope;
use sqlx::Row;
use std::{sync::Arc, time::Duration};

#[derive(Default)]
pub struct WorkerHealth {
    pub ready: bool,
    pub restarted: bool,
}

#[async_trait]
pub trait WorkerProvider: Send + Sync {
    async fn apply(&self, plan: &WorkerPlan) -> Result<WorkerHealth>;
    async fn remove(&self, plan: &WorkerPlan) -> Result<bool>;
}

pub struct WorkerReconciler {
    state: Arc<SigbotState>,
    provider: Arc<dyn WorkerProvider>,
    scope: MessageScope,
    timeout: Duration,
}
impl WorkerReconciler {
    pub fn new(state: Arc<SigbotState>, provider: Arc<dyn WorkerProvider>) -> Result<Self> {
        let auth = &state.config.authguard;
        let scope = Self::scope(auth)?;
        let settings = &state.config.services.deployer.workers;
        Self::validate_settings(settings)?;
        let timeout = Duration::from_secs(settings.reconciliation_timeout_seconds);
        Ok(Self {
            state,
            provider,
            scope,
            timeout,
        })
    }
    pub fn scope(auth: &AuthguardProperties) -> Result<MessageScope> {
        ensure!(
            auth.service_principal_id.as_ref().is_some_and(|id| !id.is_empty()),
            "explicit controller principal required"
        );
        let scope = MessageScope {
            env: auth.service_env.clone().context("controller environment required")?,
            tenant_id: auth.service_tenant_id.context("controller tenant required")?,
            space_id: auth.service_space_id.context("controller space required")?,
        };
        scope.validate()?;
        Ok(scope)
    }
    fn validate_settings(settings: &WorkerDeployProperties) -> Result<()> {
        ensure!(
            (100..=16000).contains(&settings.cpu_millis) && (64..=32768).contains(&settings.memory_mb),
            "invalid worker resource limits"
        );
        ensure!(
            (8..=1024).contains(&settings.pids_limit) && (5..=120).contains(&settings.reconciliation_timeout_seconds),
            "invalid worker process/timeout limits"
        );
        Ok(())
    }
    pub async fn reconcile(&self) -> Result<()> {
        let pool = self.state.postgres_pool().await?;
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM s_workflow WHERE tenant_id=$1 AND env=$2 AND space_id=$3 AND del_flag=0 AND status IN ('PENDING','STARTING','RUNNING','STOPPING','CANCELLING') ORDER BY id LIMIT 128")
            .bind(self.scope.tenant_id).bind(&self.scope.env).bind(self.scope.space_id).fetch_all(&pool).await?;
        for id in ids {
            match tokio::time::timeout(self.timeout, self.workflow(&pool, id)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(workflow_id=id,error=%error,"worker reconciliation pending"),
                Err(_) => tracing::warn!(
                    workflow_id = id,
                    "worker reconciliation timed out; durable state retained"
                ),
            }
        }
        Ok(())
    }
    async fn workflow(&self, pool: &sqlx::PgPool, id: i64) -> Result<()> {
        let mut tx = pool.begin().await?;
        let Some(row)=sqlx::query("SELECT status FROM s_workflow WHERE id=$1 AND tenant_id=$2 AND env=$3 AND space_id=$4 AND del_flag=0 FOR UPDATE SKIP LOCKED")
            .bind(id).bind(self.scope.tenant_id).bind(&self.scope.env).bind(self.scope.space_id).fetch_optional(&mut *tx).await? else { return Ok(()) };
        let status: String = row.get("status");
        if !matches!(
            status.as_str(),
            "PENDING" | "STARTING" | "RUNNING" | "STOPPING" | "CANCELLING"
        ) {
            return Ok(());
        }
        let run=sqlx::query("SELECT run_id,params FROM workflow_run WHERE workflow_id=$1 AND tenant_id=$2 AND env=$3 AND space_id=$4 AND state IN ('queued','running') ORDER BY started_at DESC LIMIT 1 FOR UPDATE")
            .bind(id).bind(self.scope.tenant_id).bind(&self.scope.env).bind(self.scope.space_id).fetch_optional(&mut *tx).await?.context("workflow has no active run")?;
        let run_id: String = run.get("run_id");
        let plans = WorkerPlan::from_snapshot(&self.scope, id, &run_id, &run.get("params"))?;
        let stopping = matches!(status.as_str(), "STOPPING" | "CANCELLING");
        let mut ready = true;
        let mut restarted = false;
        for plan in &plans {
            if stopping {
                ready &= self.provider.remove(plan).await?;
            } else {
                let health = self.provider.apply(plan).await?;
                restarted |= health.restarted;
                ready &= health.ready;
                if !ready {
                    break;
                } // Subscribers are ready before producers start.
            }
        }
        if restarted {
            sqlx::query("UPDATE workflow_run SET attempt_no=attempt_no+1 WHERE run_id=$1")
                .bind(&run_id)
                .execute(&mut *tx)
                .await?;
        }
        if ready {
            sqlx::query(
                "UPDATE workflow_run SET state=$2,ended_at=CASE WHEN $3 THEN now() ELSE NULL END WHERE run_id=$1",
            )
            .bind(&run_id)
            .bind(if stopping { "stopped" } else { "running" })
            .bind(stopping)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE s_workflow SET status=$2,updated_at=now() WHERE id=$1")
                .bind(id)
                .bind(if stopping { "STOPPED" } else { "RUNNING" })
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

/// Scheduler owns lifecycle only; worker operations and retry semantics stay in the controller.
#[derive(Clone)]
pub struct WorkerScheduler {
    cron: String,
    channels: usize,
    running: Arc<tokio::sync::Mutex<Option<tokio_cron_scheduler::JobScheduler>>>,
}
impl WorkerScheduler {
    pub fn new(cron: Option<String>, channels: Option<usize>) -> Self {
        Self {
            cron: cron.unwrap_or_else(|| "0/10 * * * * *".into()),
            channels: channels.unwrap_or(2),
            running: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
    pub async fn start(&self, reconciler: Arc<WorkerReconciler>) -> Result<()> {
        let mut running = self.running.lock().await;
        ensure!(running.is_none(), "controller scheduler already started");
        let job = tokio_cron_scheduler::Job::new_async(&self.cron, move |_, _| {
            let reconciler = reconciler.clone();
            Box::pin(async move {
                if let Err(error) = reconciler.reconcile().await {
                    tracing::warn!(%error,"worker scan failed");
                }
            })
        })?; // Invalid cron fails startup, never falls back to a different schedule.
        let scheduler = tokio_cron_scheduler::JobScheduler::new_with_channel_size(self.channels).await?;
        scheduler.add(job).await?;
        scheduler.start().await?;
        *running = Some(scheduler);
        Ok(())
    }
    pub async fn stop(&self) {
        if let Some(mut scheduler) = self.running.lock().await.take() {
            if let Err(error) = scheduler.shutdown().await {
                tracing::warn!(%error,"controller scheduler shutdown failed");
            }
        }
    }
}
