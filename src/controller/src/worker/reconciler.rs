//! One scoped controller owns worker deployment and run lifecycle. PostgreSQL row
//! locks prevent duplicate controllers; service recovery never allocates a new run.
use crate::worker::middleware::MiddlewareTopology;
use crate::worker::plan::WorkerPlan;
use anyhow::ensure;
use anyhow::Result;
use async_trait::async_trait;
use sigbot_core::config::config::WorkerDeployProperties;
use sigbot_core::context::state::SigbotState;
use sigbot_core::modules::controller::handler::controller::ControllerHandler;
use sigbot_types::controller::WorkerInfo;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
pub struct WorkerHealth {
    pub ready: bool,
    pub restarted: bool,
}

#[async_trait]
pub trait WorkerProvider: Send + Sync {
    async fn prepare(&self, target: &WorkerInfo) -> Result<WorkerHealth>;
    async fn apply(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<WorkerHealth>;
    async fn remove(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<bool>;
}

pub struct WorkerReconciler {
    state: Arc<SigbotState>,
    provider: Arc<dyn WorkerProvider>,
    timeout: Duration,
}
impl WorkerReconciler {
    pub fn new(state: Arc<SigbotState>, provider: Arc<dyn WorkerProvider>) -> Result<Self> {
        ensure!(
            state
                .config
                .authguard
                .service_principal_id
                .as_ref()
                .is_some_and(|id| !id.is_empty()),
            "explicit controller service principal required"
        );
        let settings = &state.config.services.controller.workers;
        Self::validate_settings(settings)?;
        let timeout = Duration::from_secs(settings.reconciliation_timeout_seconds);
        Ok(Self {
            state,
            provider,
            timeout,
        })
    }
    fn validate_settings(settings: &WorkerDeployProperties) -> Result<()> {
        ensure!(settings.enabled, "controller worker reconciliation is disabled");
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
        let handler = ControllerHandler::from_state(&self.state).await?;
        let workflows = handler.active_workflows().await?;
        for workflow in workflows {
            match tokio::time::timeout(self.timeout, self.workflow(&handler, &workflow.scope, workflow.id)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(workflow_id=%workflow.id,error=%error,"worker reconciliation pending"),
                Err(_) => tracing::warn!(
                    workflow_id = %workflow.id,
                    "worker reconciliation timed out; durable state retained"
                ),
            }
        }
        Ok(())
    }
    async fn workflow(&self, handler: &ControllerHandler, scope: &MessageScope, id: Uuid) -> Result<()> {
        let Some(claim) = handler.claim(scope, id).await? else {
            return Ok(());
        };
        let mut deployment = WorkerPlan::deployment_from_snapshot(scope, id, &claim.run_id, &claim.parameters)?;
        let mut ready = true;
        let mut restarted = false;
        if !claim.stopping {
            let health = self.provider.prepare(&claim.worker).await?;
            restarted |= health.restarted;
            if !health.ready {
                return claim.complete(restarted, false).await;
            }
            let topology = MiddlewareTopology::new(&claim.worker);
            for plan in &mut deployment.workers {
                topology.bind(plan)?;
            }
        }
        for plan in &deployment.workers {
            if claim.stopping {
                ready &= self.provider.remove(&claim.worker, plan).await?;
            } else {
                let health = self.provider.apply(&claim.worker, plan).await?;
                restarted |= health.restarted;
                ready &= health.ready;
                if !ready {
                    break;
                } // Subscribers are ready before producers start.
            }
        }
        claim.complete(restarted, ready).await
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
