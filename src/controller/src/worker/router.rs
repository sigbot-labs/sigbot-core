use crate::docker::docker::DockerController;
use crate::kubernetes::kubernetes::KubernetesController;
use crate::worker::plan::WorkerPlan;
use crate::worker::reconciler::{WorkerHealth, WorkerProvider};
use anyhow::Result;
use async_trait::async_trait;
use sigbot_core::context::state::SigbotState;
use sigbot_types::controller::{WorkerInfo, WorkerProvider as WorkerKind};
use std::sync::Arc;

pub struct WorkerRouter {
    state: Arc<SigbotState>,
}

impl WorkerRouter {
    pub fn new(state: Arc<SigbotState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl WorkerProvider for WorkerRouter {
    async fn prepare(&self, target: &WorkerInfo) -> Result<WorkerHealth> {
        match target.provider {
            WorkerKind::Docker => {
                let provider = DockerController::for_worker(self.state.clone(), target).await?;
                WorkerProvider::prepare(&provider, target).await
            }
            WorkerKind::Kubernetes => {
                let provider = KubernetesController::for_worker(self.state.clone(), target).await?;
                WorkerProvider::prepare(&provider, target).await
            }
        }
    }

    async fn apply(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<WorkerHealth> {
        match target.provider {
            WorkerKind::Docker => {
                let provider = DockerController::for_worker(self.state.clone(), target).await?;
                WorkerProvider::apply(&provider, target, plan).await
            }
            WorkerKind::Kubernetes => {
                let provider = KubernetesController::for_worker(self.state.clone(), target).await?;
                WorkerProvider::apply(&provider, target, plan).await
            }
        }
    }

    async fn remove(&self, target: &WorkerInfo, plan: &WorkerPlan) -> Result<bool> {
        match target.provider {
            WorkerKind::Docker => {
                let provider = DockerController::for_worker(self.state.clone(), target).await?;
                WorkerProvider::remove(&provider, target, plan).await
            }
            WorkerKind::Kubernetes => {
                let provider = KubernetesController::for_worker(self.state.clone(), target).await?;
                WorkerProvider::remove(&provider, target, plan).await
            }
        }
    }
}
