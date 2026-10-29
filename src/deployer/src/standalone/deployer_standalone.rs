//! Compatibility name for the former no-op standalone deployer. Runtime hosts now
//! use Docker isolation; local service CLI commands remain available for development.
use crate::{deployer_factory::ISigbotDeployer, docker::deployer_docker::SigbotDockerDeployer};
use async_trait::async_trait;
use sigbot_core::context::state::SigbotState;
use std::sync::Arc;

pub struct SigbotStandaloneDeployer {
    docker: Arc<SigbotDockerDeployer>,
}
impl SigbotStandaloneDeployer {
    pub const NAME: &'static str = "STANDALONE";
    pub async fn new(cron: Option<String>, channels: Option<usize>, state: Option<Arc<SigbotState>>) -> Arc<Self> {
        Arc::new(Self {
            docker: SigbotDockerDeployer::new(cron, channels, state).await,
        })
    }
}
#[async_trait]
impl ISigbotDeployer for SigbotStandaloneDeployer {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    async fn startup(&self) {
        tracing::warn!("standalone deployer is a compatibility alias for isolated Docker workers");
        self.docker.startup().await;
    }
    async fn shutdown(&self) {
        self.docker.shutdown().await;
    }
}
