// SPDX-License-Identifier: GPL-3.0-only

pub use crate::modules::controller::storage::postgres::ActiveWorkflow;
use crate::modules::controller::storage::postgres::ControllerPostgresRepository;
pub use crate::modules::controller::storage::postgres::WorkerClaim;
use anyhow::Result;
use sigbot_types::messager::topic::MessageScope;
use uuid::Uuid;

pub struct ControllerHandler {
    repository: ControllerPostgresRepository,
}

impl ControllerHandler {
    pub async fn from_state(state: &crate::context::state::SigbotState) -> Result<Self> {
        Ok(Self {
            repository: ControllerPostgresRepository::from_state(state).await?,
        })
    }

    pub async fn active_workflows(&self) -> Result<Vec<ActiveWorkflow>> {
        self.repository.active_workflows().await
    }

    pub async fn claim(&self, scope: &MessageScope, workflow_id: Uuid) -> Result<Option<WorkerClaim<'_>>> {
        self.repository.claim(scope, workflow_id).await
    }
}
