// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::core::agent::{ISigbotAgent, SigbotAgentContext, SigbotAgentResult};
use anyhow::Error;
use common_telemetry::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Workflow orchestrator that coordinates multiple agents
pub struct SigbotOrchestrator {
    agents: Vec<Arc<dyn ISigbotAgent>>,
    max_retry_attempts: u32,
    retry_timeout: Duration,
}

impl SigbotOrchestrator {
    pub fn new(agents: Vec<Arc<dyn ISigbotAgent>>) -> Self {
        Self {
            agents,
            max_retry_attempts: 3,
            retry_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retry_attempts = max_retries;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.retry_timeout = timeout;
        self
    }

    /// Execute the workflow with agent orchestration
    pub async fn execute(&self, ctx: SigbotAgentContext) -> Result<SigbotAgentResult, Error> {
        let start_time = Instant::now();
        info!(
            "Orchestrator: Starting workflow for tenant_id={}, workflow_id={:?}",
            ctx.tenant_id, ctx.workflow_id
        );

        let mut current_ctx = ctx;
        let mut last_result: Option<SigbotAgentResult> = None;

        // Execute agents sequentially - all agents use unified execute() interface
        for (idx, agent) in self.agents.iter().enumerate() {
            info!(
                "Orchestrator: Executing agent {}/{}: {}",
                idx + 1,
                self.agents.len(),
                agent.name()
            );

            // Unified agent execution - LoopAgent handles its own retry logic internally
            match agent.execute(&current_ctx).await {
                Ok(result) => {
                    if !result.success {
                        error!("Agent {} failed: {:?}", agent.name(), result.error);
                        return Err(Error::msg(format!(
                            "Agent {} failed: {}",
                            agent.name(),
                            result.error.as_deref().unwrap_or("Unknown error")
                        )));
                    }
                    last_result = Some(result.clone());
                    current_ctx = current_ctx.with_data(result.data);
                    info!("Agent {} completed successfully", agent.name());
                }
                Err(e) => {
                    error!("Agent {} execution error: {}", agent.name(), e);
                    return Err(e);
                }
            }
        }

        if let Some(result) = last_result {
            info!(
                "Orchestrator: Workflow completed successfully in {:?}",
                start_time.elapsed()
            );
            Ok(result)
        } else {
            Err(Error::msg("Workflow completed but no result was produced"))
        }
    }
}
