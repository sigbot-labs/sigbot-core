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

use crate::executor::adk::core::agent_base::{ISigbotAgent, SigbotAgentContext, SigbotAgentResult};
use anyhow::Error;
use common_telemetry::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

// Import toolset support
use adk_tool::Toolset;

/// SigbotMultiAgentOrchestrator is a generic multi-agent orchestrator that can be reused across projects
///
/// Design Philosophy: Keep code simple, directly orchestrate agents with parallel and sequential execution
/// We don't build complex custom orchestration logic - we use simple, proven patterns
///
/// Architecture (workflow-based execution):
///
/// ```text
/// RootOrchestrator
/// ├── ParallelAgents - Runs datafeed agents in parallel
/// │   └── All datafeed agents (e.g., BootAgent, LoaderAgent)
/// └── SequentialAgents - Runs strategy agents sequentially
///     └── All strategy agents (e.g., AlphaAgent, AuditorAgent with LoopAgent)
/// ```
///
/// Key Features:
/// - Supports both parallel and sequential agent execution
/// - All agents use unified execute() interface
/// - LoopAgent handles retry logic internally for iterative refinement
/// - Generic design allows easy adaptation for other multi-agent projects
///
/// Usage Example:
///
/// ```rust,ignore
/// // 1. Create your datafeed agents (will run in parallel)
/// let datafeed_agents = vec![
///     Arc::new(BootAgent::new()) as Arc<dyn ISigbotAgent>,
///     Arc::new(LoaderAgent::new()) as Arc<dyn ISigbotAgent>,
/// ];
///
/// // 2. Create your strategy agents (will run sequentially)
/// let alpha_agent = Arc::new(AlphaAgent::new());
/// let auditor_agent = Arc::new(AuditorAgent::new());
/// let alpha_auditor_loop = Arc::new(
///     LoopAgent::new(vec![alpha_agent, auditor_agent])
///         .configure_agent(1, |rule| rule.with_goto_on_failure(Some(0)))
/// );
/// let strategy_agents = vec![alpha_auditor_loop as Arc<dyn ISigbotAgent>];
///
/// // 3. Create the orchestrator
/// let toolset = Arc::new(register_default_tools());
/// let orchestrator = GenericMultiAgentOrchestrator::new(
///     datafeed_agents,  // Parallel execution
///     strategy_agents,  // Sequential execution
///     toolset,
///     "workflow_123".to_string(),
/// );
///
/// // 4. Start execution
/// let ctx = SigbotAgentContext::new("tenant_1".to_string(), Some("workflow_123".to_string()));
/// let result = orchestrator.execute(ctx).await?;
/// ```
///
/// How to Adapt for Other Projects:
/// 1. Copy the orchestrator module to your new project
/// 2. Create your own agent implementations (all must implement ISigbotAgent):
///    - Create datafeed/collection agents (e.g., data_collector, api_fetcher, etc.)
///    - Create strategy/processing agents (e.g., analyzer, processor, validator, etc.)
/// 3. Use LoopAgent to wrap agents that need retry/refinement logic
/// 4. Pass your agents to GenericMultiAgentOrchestrator::new() and call execute()
///
/// TransferToAgent/Retry Usage:
/// - Use LoopAgent to wrap agents that need iterative refinement
/// - Example: If auditor finds issues, it can loop back to alpha_generator for refinement
/// - This enables flexible agent coordination and iterative refinement
///
pub struct SigbotMultiAgentOrchestrator {
    /// Parallel datafeed agents (execute concurrently)
    datafeed_agents: Vec<Arc<dyn ISigbotAgent>>,
    /// Sequential strategy agents (execute one after another)
    strategy_agents: Vec<Arc<dyn ISigbotAgent>>,
    /// Shared toolset for all agents
    pub(crate) toolset: Option<Arc<dyn Toolset>>,
    /// Workflow/case identifier
    pub(crate) workflow_id: String,
    /// Maximum retry attempts for the entire orchestrator
    pub(crate) max_retry_attempts: u32,
    /// Retry timeout for the entire orchestrator
    pub(crate) retry_timeout: Duration,
}

impl SigbotMultiAgentOrchestrator {
    /// Create a new generic multi-agent orchestrator with pre-created agents
    ///
    /// Parameters:
    /// - datafeed_agents: Parallel agents that collect data from different sources
    ///   These agents will execute concurrently
    /// - strategy_agents: Sequential agents that process data
    ///   These agents will execute sequentially and can use LoopAgent for retry logic
    /// - toolset: Shared toolset for all agents
    /// - workflow_id: Unique identifier for this orchestration workflow
    ///
    /// This is a generic orchestrator that can be reused for other multi-agent projects.
    /// Simply change the agent implementations to adapt it for different use cases.
    pub fn new(
        datafeed_agents: Vec<Arc<dyn ISigbotAgent>>,
        strategy_agents: Vec<Arc<dyn ISigbotAgent>>,
        toolset: Arc<dyn Toolset>,
        workflow_id: String,
    ) -> Self {
        Self {
            datafeed_agents,
            strategy_agents,
            toolset: Some(toolset),
            workflow_id,
            max_retry_attempts: 3,
            retry_timeout: Duration::from_secs(30),
        }
    }

    /// Backward compatibility: Create orchestrator with sequential agents only
    pub fn new_sequential(agents: Vec<Arc<dyn ISigbotAgent>>, toolset: Arc<dyn Toolset>) -> Self {
        Self {
            datafeed_agents: Vec::new(),
            strategy_agents: agents,
            toolset: Some(toolset),
            workflow_id: "default".to_string(),
            max_retry_attempts: 3,
            retry_timeout: Duration::from_secs(30),
        }
    }

    /// Execute the workflow with parallel datafeed agents followed by sequential strategy agents
    ///
    /// Execution Flow:
    /// 1. Execute all datafeed agents in parallel (concurrent data collection)
    /// 2. Execute all strategy agents sequentially (ordered processing pipeline)
    /// 3. LoopAgent handles retry logic internally for iterative refinement
    ///
    /// All agents use unified execute() interface
    pub async fn execute(&self, ctx: SigbotAgentContext) -> Result<SigbotAgentResult, Error> {
        let start_time = Instant::now();
        info!(
            "GenericOrchestrator: Starting workflow for tenant_id={}, workflow_id={:?}, datafeed_agents={}, strategy_agents={}",
            ctx.tenant_id, ctx.workflow_id, self.datafeed_agents.len(), self.strategy_agents.len()
        );

        let mut current_ctx = ctx;

        // Add toolset to context if available
        if let Some(toolset) = &self.toolset {
            current_ctx = current_ctx.with_toolset(toolset.clone());
        }

        // Step 1: Execute datafeed agents in parallel
        if !self.datafeed_agents.is_empty() {
            info!(
                "GenericOrchestrator: Executing {} datafeed agents in parallel",
                self.datafeed_agents.len()
            );

            let mut handles = Vec::new();
            for (idx, agent) in self.datafeed_agents.iter().enumerate() {
                let agent1 = agent.clone();
                let ctx = current_ctx.clone();
                let handle = tokio::spawn(async move {
                    info!("Parallel datafeed agent {}: {} starting", idx + 1, agent1.name());
                    let result = agent1.execute(&ctx).await;
                    info!("Parallel datafeed agent {}: {} completed", idx + 1, agent1.name());
                    result
                });
                handles.push((agent.name().to_string(), handle));
            }

            // Wait for all parallel agents to complete
            let mut parallel_results = Vec::new();
            for (agent_name, handle) in handles {
                match handle.await {
                    Ok(Ok(result)) => {
                        if !result.success {
                            error!("Parallel datafeed agent {} failed: {:?}", agent_name, result.error);
                            return Err(Error::msg(format!(
                                "Parallel datafeed agent {} failed: {}",
                                agent_name,
                                result.error.as_deref().unwrap_or("Unknown error")
                            )));
                        }
                        parallel_results.push(result);
                        info!("Parallel datafeed agent {} completed successfully", agent_name);
                    }
                    Ok(Err(e)) => {
                        error!("Parallel datafeed agent {} execution error: {}", agent_name, e);
                        return Err(e);
                    }
                    Err(e) => {
                        error!("Parallel datafeed agent {} join error: {}", agent_name, e);
                        return Err(Error::msg(format!("Agent join error: {}", e)));
                    }
                }
            }

            // Merge all parallel results into context
            for result in parallel_results {
                current_ctx = current_ctx.with_data(result.data);
            }

            info!("GenericOrchestrator: All parallel datafeed agents completed successfully");
        }

        // Step 2: Execute strategy agents sequentially
        let mut last_result: Option<SigbotAgentResult> = None;

        for (idx, agent) in self.strategy_agents.iter().enumerate() {
            info!(
                "GenericOrchestrator: Executing sequential strategy agent {}/{}: {}",
                idx + 1,
                self.strategy_agents.len(),
                agent.name()
            );

            // Unified agent execution - LoopAgent handles its own retry logic internally
            match agent.execute(&current_ctx).await {
                Ok(result) => {
                    if !result.success {
                        error!("Sequential strategy agent {} failed: {:?}", agent.name(), result.error);
                        return Err(Error::msg(format!(
                            "Sequential strategy agent {} failed: {}",
                            agent.name(),
                            result.error.as_deref().unwrap_or("Unknown error")
                        )));
                    }
                    last_result = Some(result.clone());
                    current_ctx = current_ctx.with_data(result.data);
                    info!("Sequential strategy agent {} completed successfully", agent.name());
                }
                Err(e) => {
                    error!("Sequential strategy agent {} execution error: {}", agent.name(), e);
                    return Err(e);
                }
            }
        }

        if let Some(result) = last_result {
            info!(
                "GenericOrchestrator: Workflow completed successfully in {:?}",
                start_time.elapsed()
            );
            Ok(result)
        } else {
            // If no strategy agents, return success with merged datafeed data
            info!(
                "GenericOrchestrator: Workflow completed (datafeed only) in {:?}",
                start_time.elapsed()
            );
            Ok(SigbotAgentResult {
                success: true,
                data: current_ctx.data,
                error: None,
            })
        }
    }

    /// Stop the orchestrator (for future use with cancellation support)
    pub fn stop(&self) {
        info!("GenericOrchestrator: Stopping workflow: {}", self.workflow_id);
        // TODO: Implement cancellation logic if needed
    }

    /// Get the current status of the orchestrator
    pub fn get_status(&self) -> String {
        "running".to_string()
        // TODO: Implement proper status tracking if needed
    }
}
