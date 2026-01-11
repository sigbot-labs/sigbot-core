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

use crate::executor::strategy_factory::SigbotStrategyExecutorFactory;
use common_telemetry::{debug, info, warn};
use sigbot_core::{
    config::config,
    context::state::SigbotState,
    modules::workflow::{
        WorkflowCallHandlers, WorkflowJobStatus, WorkflowManager, WorkflowStartHandler, WorkflowStopHandler,
    },
};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use sigbot_types::modules::workflow::workflow::WorkflowInfo;
use std::sync::Arc;

pub struct SigbotStrategyRunner {}

impl SigbotStrategyRunner {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        // Parse SigbotStrategyArgument from command line arguments first
        debug!("Parsing Strategy Executor configuration.");
        let configuration = matches
            .try_get_one::<String>("STRATEGY_RUNNER_CONFIGURATION")
            .map(|s| s.map(|s| s.to_owned()).unwrap_or_default())
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            sigbot_types::modules::decode_arg_configuration(&configuration)
                .and_then(|json| {
                    sigbot_types::modules::strategy::SigbotStrategyArgument::from_json(&json)
                        .map_err(|e| anyhow::Error::msg(format!("Failed to parse the configuration: {}", e)))
                })
                .expect("Failed to decode and parse the configuration."),
        );

        debug!("Initializing Messager Client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.to_owned().messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager Client.");
        info!("Initialized Messager Client. {}", messager.provider().as_str());

        // Initialize Workflow Manager
        debug!("Initializing Workflow Manager.");
        let state = Arc::new(SigbotState::new(&config::get_config()).await);

        // Create start handler callback
        // This handler receives WorkflowManager and should spawn async task to start the strategy executor
        // The async operation and status updates are handled here, not in WorkflowManager.process
        let messager_for_start = messager.to_owned();
        let argument_for_start = argument.to_owned();
        let call_start_handler: WorkflowStartHandler =
            Arc::new(move |workflow: Arc<WorkflowInfo>, manager: Arc<WorkflowManager>| {
                let workflow_id = workflow
                    .base
                    .id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                // Validate workflow has required fields
                if workflow.provider.is_none() {
                    return Err(anyhow::Error::msg(format!(
                        "Workflow {} has no provider specified",
                        workflow_id
                    )));
                }

                // Get the job to update status
                let manager_clone = manager.clone();
                let workflow_id_clone = workflow_id.clone();
                let workflow_clone = workflow.clone();
                let messager_clone = messager_for_start.clone();
                let argument_clone = argument_for_start.clone();

                // Spawn async task to start strategy executor
                tokio::spawn(async move {
                    info!("Starting strategy executor for workflow: {}", workflow_id_clone);

                    // Extract provider from workflow
                    let provider_str = workflow_clone.provider.as_ref().map(|p| p.as_str()).unwrap_or("PYCODE");

                    // Map WorkflowProvider to StrategyProvider
                    use sigbot_types::modules::strategy::strategy::StrategyProvider;
                    let strategy_provider = match provider_str {
                        "PYCODE" => StrategyProvider::PYCODE,
                        "LLM" => StrategyProvider::LLM,
                        _ => {
                            warn!("Unknown workflow provider: {}, defaulting to PYCODE", provider_str);
                            StrategyProvider::PYCODE
                        }
                    };

                    // Extract node_id from flow_info
                    let node_id = if let Some(ref flow_info) = workflow_clone.flow_info {
                        flow_info
                            .nodes
                            .iter()
                            .find_map(|node| {
                                if node.r#type == "AI_EVALUATOR" || node.r#type == "PY_EVALUATOR" {
                                    Some(node.id.clone())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_else(|| "default".to_string())
                    } else {
                        "default".to_string()
                    };

                    let provider0 = strategy_provider.as_str();

                    // Initialize and start strategy executor for this workflow node
                    match SigbotStrategyExecutorFactory::init(
                        workflow_id_clone.clone(),
                        node_id.clone(),
                        strategy_provider,
                        argument_clone,
                    )
                    .await
                    {
                        Ok(executor) => {
                            // Start the executor with messager
                            executor.startup(messager_clone).await;
                            info!(
                                "Started strategy executor {} for workflow {} node {}",
                                provider0, workflow_id_clone, node_id
                            );

                            // Update job status to RUNNING on success
                            if let Some(job) = manager_clone.get_node_job(&workflow_id_clone).await {
                                job.set_status(WorkflowJobStatus::RUNNING).await;
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to initialize strategy executor {} for workflow {} node {}: {}",
                                provider0, workflow_id_clone, node_id, e
                            );

                            // Update job status to STOPPED on failure and unregister
                            if let Some(failed_job) = manager_clone.unregister_job(&workflow_id_clone).await {
                                failed_job.set_status(WorkflowJobStatus::STOPPED).await;
                            }
                        }
                    }
                });

                // Return Ok immediately since actual work is done in spawned task
                Ok(())
            });

        // Create stop handler callback
        // This handler receives WorkflowManager and should spawn async task to stop the strategy executor
        // The async operation and status updates are handled here, not in WorkflowManager.process
        let call_stop_handler: WorkflowStopHandler =
            Arc::new(move |workflow_id: String, manager: Arc<WorkflowManager>| {
                if workflow_id.is_empty() {
                    return Err(anyhow::Error::msg("Workflow ID is empty"));
                }

                let manager_clone = manager.clone();
                let workflow_id_clone = workflow_id.clone();

                // Spawn async task to stop strategy executor
                tokio::spawn(async move {
                    info!("Stopping strategy executor for workflow: {}", workflow_id_clone);

                    // Get the workflow job to determine which executor to stop
                    if let Some(job) = manager_clone.get_node_job(&workflow_id_clone).await {
                        // Extract node_id from workflow info
                        let node_id = if let Some(ref flow_info) = job.workflow_info().flow_info {
                            flow_info
                                .nodes
                                .iter()
                                .find_map(|node| {
                                    if node.r#type == "AI_EVALUATOR" || node.r#type == "PY_EVALUATOR" {
                                        Some(node.id.clone())
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or_else(|| "default".to_string())
                        } else {
                            "default".to_string()
                        };

                        // Build executor_id: "{workflow_id}:{node_id}"
                        let executor_id = format!("{}:{}", workflow_id_clone, node_id);

                        // Unregister and shutdown strategy executor
                        match SigbotStrategyExecutorFactory::unregister(executor_id.to_owned()).await {
                            Ok(_) => {
                                info!(
                                    "Stopped strategy executor for workflow {} node {}",
                                    workflow_id_clone, node_id
                                );
                            }
                            Err(e) => {
                                warn!("Failed to unregister strategy executor {}: {}", executor_id, e);
                            }
                        }

                        // Unregister job and update status to STOPPED
                        if let Some(unregistered_job) = manager_clone.unregister_job(&workflow_id_clone).await {
                            unregistered_job.set_status(WorkflowJobStatus::STOPPED).await;
                        }
                    } else {
                        debug!("Workflow {} is not running", workflow_id_clone);
                    }
                });

                // Return Ok immediately since actual work is done in spawned task
                Ok(())
            });

        // Create handlers tuple (must be provided together)
        let handlers: WorkflowCallHandlers = (call_start_handler, call_stop_handler);

        // Create, initialize and start WorkflowManager using the global singleton
        let workflow_manager = WorkflowManager::new(
            state, handlers, None, // Use default cron expression
            None, // Use default channel size
        );

        workflow_manager
            .startup(None, None)
            .await
            .expect("Failed to start Workflow Manager");

        info!("Started Workflow Manager.");
    }

    pub async fn shutdown() {
        info!("Shutting down Strategy Executor.");
        SigbotStrategyExecutorFactory::close().await;
        info!("Shutdown Strategy Executor.");

        info!("Shutting down Messager Client.");
        SigbotMessagerClientFactory::close().await;
        info!("Shutdown Messager Client.");

        // Shutdown WorkflowManager (uses global singleton)
        WorkflowManager::shutdown_global().await;
    }
}

#[cfg(test)]
mod tests {}
