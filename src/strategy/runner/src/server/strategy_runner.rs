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
    config::config_tenant::{get_tenant_config, init_tenant_config},
    context::state::SigbotState,
    modules::workflow::{
        SigbotWorkflowHandlerWrapper, SigbotWorkflowManager, SigbotWorkflowStartHandler, SigbotWorkflowStopHandler,
    },
};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use sigbot_types::modules::{
    decode_arg_config,
    strategy::SigbotStrategyArgument,
    workflow::workflow::{WorkflowStageType, WorkflowStageWrapper},
};
use std::sync::Arc;

pub struct SigbotStrategyRunner {}

impl SigbotStrategyRunner {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        debug!("Parsing Strategy Executor configuration.");

        let configuration = matches
            .try_get_one::<String>("STRATEGY_RUNNER_CONFIGURATION")
            .map(|s| s.map(|s| s.to_owned()).unwrap_or_default())
            .expect("Failed to parse the configuration from the command line arguments.");
        init_tenant_config(&configuration);

        let argument = Arc::new(
            SigbotStrategyArgument::from_json(
                &decode_arg_config(&configuration).expect("Failed to decode tenant configuration."),
            )
            .map_err(|e| anyhow::Error::msg(format!("Failed to parse the configuration: {}", e)))
            .expect("Failed to parse the configuration."),
        );

        debug!("Initializing Messager Client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.to_owned().messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager Client.");
        info!("Initialized Messager Client. {}", messager.provider().as_str());

        // Initialize Workflow Manager with tenant-specific configuration
        debug!("Initializing Workflow Manager.");
        let state = Arc::new(SigbotState::new(&get_tenant_config()).await);

        // Create start handler callback
        // This handler receives NodeJob, and should start the strategy executor asynchronously
        // The async operation and status updates are handled here
        let messager0 = messager.to_owned();
        let argument0 = argument.to_owned();
        let call_start_handler: SigbotWorkflowStartHandler = Arc::new(move |node_job| {
            let workflow = node_job.workflow_info();
            let workflow_id = workflow
                .base
                .id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            let workflow_id0 = workflow_id.clone();
            let node_info = node_job.node_info().clone();
            let node_id = node_job.node_id();
            let messager1 = messager0.clone();
            let argument1 = argument0.clone();

            Box::pin(async move {
                info!(
                    "Starting strategy executor for workflow: {} node: {}",
                    workflow_id0, node_id
                );
                // Extract StrategyProvider from node stage
                // Strategy runner only handles EVALUATION stage with Strategy provider
                let strategy_provider = match &node_info.stage {
                    WorkflowStageType::EVALUATION(providers) => match providers.as_ref().and_then(|v| v.first()) {
                        Some(WorkflowStageWrapper::Strategy(provider)) => provider.clone(),
                        _ => {
                            warn!(
                                    "Workflow {} node {} is not a Strategy provider node (stage: EVALUATION but provider is not Strategy), skipping start",
                                    workflow_id0, node_id
                                );
                            return Err(anyhow::Error::msg(format!(
                                "Node {} is not a Strategy provider node, cannot start strategy executor",
                                node_id
                            )));
                        }
                    },
                    _ => {
                        warn!(
                            "Workflow {} node {} is not an EVALUATION stage node (stage: {:?}), skipping start as it's not the responsibility of strategy runner",
                            workflow_id0, node_id, node_info.stage
                        );
                        return Err(anyhow::Error::msg(format!(
                            "Node {} is not an EVALUATION stage node, cannot start strategy executor",
                            node_id
                        )));
                    }
                };
                let provider_str = strategy_provider.as_str();

                // Initialize and start strategy executor for this workflow node
                match SigbotStrategyExecutorFactory::init(
                    workflow_id0.clone(),
                    node_id.to_string(),
                    strategy_provider,
                    argument1,
                )
                .await
                {
                    Ok(executor) => {
                        // Start the executor with messager
                        executor.startup(messager1).await;
                        info!(
                            "Started strategy executor {} for workflow {} node {}",
                            provider_str, workflow_id0, node_id
                        );
                        Ok(())
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize strategy executor {} for workflow {} node {}: {}",
                            provider_str, workflow_id0, node_id, e
                        );
                        Err(e)
                    }
                }
            })
        });

        // Create stop handler callback
        // This handler receives NodeJob, and should stop the strategy executor asynchronously
        // The async operation and status updates are handled here
        let call_stop_handler: SigbotWorkflowStopHandler = Arc::new(move |node_job| {
            let workflow_id = node_job.workflow_id();
            let node_id = node_job.node_id();

            Box::pin(async move {
                info!(
                    "Stopping strategy executor for workflow: {} node: {}",
                    workflow_id, node_id
                );
                // Build executor_id: "{workflow_id}:{node_id}"
                let executor_id = format!("{}:{}", workflow_id, node_id);

                // Unregister and shutdown strategy executor
                match SigbotStrategyExecutorFactory::close(executor_id.to_owned()).await {
                    Ok(_) => {
                        info!(
                            "Stopped strategy executor for workflow {} node {}",
                            workflow_id, node_id
                        );
                    }
                    Err(e) => {
                        warn!("Failed to unregister strategy executor {}: {}", executor_id, e);
                        return Err(e);
                    }
                }
                Ok(())
            })
        });

        // Create handlers tuple (must be provided together)
        let handlers: SigbotWorkflowHandlerWrapper = (call_start_handler, call_stop_handler);

        // Create, initialize and start WorkflowManager using the global singleton
        // Strategy runner supports EVALUATION stage type
        let workflow_manager =
            SigbotWorkflowManager::new(state, handlers, None, None, WorkflowStageType::EVALUATION(None));

        workflow_manager
            .startup(None, None)
            .await
            .expect("Failed to start Workflow Manager");

        info!("Started Workflow Manager.");
    }

    pub async fn shutdown() {
        info!("Shutting down Strategy Executor.");
        SigbotStrategyExecutorFactory::shutdown().await;
        info!("Shutdown Strategy Executor.");

        info!("Shutting down Messager Client.");
        SigbotMessagerClientFactory::shutdown().await;
        info!("Shutdown Messager Client.");

        // Shutdown WorkflowManager (uses global singleton)
        SigbotWorkflowManager::shutdown().await;
    }
}

#[cfg(test)]
mod tests {}
