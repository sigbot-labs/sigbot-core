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

use crate::executor::{
    adk::{
        agents::{
            alpha_agent::SigbotAlphaAgent, auditor_agent::SigbotAuditorAgent, boot_agent::SigbotBootAgent,
            loader_agent::SigbotLoaderAgent,
        },
        core::{
            agent_base::{ISigbotAgent, SigbotAgentContext},
            agent_loop::SigbotLoopAgent,
            orchestrator::SigbotMultiAgentOrchestrator,
        },
    },
    evaluator_factory::ISigbotEvaluationExecutor,
};
use async_trait::async_trait;
use common_telemetry::{debug, error, info, warn};
use sigbot_core::config::config_tenant::get_tenant_config;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::{
    evaluator::{evaluator::EvaluatorProvider, events::SigbotHyperparameterUpdateEvent, SigbotEvaluatorRunnerArgument},
    messager::TOPIC_WF_HYPERPARAMETER_UPDATE,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

/// Multi-Agent System (MAS) based evaluation executor
///
/// Dual Trigger Mechanism:
/// 1. Event-Driven: Subscribe to datafeed market data from messager (e.g., Twitter/TruthSocial news)
/// 2. Time-Driven: Cron-based periodic scan job to fetch latest data from TimescaleDB
///
/// Similar to SigbotStrategyExecutor in strategy runner
pub struct SigbotMasEvaluationExecutor {
    executor_id: String,
    workflow_id: String,
    node_id: String,
    orchestrator: Arc<SigbotMultiAgentOrchestrator>,
    job_cron: Option<String>,          // default: "0/30 * * * * *"
    scheduler_channels: Option<usize>, // default: 5
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
}

impl SigbotMasEvaluationExecutor {
    pub fn new(
        executor_id: String,
        workflow_id: String,
        node_id: String,
        scan_cron: Option<String>,
        scheduler_channels: Option<usize>,
    ) -> Arc<Self> {
        // Create datafeed agents (parallel execution)
        let datafeed_agents: Vec<Arc<dyn ISigbotAgent>> =
            vec![Arc::new(SigbotBootAgent::new()), Arc::new(SigbotLoaderAgent::new())];

        // Create strategy agents (sequential execution)
        let alpha_agent = Arc::new(SigbotAlphaAgent::new());
        let auditor_agent = Arc::new(SigbotAuditorAgent::new());

        // Create LoopAgent for AlphaAgent + AuditorAgent retry loop
        // Configure agent rules:
        // - AlphaAgent (index 0): Default success/failure conditions
        // - AuditorAgent (index 1): On failure, goto AlphaAgent (index 0) to retry
        let alpha_auditor_loop = Arc::new(
            SigbotLoopAgent::new(vec![alpha_agent, auditor_agent])
                .with_max_retries(3)
                .with_retry_delay(std::time::Duration::from_millis(500))
                .with_feedback(true)
                // Configure AuditorAgent (index 1) rule: on failure, goto AlphaAgent (index 0)
                .configure_agent(1, |rule| {
                    rule.with_goto_on_failure(Some(0)) // On failure, goto AlphaAgent to retry
                        // AuditorAgent success condition: result.success == true
                        .with_success_condition(|result| result.success)
                        // AuditorAgent failure condition: result.success == false
                        .with_failure_condition(|result| !result.success)
                }),
        );

        let strategy_agents: Vec<Arc<dyn ISigbotAgent>> = vec![alpha_auditor_loop];

        // Create and register default toolset
        let toolset = Arc::new(crate::executor::adk::tools::register_default_tools());

        // Create orchestrator with datafeed and strategy agents
        let orchestrator = Arc::new(SigbotMultiAgentOrchestrator::new(
            datafeed_agents,
            strategy_agents,
            toolset,
            workflow_id.clone(),
        ));

        Arc::new(Self {
            executor_id,
            workflow_id,
            node_id,
            orchestrator,
            job_cron: scan_cron,
            scheduler_channels,
            scheduler: Arc::new(Mutex::new(None)),
        })
    }

    /// Event-Driven: Subscribe to datafeed market data events from messager
    /// This allows the executor to react to incoming market data (e.g., Twitter/TruthSocial news)
    /// When accumulated data reaches a threshold, trigger multi-agent orchestrator
    ///
    /// Also subscribes to dynamic assistant prompt updates (similar to strategy runner's Python code updates)
    async fn subscribe_from_input_stage(&self, _messager: Arc<dyn ISigbotMessagerClient + Send + Sync>) {
        info!(
            "Subscribing to datafeed events for executor_id={}, workflow_id={}, node_id={}",
            self.executor_id, self.workflow_id, self.node_id
        );

        // TODO: Implement event-driven subscription for datafeed market data
        // Example topics:
        // - datafeed/twitter/{workflow_id}
        // - datafeed/truthsocial/{workflow_id}
        // - datafeed/kline/{workflow_id}
        //
        // When data accumulates to threshold, trigger orchestrator execution

        // TODO: Implement dynamic assistant prompt update subscription
        // Similar to strategy runner's Python code dynamic updates via messager (e.g., EMQX)
        // This allows updating LLM node assistant prompts without restarting K8s pod
        //
        // Example topic:
        // - workflow/llm/prompt_update/{tenant_id}/{workflow_id}/{node_id}
        //
        // Message format:
        // {
        //   "tenant_id": "default",
        //   "workflow_id": "wf_123",
        //   "node_id": "node_456",
        //   "prompts": {
        //     "boot_agent": "Updated guidance prompt...",
        //     "loader_agent": "Updated skills prompt...",
        //     "alpha_agent": "Updated analysis prompt...",
        //     "auditor_agent": "Updated risk prompt..."
        //   },
        //   "timestamp": 1706198400000
        // }
        //
        // On receiving update:
        // 1. Validate message structure and permissions
        // 2. Update agent prompts in orchestrator
        // 3. Log the update for audit trail
        // 4. Optionally trigger a test evaluation to validate new prompts

        info!("Subscribed to datafeed events for executor_id={}", self.executor_id);
    }

    /// Time-Driven: Start cron-based periodic scan job
    /// Uses tokio_cron_scheduler for standard cron-based scheduling (default: every 30s)
    /// Periodically scan TimescaleDB for latest market data (e.g., last 4h)
    /// Fetch latest data and trigger multi-agent orchestrator
    async fn start_job_from_input_stage(&self, messager: Arc<dyn ISigbotMessagerClient + Send + Sync>) {
        info!(
            "Starting cron-based scan job for executor_id={}, workflow_id={}, node_id={}",
            self.executor_id, self.workflow_id, self.node_id
        );

        // Clone for async task
        let executor_id = self.executor_id.clone();
        let workflow_id = self.workflow_id.clone();
        let node_id = self.node_id.clone();
        let orchestrator = self.orchestrator.clone();

        // Get cron expression and channel size from config or use defaults
        let tenant_config = get_tenant_config();
        let evaluator_config = &tenant_config.services.evaluator;

        let cron_expression = self.job_cron.as_deref().unwrap_or(&evaluator_config.inner.cron);
        let channel_size = self.scheduler_channels.unwrap_or(evaluator_config.inner.channel_size);

        // Validate the cron expression
        let cron = match Job::new_async(cron_expression, |_uuid, _lock| Box::pin(async {})) {
            Ok(_) => cron_expression,
            Err(e) => {
                warn!(
                    "Invalid cron expression '{}': {}. Using default '{}'",
                    cron_expression, e, evaluator_config.inner.cron
                );
                &evaluator_config.inner.cron
            }
        };

        debug!("Creating cron job for executor_id={} with cron '{}'", executor_id, cron);

        // Create cron job
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let executor_id = executor_id.clone();
            let workflow_id = workflow_id.clone();
            let node_id = node_id.clone();
            let orchestrator = orchestrator.clone();
            let messager = messager.clone();

            Box::pin(async move {
                debug!("Executing evaluation scan for executor_id={}", executor_id);

                // Create agent context.
                let tenant_id = "default".to_string(); // TODO: Extract from workflow configuration
                let ctx = SigbotAgentContext::new(tenant_id.clone(), Some(workflow_id.clone()));

                match orchestrator.execute(ctx).await {
                    Ok(result) => {
                        if result.success {
                            info!(
                                "Evaluation completed successfully for executor_id={}, workflow_id={}, node_id={}",
                                executor_id, workflow_id, node_id
                            );

                            // Publish hyperparameter update
                            let strategy_id = "default_strategy".to_string(); // TODO: Extract from workflow
                            let hp_event = SigbotHyperparameterUpdateEvent::new(
                                tenant_id,
                                workflow_id.clone(),
                                strategy_id,
                                result.data,
                                Uuid::new_v4().to_string(),
                            );

                            let topic = TOPIC_WF_HYPERPARAMETER_UPDATE
                                .replace("{TENANT_ID}", &hp_event.tenant_id)
                                .replace("{WORKFLOW_ID}", &hp_event.workflow_id);

                            if let Ok(message) = serde_json::to_string(&hp_event) {
                                if let Err(e) = messager.publish(&topic, &message).await {
                                    error!("Failed to publish hyperparameter update: {}", e);
                                } else {
                                    info!("Published hyperparameter update to topic: {}", topic);
                                }
                            }
                        } else {
                            error!(
                                "Evaluation failed for executor_id={}, workflow_id={}, node_id={}: {:?}",
                                executor_id, workflow_id, node_id, result.error
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Evaluation error for executor_id={}, workflow_id={}, node_id={}: {}",
                            executor_id, workflow_id, node_id, e
                        );
                    }
                }

                debug!("Executed evaluation scan for executor_id={}", executor_id);
            })
        })
        .expect("Failed to create evaluation scan job");

        // Create and start scheduler
        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create scheduler");
        scheduler.add(job).await.expect("Failed to add evaluation scan job");
        scheduler
            .start()
            .await
            .expect("Failed to start evaluation scan scheduler");

        // Store scheduler
        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started cron-based scan job for executor_id={} with cron '{}', channels '{}'",
            self.executor_id, cron, channel_size
        );
    }
}

#[async_trait]
impl ISigbotEvaluationExecutor for SigbotMasEvaluationExecutor {
    fn provider(&self) -> EvaluatorProvider {
        EvaluatorProvider::MAS
    }

    async fn startup(
        &self,
        _argument: Arc<SigbotEvaluatorRunnerArgument>,
        messager: Arc<dyn ISigbotMessagerClient + Send + Sync>,
    ) {
        info!(
            "Starting MAS evaluation executor: executor_id={}, workflow_id={}, node_id={}",
            self.executor_id, self.workflow_id, self.node_id
        );

        // 1. Event-Driven: Subscribe to datafeed market data events
        self.subscribe_from_input_stage(messager.clone()).await;

        // 2. Time-Driven: Start periodic scan job
        self.start_job_from_input_stage(messager).await;

        info!(
            "Started MAS evaluation executor: executor_id={}, workflow_id={}, node_id={}",
            self.executor_id, self.workflow_id, self.node_id
        );
    }

    async fn shutdown(&self) {
        info!(
            "Stopping MAS evaluation executor: executor_id={}, workflow_id={}, node_id={}",
            self.executor_id, self.workflow_id, self.node_id
        );

        // Stop the scheduler first
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown evaluation scan scheduler");
            info!("Stopped cron scheduler for executor_id={}", self.executor_id);
        }

        // Then stop the orchestrator
        self.orchestrator.stop();

        info!(
            "Stopped MAS evaluation executor: executor_id={}, workflow_id={}, node_id={}",
            self.executor_id, self.workflow_id, self.node_id
        );
    }
}

#[cfg(test)]
mod tests {}
