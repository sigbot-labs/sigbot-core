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

use crate::agents::{
    alpha_agent::SigbotAlphaAgent, auditor_agent::SigbotAuditorAgent, boot_agent::SigbotBootAgent,
    loader_agent::SigbotLoaderAgent,
};
use crate::core::agent_base::SigbotAgentContext;
use crate::core::agent_loop::{AgentRule, SigbotLoopAgent};
use crate::core::orchestrator::SigbotOrchestrator;
use anyhow::Error;
use common_telemetry::{debug, error, info};
use sigbot_messager::client::messager_factory::{ISigbotMessagerClient, SigbotMessagerClientFactory};
use sigbot_types::modules::{
    evaluator::events::{SigbotEvaluatorTriggerEvent, SigbotHyperparameterUpdateEvent},
    evaluator::SigbotEvaluatorManagerArgument,
    messager::{TOPIC_EVALUATOR_TRIGGER, TOPIC_WF_HYPERPARAMETER_UPDATE},
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

pub struct SigbotEvaluatorRunner {
    orchestrator: Arc<SigbotOrchestrator>,
    messager: Arc<Mutex<Option<Arc<dyn ISigbotMessagerClient + Send + Sync>>>>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
}

impl SigbotEvaluatorRunner {
    pub fn new() -> Self {
        // Create individual agents
        let boot_agent = Arc::new(SigbotBootAgent::new());
        let loader_agent = Arc::new(SigbotLoaderAgent::new());
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

        // Create orchestrator with agents (LoopAgent handles its own retry logic)
        let orchestrator = Arc::new(SigbotOrchestrator::new(vec![
            boot_agent,
            loader_agent,
            alpha_auditor_loop,
        ]));

        Self {
            orchestrator,
            messager: Arc::new(Mutex::new(None)),
            scheduler: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        debug!("Initializing Evaluator runner.");

        // Initialize messager
        let argument = Self::parse_config(matches).expect("Failed to parse configuration");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {:?}", messager.provider());

        let runner = Arc::new(Self::new());
        *runner.messager.lock().await = Some(messager.clone());

        // Start cron job scheduler
        let cron_expression = "0 */6 * * *"; // Every 6 hours by default
        runner.start_cron_job(cron_expression).await;

        // Subscribe to market data trigger events
        runner.subscribe_events(messager.clone()).await;

        info!("Evaluator runner started successfully.");
    }

    fn parse_config(matches: &clap::ArgMatches) -> Result<Arc<SigbotEvaluatorManagerArgument>, Error> {
        use sigbot_types::modules::decode_arg_config;

        let configuration = matches
            .try_get_one::<String>("EVALUATOR_MANAGER_CONFIGURATION")
            .map(|s| s.map(|s| s.to_owned()).unwrap_or_else(|| "{}".to_string()))
            .expect("Failed to parse the configuration from the command line arguments.");

        Ok(Arc::new(SigbotEvaluatorManagerArgument::from_json(
            &decode_arg_config(&configuration)?,
        )?))
    }

    async fn start_cron_job(&self, cron_expression: &str) {
        info!("Starting cron job with expression: {}", cron_expression);

        let orchestrator = self.orchestrator.to_owned();
        let messager0 = self.messager.to_owned();
        let job = Job::new_async(cron_expression, move |_uuid, _lock| {
            let orchestrator = orchestrator.to_owned();
            let messager1 = messager0.to_owned();
            Box::pin(async move {
                info!("Cron job triggered. Starting evaluation...");
                // TODO: Get tenant_id and workflow_id from configuration or database
                let tenant_id = "default".to_string();
                let ctx = SigbotAgentContext::new(tenant_id.to_owned(), None);

                match orchestrator.execute(ctx).await {
                    Ok(result) => {
                        if result.success {
                            // Publish hyperparameters
                            if let Some(messager_guard) = messager1.lock().await.as_ref() {
                                let hyperparameter_event = SigbotHyperparameterUpdateEvent::new(
                                    tenant_id.to_owned(),
                                    "default".to_string(),          // TODO: Get from configuration
                                    "default_strategy".to_string(), // TODO: Get from configuration
                                    result.data,
                                    Uuid::new_v4().to_string(),
                                );
                                let topic = TOPIC_WF_HYPERPARAMETER_UPDATE
                                    .replace("{TENANT_ID}", &hyperparameter_event.tenant_id)
                                    .replace("{WORKFLOW_ID}", &hyperparameter_event.workflow_id);
                                if let Ok(message) = serde_json::to_string(&hyperparameter_event) {
                                    if let Err(e) = messager_guard.publish(&topic, &message).await {
                                        error!("Failed to publish hyperparameter update: {}", e);
                                    } else {
                                        info!("Published hyperparameter update from cron job to topic: {}", topic);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Cron job evaluation failed for tenant_id={}: {}", tenant_id, e);
                    }
                }
            })
        })
        .expect("Failed to create evaluator cron job");

        let scheduler = JobScheduler::new().await.expect("Failed to create scheduler");
        scheduler.add(job).await.expect("Failed to add evaluator cron job");
        scheduler.start().await.expect("Failed to start evaluator scheduler");

        *self.scheduler.lock().await = Some(scheduler);
        info!("Started cron job scheduler.");
    }

    async fn subscribe_events(&self, messager: Arc<dyn ISigbotMessagerClient + Send + Sync>) {
        let orchestrator = self.orchestrator.clone();
        let messager_clone = messager.clone();

        let handler: Arc<
            dyn Fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, Error>> + Send>>
                + Send
                + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let orchestrator = orchestrator.clone();
            let messager = messager_clone.clone();
            Box::pin(async move {
                let event: SigbotEvaluatorTriggerEvent = match serde_json::from_slice(&data) {
                    Ok(e) => e,
                    Err(e) => {
                        error!("Failed to deserialize SigbotEvaluatorTriggerEvent: {}", e);
                        return Err(Error::msg(format!(
                            "Failed to deserialize SigbotEvaluatorTriggerEvent: {}",
                            e
                        )));
                    }
                };

                info!(
                    "Received SigbotEvaluatorTriggerEvent: tenant_id={}, workflow_id={:?}, trigger_type={:?}",
                    event.tenant_id, event.workflow_id, event.trigger_type
                );

                tokio::spawn(async move {
                    if let Err(e) = Self::handle_event(orchestrator, messager, event).await {
                        error!("Failed to process trigger event: {}", e);
                    }
                });

                Ok(String::from_utf8_lossy(&data).to_string())
            })
        });

        let topic = TOPIC_EVALUATOR_TRIGGER.replace("{tenant_id}", "+"); // MQTT single level wildcard
        messager
            .subscribe(&topic, handler)
            .await
            .expect("Failed to subscribe to evaluator trigger topic");

        info!("Subscribed to Evaluator trigger topic.");
    }

    async fn handle_event(
        orchestrator: Arc<SigbotOrchestrator>,
        messager: Arc<dyn ISigbotMessagerClient + Send + Sync>,
        event: SigbotEvaluatorTriggerEvent,
    ) -> Result<(), Error> {
        info!(
            "Handling trigger event: tenant_id={}, workflow_id={:?}",
            event.tenant_id, event.workflow_id
        );

        // Execute evaluation
        let ctx = SigbotAgentContext::new(event.tenant_id.clone(), event.workflow_id.clone());
        let result = orchestrator.execute(ctx).await?;

        if !result.success {
            return Err(Error::msg(format!(
                "Evaluation failed: {}",
                result.error.as_deref().unwrap_or("Unknown error")
            )));
        }

        // Publish hyperparameter update
        let hyperparameter_event = SigbotHyperparameterUpdateEvent::new(
            event.tenant_id.clone(),
            event.workflow_id.clone().unwrap_or_default(),
            "default_strategy".to_string(), // TODO: Get from configuration
            result.data,
            Uuid::new_v4().to_string(),
        );

        let topic = TOPIC_WF_HYPERPARAMETER_UPDATE
            .replace("{TENANT_ID}", &hyperparameter_event.tenant_id)
            .replace("{WORKFLOW_ID}", &hyperparameter_event.workflow_id);
        let message = serde_json::to_string(&hyperparameter_event)?;
        messager.publish(&topic, &message).await?;

        info!(
            "Published hyperparameter update to topic: {}, tenant_id={}, workflow_id={}",
            topic, hyperparameter_event.tenant_id, hyperparameter_event.workflow_id
        );

        Ok(())
    }

    pub async fn shutdown() {
        info!("Shutting down Evaluator runner.");
        SigbotMessagerClientFactory::close().await;
        info!("Shutdown Evaluator runner.");
    }
}

impl Clone for SigbotEvaluatorRunner {
    fn clone(&self) -> Self {
        Self {
            orchestrator: self.orchestrator.clone(),
            messager: self.messager.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

#[cfg(test)]
mod tests {}
