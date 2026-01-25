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

use crate::executor::adk::executor_mas::SigbotMasEvaluationExecutor;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use sigbot_core::config::config_tenant::get_tenant_config;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::evaluator::{evaluator::EvaluatorProvider, SigbotEvaluatorRunnerArgument};
use sigbot_utils::dash_maps::ConcurrentMap;
use std::sync::Arc;

/// Core trait for evaluation executors
/// Includes both core functionality and lifecycle management
#[async_trait]
pub trait ISigbotEvaluationExecutor: Send + Sync {
    fn provider(&self) -> EvaluatorProvider;

    /// This will be called by WorkflowManager when the workflow node starts
    async fn startup(
        &self,
        argument: Arc<SigbotEvaluatorRunnerArgument>,
        messager: Arc<dyn ISigbotMessagerClient + Send + Sync>,
    );

    /// This will be called by WorkflowManager when the workflow node stops
    async fn shutdown(&self);
}

/// SigbotEvaluationExecutorFactory manages evaluation executor lifecycle
///
/// Responsibilities:
/// - Create and register evaluation executors for individual LLM workflow nodes
/// - Support multiple executor implementations through ISigbotEvaluationExecutor trait
/// - Track running executors in registry
///
pub struct SigbotEvaluationExecutorFactory {}

impl SigbotEvaluationExecutorFactory {
    /// executor_id => ISigbotEvaluationExecutor, e.g. executor_id="{workflow_id}:{node_id}"
    fn get_registry() -> &'static Arc<ConcurrentMap<String, Arc<dyn ISigbotEvaluationExecutor>>> {
        use lazy_static::lazy_static;
        lazy_static! {
            static ref REGISTRY: Arc<ConcurrentMap<String, Arc<dyn ISigbotEvaluationExecutor>>> =
                Arc::new(ConcurrentMap::new());
        }
        &REGISTRY
    }

    /// Initialize and register evaluation executor based on provider
    pub async fn init(
        workflow_id: String,
        node_id: String,
        provider: EvaluatorProvider,
        argument: Arc<SigbotEvaluatorRunnerArgument>,
    ) -> Result<Arc<dyn ISigbotEvaluationExecutor>, Error> {
        let executor_id = format!("{}:{}", workflow_id, node_id);

        // Check if already registered
        if Self::get_registry().contains_key(&executor_id) {
            return Err(Error::msg(format!(
                "Evaluation executor {} already registered",
                executor_id
            )));
        }

        debug!(
            "Initializing executor with provider={:?}, argument properties: {:?}",
            provider, argument.properties
        );

        // Create executor based on provider
        let executor: Arc<dyn ISigbotEvaluationExecutor> = match provider {
            EvaluatorProvider::MAS => {
                let evaluator_config = get_tenant_config().services.evaluator.inner.to_owned();
                SigbotMasEvaluationExecutor::new(
                    executor_id.clone(),
                    workflow_id,
                    node_id,
                    Some(evaluator_config.cron.to_owned()),
                    Some(evaluator_config.channel_size.to_owned()),
                )
            } // When new providers are added, add new match arms here
        };

        // Register executor
        Self::get_registry().insert(executor_id.clone(), executor.clone());
        info!(
            "Registered evaluation executor: {} with provider: {:?}",
            executor_id, provider
        );

        Ok(executor)
    }

    /// Get a registered executor by ID
    pub fn get(executor_id: &str) -> Option<Arc<dyn ISigbotEvaluationExecutor>> {
        Self::get_registry().get(executor_id).map(|e| e.clone())
    }

    /// Unregister and shutdown evaluation executor
    pub async fn close(executor_id: String) -> Result<(), Error> {
        if let Some(executor) = Self::get_registry().remove(&executor_id) {
            executor.shutdown().await;
            info!("Unregistered evaluation executor: {}", executor_id);
            Ok(())
        } else {
            Err(Error::msg(format!("Evaluation executor {} not found", executor_id)))
        }
    }

    /// Shutdown all executors
    pub async fn shutdown() {
        info!("Shutting down all evaluation executors...");
        for executor in Self::get_registry().iter() {
            executor.shutdown().await;
        }
        Self::get_registry().clear();
        info!("Shutdown all evaluation executors.");
    }
}

#[cfg(test)]
mod tests {}
