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

use crate::executor::{strategy_llm::SigbotLLMStrategyExecutor, strategy_pycode::SigbotPythonStrategyExecutor};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::{
    decode_arg_configuration,
    strategy::{strategy::StrategyProvider, SigbotStrategyArgument},
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotStrategyExecutor: Send + Sync {
    fn provider(&self) -> StrategyProvider;
    async fn startup(&self, messager: Arc<dyn ISigbotMessagerClient + Send + Sync>);
    async fn shutdown(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotStrategyExecutorFactory> =
        RwLock::new(SigbotStrategyExecutorFactory::new());
}

pub struct SigbotStrategyExecutorFactory {
    /// Map from executor_id (format: "{workflow_id}:{node_id}") to executor instance
    pub implementations: HashMap<String, Arc<dyn ISigbotStrategyExecutor + Send + Sync>>,
}

impl SigbotStrategyExecutorFactory {
    fn new() -> Self {
        SigbotStrategyExecutorFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotStrategyExecutorFactory> {
        &SINGLE_INSTANCE
    }

    /// Initialize strategy executor for a workflow node
    pub async fn init(
        workflow_id: String,
        node_id: String,
        provider: StrategyProvider,
        argument: Arc<SigbotStrategyArgument>,
    ) -> Result<Arc<dyn ISigbotStrategyExecutor + Send + Sync>, Error> {
        let executor_id = format!("{}:{}", workflow_id, node_id);
        debug!(
            "Registering Strategy Executor with id: {}, provider: {}",
            executor_id,
            provider.as_str()
        );

        let executor: Arc<dyn ISigbotStrategyExecutor + Send + Sync> = match provider {
            StrategyProvider::PYCODE => {
                let py_executor = SigbotPythonStrategyExecutor::new(argument).await;
                Self::get()
                    .write()
                    .unwrap()
                    .register0(executor_id.clone(), py_executor.clone())
                    .map_err(|e| Error::msg(format!("Failed to register PYCODE executor: {}", e)))?;
                py_executor as Arc<dyn ISigbotStrategyExecutor + Send + Sync>
            }
            StrategyProvider::LLM => {
                let llm_executor = SigbotLLMStrategyExecutor::new(argument).await;
                Self::get()
                    .write()
                    .unwrap()
                    .register0(executor_id.clone(), llm_executor.clone())
                    .map_err(|e| Error::msg(format!("Failed to register LLM executor: {}", e)))?;
                llm_executor as Arc<dyn ISigbotStrategyExecutor + Send + Sync>
            }
        };

        info!(
            "Registered the Strategy Executor with id: {}, provider: {}",
            executor_id,
            provider.as_str()
        );
        Ok(executor)
    }

    /// Register a strategy executor with a unique id
    /// Same provider can be registered multiple times with different ids
    fn register0<T: ISigbotStrategyExecutor + Send + Sync + 'static>(
        &mut self,
        id: String,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(&id) {
            debug!("Already register the Strategy Executor with id '{}'", id);
            return Ok(handler);
        }
        self.implementations.insert(id.clone(), handler.to_owned());
        debug!("Registered Strategy Executor with id: {}", id);
        Ok(handler)
    }

    /// Get strategy executor by id
    pub async fn get_impl(id: String) -> Result<Arc<dyn ISigbotStrategyExecutor + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotStrategyExecutorFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&id) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Strategy Executor with id '{}'.", id);
            return Err(Error::msg(errmsg));
        }
    }

    /// Unregister and shutdown a strategy executor by id
    pub async fn close(id: String) -> Result<(), Error> {
        let executor = {
            let mut this = SigbotStrategyExecutorFactory::get().write().unwrap();
            this.implementations.remove(&id)
        };
        if let Some(executor) = executor {
            executor.shutdown().await;
            info!("Unregistered and shutdown Strategy Executor with id: {}", id);
            Ok(())
        } else {
            Err(Error::msg(format!("Strategy Executor with id '{}' not found", id)))
        }
    }

    /// Close all registered strategy executors
    pub async fn shutdown() {
        let executors: Vec<_> = {
            let this = SigbotStrategyExecutorFactory::get().read().unwrap();
            this.implementations.values().cloned().collect()
        };

        for executor in executors {
            executor.shutdown().await;
        }

        // Clear all implementations
        {
            let mut this = SigbotStrategyExecutorFactory::get().write().unwrap();
            this.implementations.clear();
        }

        info!("Closed all Strategy Executors");
    }
}

#[cfg(test)]
mod tests {}
