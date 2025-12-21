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

use crate::executor::strategy_python::SigbotPythonStrategyExecutor;
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

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<
        (
            Arc<dyn ISigbotStrategyExecutor + Send + Sync>,
            Arc<SigbotStrategyArgument>,
        ),
        Error,
    > {
        debug!("Starting Strategy Executor ...");

        // e.g '--strategy-runner-provider=python'
        let provider = StrategyProvider::of(
            &matches
                .get_one::<String>("STRATEGY_RUNNER_PROVIDER")
                .unwrap_or(&StrategyProvider::PYTHON.as_str().to_owned()),
        )?;

        debug!("Registering Strategy Executor with provider: {}", &provider.as_str());

        // e.g '--strategy-runner-configuration=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("STRATEGY_RUNNER_CONFIGURATION")
            .map(|s| s.map(|s| s.to_owned()).unwrap_or_default())
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            SigbotStrategyArgument::from_json(
                &decode_arg_configuration(&configuration)
                    .map_err(|e| Error::msg(format!("Failed to decode the configuration: {}", e)))?,
            )
            .map_err(|e| Error::msg(format!("Failed to parse the configuration: {}", e)))?,
        );

        match provider {
            StrategyProvider::PYTHON => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        provider.as_str(),
                        SigbotPythonStrategyExecutor::new(argument.to_owned()).await, // TODO: set up run configuration?
                    )
                    .expect(
                        &format!(
                            "Failed to register the Strategy Executor with provider: {}.",
                            &provider.as_str()
                        )
                        .as_str(),
                    );
            }
        };

        let registered = Self::get_implementation(provider.as_str().to_owned())
            .await
            .expect(&format!(
                "Failed to get the registered Strategy Executor with provider: {}.",
                &provider.as_str()
            ));
        info!("Registered the Strategy Executor with provider: {}", &provider.as_str());

        Ok((registered, argument))
    }

    fn register0<T: ISigbotStrategyExecutor + Send + Sync + 'static>(
        &mut self,
        name: &str,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(name) {
            tracing::debug!("Already register the Strategy Executor '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotStrategyExecutor + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotStrategyExecutorFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Strategy Executor '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn shutdown() {
        let this = SigbotStrategyExecutorFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {}
