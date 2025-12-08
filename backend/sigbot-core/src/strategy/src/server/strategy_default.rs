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

use crate::server::{embed::pyo3_executor::PyO3StrategyExecutor, strategy_factory::ISigbotStrategyRunner};
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info, warn};
use sigbot_messaging::client::messaging_factory::SigbotMessagingClientFactory;
use sigbot_types::modules::strategy::models::strategy_embed::StrategyExecutionInput;
use std::{future::Future, pin::Pin, sync::Arc};

#[derive(Clone)]
pub struct SigbotDefaultStrategyRunner {
    pyo3_executor: Arc<PyO3StrategyExecutor>,
}

impl SigbotDefaultStrategyRunner {
    pub const NAME: &'static str = "DEFAULT";

    pub async fn new() -> Arc<Self> {
        let executor = Arc::new(PyO3StrategyExecutor::new());

        // Check if the common data analysis packages are installed.
        let installed_packages = executor.get_installed_packages();
        if installed_packages.is_empty() {
            warn!("No common data analysis packages found. Users may need to install polars, pandas, numpy, etc.");
        } else {
            info!(
                "Detected installed common data analysis packages: {:?}",
                installed_packages
            );
        }

        Arc::new(Self {
            pyo3_executor: executor,
        })
    }

    /// Check if the specified Python package is installed
    pub fn check_package(&self, package_name: &str) -> bool {
        self.pyo3_executor.check_package_installed(package_name)
    }

    /// Get the list of installed Python packages
    pub fn get_installed_packages(&self) -> Vec<String> {
        self.pyo3_executor.get_installed_packages()
    }

    /// Execute strategy code
    ///
    /// # Parameters
    /// - `code`: Python strategy code string
    /// - `context`: Strategy execution context (market data, parameters, etc.)
    ///
    /// # Return
    /// Strategy execution result
    pub async fn execute(&self, matches: &clap::ArgMatches, verbose: bool) {
        info!("Initializing Messaging client.");
        let messaging = SigbotMessagingClientFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Messaging client.");
        info!("Initialized Messaging client. {:?}", messaging.name());

        // TODO: Subscribe market data from messaging with topics.
        let executor = self.pyo3_executor.to_owned();
        let handler: Arc<
            dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            debug!("Received message: {:?}", data);
            let executor0 = executor.to_owned();
            Box::pin(async move {
                let data0 = data.to_owned();
                let input: StrategyExecutionInput =
                    serde_json::from_slice(&data0).context("Failed to parse the data from the message.")?;
                debug!("Parsed input: {:?}", input);
                let result = executor0.execute(&input.code, Some(input.context));
                if let Err(ref e) = result {
                    warn!("Failed to execute strategy: {}", e);
                } else {
                    debug!("Executed strategy successfully. Result: {:?}", result);
                }
                Ok(data0)
            })
        });

        let _ = messaging
            .subscribe("sigbot/notification/alarm", handler) // TODO: configuable
            .await
            .expect("Failed to subscribe to the messaging topic.");
        info!("Initialized Messaging client with provider: {:?}.", messaging.name());
    }
}

#[async_trait]
impl ISigbotStrategyRunner for SigbotDefaultStrategyRunner {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn startup(&self) {
        info!("Starting Embed Strategy Runner.");

        // Warm up the Python interpreter.
        if let Err(e) = self.pyo3_executor.ensure_initialized() {
            warn!("Failed to initialize Python interpreter: {}", e);
        } else {
            info!("Python interpreter ready for strategy execution");
        }
    }

    async fn shutdown(&self) {
        info!("Shutting down Embed Strategy Runner.");
    }
}

#[cfg(test)]
mod tests {}
