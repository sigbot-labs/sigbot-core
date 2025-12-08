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

use crate::server::{
    embed::pyo3_executor::{PyO3StrategyExecutor, StrategyContext, StrategyExecutionResult},
    strategy_factory::ISigbotStrategyRunner,
};
use anyhow::Result;
use async_trait::async_trait;
use common_telemetry::{info, warn};
use std::sync::Arc;

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
    pub async fn execute(&self, code: &str, context: Option<StrategyContext>) -> Result<StrategyExecutionResult> {
        info!("Processing strategy embed ...");
        let result = self.pyo3_executor.execute(code, context);
        if let Err(ref e) = result {
            warn!("Failed to execute strategy embed: {}", e);
        } else {
            info!("Strategy embed executed successfully");
        }
        result
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
