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

use anyhow::{Error, Result};
use common_telemetry::info;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use sigbot_types::modules::strategy::SigbotStrategyArgument;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Batch strategy executor
/// Processes entire dataset at once, suitable for research and backtesting
pub struct BatchStrategyExecutor {
    /// Strategy runtime argument (including system environment variables)
    strategy_argument: Arc<SigbotStrategyArgument>,
    /// Strategy runtime environment variables
    environment: Arc<Mutex<HashMap<String, String>>>,
    /// Whether init() has been called
    initialized_flag: Arc<Mutex<bool>>,
    /// Compiled Python module (shared across all symbol+timeframe combinations)
    /// This allows global variables set in init() to be accessible in all on_process() calls
    initialized_pymodule: Arc<Mutex<Option<Py<PyModule>>>>,
}

impl BatchStrategyExecutor {
    pub fn new(strategy_argument: Arc<SigbotStrategyArgument>) -> Self {
        Self {
            strategy_argument,
            environment: Arc::new(Mutex::new(HashMap::new())),
            initialized_flag: Arc::new(Mutex::new(false)),
            initialized_pymodule: Arc::new(Mutex::new(None)),
        }
    }

    pub fn init(&self) -> Result<(), Error> {
        self.ensure_initialized(self.strategy_argument.to_owned().sys_environment.as_ref())
    }

    /// Initialize the Python interpreter (thread-safe)
    fn ensure_initialized(&self, sys_environment: Option<&HashMap<String, String>>) -> Result<(), Error> {
        let mut initialized = self.initialized_flag.lock().unwrap();
        if !*initialized {
            // Merge the All environment variables.
            if let Some(env_vars) = sys_environment {
                let mut env = self.environment.lock().unwrap();
                for (key, value) in env_vars {
                    env.insert(key.to_string(), value.to_string());
                }
            }

            // // Initialize the Python interpreter
            // Python::with_gil(|py| {
            //     // The Python interpreter will be automatically initialized when it is first called (because the auto-initialize feature is used)
            //     info!("Python interpreter initialized");
            //     // Register sigbotlib module dynamically
            //     // We create the module and register it in built-in core modules
            //     let sigbotlib_module = PyModule::new_bound(py, "sigbotlib")?;
            //     sigbot_strategy_sdk::sdk::core::lib::sigbotlib(&sigbotlib_module)?;
            //     py.import_bound("sigbotlib")?
            //         // .getattr("core")?
            //         .set_item("sigbotlib", sigbotlib_module)?;
            //     // Verify required packages
            //     if let Err(e) = verify_required_packages(py) {
            //         error!("Failed to verify required packages: {}", e);
            //     }
            //     Ok::<(), anyhow::Error>(())
            // })
            // .context("Failed to initialize Python interpreter and register sigbotlib")?;
            *initialized = true;
        }
        Ok(())
    }

    pub fn shutdown(&self) -> Result<(), Error> {
        info!("Shutting down Batch Strategy Executor. Done.");

        let mut module = self.initialized_pymodule.lock().unwrap();
        *module = None;
        let mut initialized = self.initialized_flag.lock().unwrap();
        *initialized = false;

        self.environment.lock().unwrap().clear();
        info!("Shutdown Batch Strategy Executor. Done.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
