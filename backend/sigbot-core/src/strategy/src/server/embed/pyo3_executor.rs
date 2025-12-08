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

use anyhow::{Context, Result};
use common_telemetry::{error, info};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule, PyString};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Strategy execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyExecutionResult {
    /// Whether the execution is successful
    pub success: bool,
    /// Return result (JSON string)
    pub result: Option<String>,
    /// Error message
    pub error: Option<String>,
    /// Execution duration (milliseconds)
    pub duration_ms: u64,
}

/// Strategy execution context data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyContext {
    /// Market Data（JSON format）
    pub market_data: Option<String>,
    /// Strategy parameters
    pub parameters: Option<HashMap<String, String>>,
    /// Other context data
    pub extra_data: Option<HashMap<String, String>>,
}

/// PyO3 strategy executor
///
/// This executor uses PyO3 to embed the Python interpreter, supporting:
/// - Executing Python code strings
/// - Importing third-party libraries (polars, pandas, numpy, etc.)
/// - Passing market data and strategy parameters
/// - Returning calculation results
pub struct PyO3StrategyExecutor {
    /// Whether the Python interpreter has been initialized
    initialized: Arc<Mutex<bool>>,
}

impl PyO3StrategyExecutor {
    /// Create a new executor instance
    pub fn new() -> Self {
        Self {
            initialized: Arc::new(Mutex::new(false)),
        }
    }

    /// Initialize the Python interpreter (thread-safe)
    pub fn ensure_initialized(&self) -> Result<()> {
        let mut initialized = self.initialized.lock().unwrap();
        if !*initialized {
            // Initialize the Python interpreter
            Python::with_gil(|_py| {
                // The Python interpreter will be automatically initialized when it is first called (because the auto-initialize feature is used)
                info!("Python interpreter initialized");
            });
            *initialized = true;
        }
        Ok(())
    }

    /// Execute Python strategy code
    ///
    /// # Parameters
    /// - `code`: Python code string
    /// - `context`: Strategy execution context data
    ///
    /// # Return
    /// Strategy execution result
    pub fn execute(&self, code: &str, context: Option<StrategyContext>) -> Result<StrategyExecutionResult> {
        let start_time = std::time::Instant::now();

        // Ensure the Python interpreter has been initialized
        self.ensure_initialized()?;

        Python::with_gil(|py| {
            let result = self.execute_internal(py, code, context);
            let duration_ms = start_time.elapsed().as_millis() as u64;

            match result {
                Ok(result_value) => Ok(StrategyExecutionResult {
                    success: true,
                    result: Some(result_value),
                    error: None,
                    duration_ms,
                }),
                Err(e) => {
                    error!("Strategy execution failed: {}", e);
                    Ok(StrategyExecutionResult {
                        success: false,
                        result: None,
                        error: Some(format!("{}", e)),
                        duration_ms,
                    })
                }
            }
        })
    }

    /// Internal execution method
    fn execute_internal(&self, py: Python<'_>, code: &str, context: Option<StrategyContext>) -> Result<String> {
        // Create the strategy execution environment module
        let strategy_module = PyModule::new_bound(py, "strategy_env")?;

        // Set the import path for common libraries to ensure that third-party libraries can be imported
        // These libraries should already be installed in the system's Python environment
        let setup_code = r#"
import sys
import os

# Ensure that third-party libraries can be imported
# Users can install polars, pandas, numpy, etc. using pip install
try:
    import polars as pl
except ImportError:
    pass

try:
    import pandas as pd
except ImportError:
    pass

try:
    import numpy as np
except ImportError:
    pass

try:
    import json
except ImportError:
    pass
"#;

        // Execute the setup code
        let setup_globals = strategy_module.dict();
        py.run_bound(setup_code, Some(&setup_globals), None)
            .context("Failed to setup strategy environment")?;

        // Prepare the execution context
        let globals = strategy_module.dict();

        // If there is context data, inject it into the Python environment
        if let Some(ctx) = context {
            // Inject the market data
            if let Some(market_data) = ctx.market_data {
                // Escape the single quotes in the JSON string
                let escaped_data = market_data.replace('\'', "\\'");
                let eval_code = format!("json.loads('{}')", escaped_data);
                let market_data_obj: Bound<'_, PyAny> = py.eval_bound(&eval_code, None, None).or_else(|_| {
                    // If the parsing fails, pass it as a string
                    Ok::<Bound<'_, PyAny>, PyErr>(PyString::new_bound(py, &market_data).as_any().clone())
                })?;
                globals.set_item("market_data", market_data_obj)?;
            }

            // Inject the strategy parameters
            if let Some(parameters) = ctx.parameters {
                let params_dict = PyDict::new_bound(py);
                for (k, v) in parameters {
                    params_dict.set_item(k, v)?;
                }
                globals.set_item("parameters", params_dict)?;
            }

            // Inject the extra data
            if let Some(extra_data) = ctx.extra_data {
                let extra_dict = PyDict::new_bound(py);
                for (k, v) in extra_data {
                    extra_dict.set_item(k, v)?;
                }
                globals.set_item("extra_data", extra_dict)?;
            }
        }

        // Wrap the user code, capture the return value
        let wrapped_code = format!(
            r#"
import json
import traceback

try:
    # User strategy code
    {}
    
    # Try to get the result variable, if not return None
    if 'result' in locals():
        result_value = result
    elif 'result' in globals():
        result_value = result
    else:
        result_value = None
    
    # Convert the result to a JSON string
    if result_value is None:
        output = json.dumps({{"status": "success", "result": None}})
    else:
        # Try to serialize to JSON
        try:
            output = json.dumps(result_value, default=str)
        except (TypeError, ValueError):
            # If it cannot be serialized, convert to a string
            output = json.dumps({{"status": "success", "result": str(result_value)}})
except Exception as e:
    output = json.dumps({{
        "status": "error",
        "error": str(e),
        "traceback": traceback.format_exc()
    }})
"#,
            code
        );

        // Execute the user code
        py.run_bound(&wrapped_code, Some(&globals), None)
            .context("Failed to execute strategy code")?;

        // Get the output result
        let output_obj = globals.get_item("output").context("Failed to get output variable")?;
        let output: String = output_obj
            .and_then(|obj| obj.extract::<String>().ok())
            .context("Failed to extract execution result")?;

        // Parse the output JSON
        let result_json: serde_json::Value =
            serde_json::from_str(&output).context("Failed to parse execution result")?;

        // Check if there is an error
        if let Some(status) = result_json.get("status") {
            if status == "error" {
                let error_msg = result_json
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown error");
                let traceback = result_json.get("traceback").and_then(|v| v.as_str()).unwrap_or("");
                return Err(anyhow::anyhow!("{}\n{}", error_msg, traceback));
            }
        }

        // Return the result
        Ok(output)
    }

    /// Execute Python code and return the original Python object
    ///
    /// This method allows for more flexible handling, but requires manual management of the Python GIL
    pub fn execute_raw<F, R>(&self, code: &str, context: Option<StrategyContext>, f: F) -> Result<R>
    where
        F: FnOnce(Python<'_>, Bound<'_, PyAny>) -> Result<R>,
    {
        self.ensure_initialized()?;

        Python::with_gil(|py| {
            let strategy_module = PyModule::new_bound(py, "strategy_env")?;
            let globals = strategy_module.dict();

            // Inject the context data
            if let Some(ctx) = context {
                if let Some(market_data) = ctx.market_data {
                    let escaped_data = market_data.replace('\'', "\\'");
                    let eval_code = format!("json.loads('{}')", escaped_data);
                    let market_data_obj: Bound<'_, PyAny> = py.eval_bound(&eval_code, None, None).or_else(|_| {
                        Ok::<Bound<'_, PyAny>, PyErr>(PyString::new_bound(py, &market_data).as_any().clone())
                    })?;
                    globals.set_item("market_data", market_data_obj)?;
                }

                if let Some(parameters) = ctx.parameters {
                    let params_dict = PyDict::new_bound(py);
                    for (k, v) in parameters {
                        params_dict.set_item(k, v)?;
                    }
                    globals.set_item("parameters", params_dict)?;
                }
            }

            // Execute the code
            let result = py.eval_bound(code, Some(&globals), None)?;

            // Call the user-provided processing function
            f(py, result)
        })
    }

    /// Check if the specified package is installed in the Python environment
    pub fn check_package_installed(&self, package_name: &str) -> bool {
        self.ensure_initialized().is_ok()
            && Python::with_gil(|py| {
                let check_code = format!("import {}", package_name);
                py.run_bound(&check_code, None, None).is_ok()
            })
    }

    /// Get the list of commonly used data analysis libraries that are installed
    pub fn get_installed_packages(&self) -> Vec<String> {
        let packages = vec!["polars", "pandas", "numpy", "scipy", "sklearn"];
        packages
            .into_iter()
            .filter(|pkg| self.check_package_installed(pkg))
            .map(|s| s.to_string())
            .collect()
    }
}

impl Default for PyO3StrategyExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let executor = PyO3StrategyExecutor::new();
        assert!(executor.ensure_initialized().is_ok());
    }

    #[test]
    fn test_simple_execution() {
        let executor = PyO3StrategyExecutor::new();
        let code = r#"
result = 42
"#;
        let result = executor.execute(code, None).unwrap();
        assert!(result.success);
        assert!(result.result.is_some());
    }

    #[test]
    fn test_execution_with_context() {
        let executor = PyO3StrategyExecutor::new();
        let code = r#"
import json
if isinstance(market_data, str):
    data = json.loads(market_data)
else:
    data = market_data
result = {
    "value": parameters.get("test_param", "default"),
    "data_length": len(str(data)) if data else 0
}
"#;
        let context = StrategyContext {
            market_data: Some(r#"{"price": 100, "volume": 1000}"#.to_string()),
            parameters: Some({
                let mut params = HashMap::new();
                params.insert("test_param".to_string(), "test_value".to_string());
                params
            }),
            extra_data: None,
        };
        let result = executor.execute(code, Some(context)).unwrap();
        assert!(result.success);
        assert!(result.result.is_some());
    }

    #[test]
    fn test_error_handling() {
        let executor = PyO3StrategyExecutor::new();
        let code = r#"
raise ValueError("Test error")
"#;
        let result = executor.execute(code, None).unwrap();
        assert!(!result.success);
        assert!(result.error.is_some());
    }
}
