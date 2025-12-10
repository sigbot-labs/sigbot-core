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

use crate::server::embed::pyo3_executor::PyO3StrategyExecutor;
use anyhow::{Context, Result};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use sigbot_types::modules::exchange::models::trade_market::KlineResult;
use sigbot_types::modules::strategy::models::strategy_embed::StrategyExecutionResult;
use std::collections::HashMap;
use std::sync::Arc;

/// Batch strategy executor
/// Processes entire dataset at once, suitable for research and backtesting
pub struct BatchStrategyExecutor {
    pyo3_executor: Arc<PyO3StrategyExecutor>,
}

impl BatchStrategyExecutor {
    pub fn new(pyo3_executor: Arc<PyO3StrategyExecutor>) -> Self {
        Self { pyo3_executor }
    }

    /// Execute strategy on entire dataset (batch mode)
    /// This processes all K-lines at once, suitable for research/backtesting
    pub fn execute_batch(
        &self,
        klines: Vec<KlineResult>,
        strategy_code: &str,
        parameters: Option<HashMap<String, String>>,
    ) -> Result<StrategyExecutionResult> {
        let start_time = std::time::Instant::now();

        // Ensure Python interpreter is initialized
        self.pyo3_executor.ensure_initialized()?;

        let result =
            Python::with_gil(|py| self.execute_strategy_batch(py, &klines, strategy_code, parameters.as_ref()));

        let duration_ms = start_time.elapsed().as_millis() as u64;

        match result {
            Ok(output) => Ok(StrategyExecutionResult {
                success: true,
                result: Some(output),
                error: None,
                duration_ms,
            }),
            Err(e) => Ok(StrategyExecutionResult {
                success: false,
                result: None,
                error: Some(format!("{}", e)),
                duration_ms,
            }),
        }
    }

    /// Execute strategy code in Python with batch data
    fn execute_strategy_batch(
        &self,
        py: Python<'_>,
        klines: &[KlineResult],
        strategy_code: &str,
        parameters: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        // Create strategy execution module
        let strategy_module = PyModule::new_bound(py, "strategy_env")?;
        let globals = strategy_module.dict();

        // Convert K-lines to Python list of dicts
        let klines_list = PyList::empty_bound(py);
        for kline in klines {
            let kline_dict = PyDict::new_bound(py);
            kline_dict.set_item("open_time", kline.open_time)?;
            kline_dict.set_item("open_price", kline.open_price)?;
            kline_dict.set_item("high_price", kline.high_price)?;
            kline_dict.set_item("low_price", kline.low_price)?;
            kline_dict.set_item("close_price", kline.close_price)?;
            kline_dict.set_item("volume", kline.volume)?;
            kline_dict.set_item("close_time", kline.close_time)?;
            klines_list.append(kline_dict)?;
        }

        globals.set_item("klines", klines_list)?;

        // Extract price arrays for convenience
        let closes: Vec<f64> = klines.iter().map(|k| k.close_price).collect();
        let opens: Vec<f64> = klines.iter().map(|k| k.open_price).collect();
        let highs: Vec<f64> = klines.iter().map(|k| k.high_price).collect();
        let lows: Vec<f64> = klines.iter().map(|k| k.low_price).collect();
        let volumes: Vec<f64> = klines.iter().map(|k| k.volume).collect();

        let closes_list = PyList::empty_bound(py);
        for &close in &closes {
            closes_list.append(close)?;
        }
        globals.set_item("closes", closes_list)?;

        let opens_list = PyList::empty_bound(py);
        for &open in &opens {
            opens_list.append(open)?;
        }
        globals.set_item("opens", opens_list)?;

        let highs_list = PyList::empty_bound(py);
        for &high in &highs {
            highs_list.append(high)?;
        }
        globals.set_item("highs", highs_list)?;

        let lows_list = PyList::empty_bound(py);
        for &low in &lows {
            lows_list.append(low)?;
        }
        globals.set_item("lows", lows_list)?;

        let volumes_list = PyList::empty_bound(py);
        for &volume in &volumes {
            volumes_list.append(volume)?;
        }
        globals.set_item("volumes", volumes_list)?;

        // Inject parameters
        if let Some(params) = parameters {
            let params_dict = PyDict::new_bound(py);
            for (k, v) in params {
                params_dict.set_item(k, v)?;
            }
            globals.set_item("parameters", params_dict)?;
        } else {
            let params_dict = PyDict::new_bound(py);
            globals.set_item("parameters", params_dict)?;
        }

        // Wrap user code
        let wrapped_code = format!(
            r#"
import json
import traceback
import sigbot_sdk

try:
    # User strategy code
    {}
    
    # Try to get the result variable
    if 'result' in locals():
        result_value = result
    elif 'result' in globals():
        result_value = result
    else:
        result_value = None
    
    # Convert result to JSON string
    if result_value is None:
        output = json.dumps({{"status": "success", "result": None}})
    else:
        try:
            output = json.dumps(result_value, default=str)
        except (TypeError, ValueError):
            output = json.dumps({{"status": "success", "result": str(result_value)}})
except Exception as e:
    output = json.dumps({{
        "status": "error",
        "error": str(e),
        "traceback": traceback.format_exc()
    }})
"#,
            strategy_code
        );

        // Execute code
        py.run_bound(&wrapped_code, Some(&globals), None)
            .context("Failed to execute strategy code")?;

        // Get output
        let output_obj = globals.get_item("output").context("Failed to get output variable")?;
        let output: String = output_obj
            .and_then(|obj| obj.extract::<String>().ok())
            .context("Failed to extract execution result")?;

        // Parse JSON
        let result_json: serde_json::Value =
            serde_json::from_str(&output).context("Failed to parse execution result")?;

        // Check for errors
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

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_executor_creation() {
        let pyo3_executor = Arc::new(PyO3StrategyExecutor::new());
        let executor = BatchStrategyExecutor::new(pyo3_executor);
        // Just test that it can be created
        assert!(true);
    }
}
