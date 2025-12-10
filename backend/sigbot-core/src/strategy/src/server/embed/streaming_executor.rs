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
use crate::server::embed::sdk::series::Series;
use anyhow::{Context, Result};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use sigbot_types::modules::exchange::models::trade_market::KlineResult;
use sigbot_types::modules::strategy::models::strategy_embed::StrategyExecutionResult;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Strategy execution environment for streaming mode
/// Maintains state for each symbol+timeframe combination
#[derive(Clone)]
pub struct StrategyEnvironment {
    pub symbol: String,
    pub timeframe: String,
    pub klines: VecDeque<KlineResult>,
    pub close: Series,
    pub high: Series,
    pub low: Series,
    pub open: Series,
    pub volume: Series,
    pub strategy_code: String,
    pub parameters: HashMap<String, String>,
    pub max_klines: usize,
}

impl StrategyEnvironment {
    pub fn new(
        symbol: String,
        timeframe: String,
        strategy_code: String,
        parameters: HashMap<String, String>,
        max_klines: Option<usize>,
    ) -> Self {
        let max = max_klines.unwrap_or(1000);
        Self {
            symbol,
            timeframe,
            klines: VecDeque::with_capacity(max),
            close: Series::new(Some(max)),
            high: Series::new(Some(max)),
            low: Series::new(Some(max)),
            open: Series::new(Some(max)),
            volume: Series::new(Some(max)),
            strategy_code,
            parameters,
            max_klines: max,
        }
    }

    /// Update environment with a new K-line
    pub fn on_bar(&mut self, kline: KlineResult) {
        // Add to klines deque
        self.klines.push_back(kline.clone());
        if self.klines.len() > self.max_klines {
            self.klines.pop_front();
        }

        // Update series
        self.close.append(kline.close_price);
        self.high.append(kline.high_price);
        self.low.append(kline.low_price);
        self.open.append(kline.open_price);
        self.volume.append(kline.volume);
    }

    /// Get environment key (symbol_timeframe)
    pub fn key(&self) -> String {
        format!("{}_{}", self.symbol, self.timeframe)
    }
}

/// Streaming strategy executor
/// Processes market data incrementally, maintaining state for each symbol+timeframe
pub struct StreamingStrategyExecutor {
    pyo3_executor: Arc<PyO3StrategyExecutor>,
    environments: Arc<Mutex<HashMap<String, StrategyEnvironment>>>,
}

impl StreamingStrategyExecutor {
    pub fn new(pyo3_executor: Arc<PyO3StrategyExecutor>) -> Self {
        Self {
            pyo3_executor,
            environments: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Process a new K-line (streaming mode)
    /// This is called for each new market data update
    pub fn on_bar(
        &self,
        symbol: &str,
        timeframe: &str,
        kline: KlineResult,
        strategy_code: &str,
        parameters: Option<HashMap<String, String>>,
    ) -> Result<StrategyExecutionResult> {
        let start_time = std::time::Instant::now();

        // Ensure Python interpreter is initialized
        self.pyo3_executor.ensure_initialized()?;

        // Get or create environment
        let env_key = format!("{}_{}", symbol, timeframe);
        let mut envs = self.environments.lock().unwrap();
        let env = envs.entry(env_key.clone()).or_insert_with(|| {
            StrategyEnvironment::new(
                symbol.to_string(),
                timeframe.to_string(),
                strategy_code.to_string(),
                parameters.unwrap_or_default(),
                Some(1000),
            )
        });

        // Update environment with new K-line
        env.on_bar(kline);

        // Execute strategy code
        let result = Python::with_gil(|py| self.execute_strategy(py, env, strategy_code));

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

    /// Execute strategy code in Python with environment context
    fn execute_strategy(&self, py: Python<'_>, env: &StrategyEnvironment, strategy_code: &str) -> Result<String> {
        // Create strategy execution module
        let strategy_module = PyModule::new_bound(py, "strategy_env")?;
        let globals = strategy_module.dict();

        // Inject environment data
        let env_dict = PyDict::new_bound(py);

        // Create Series objects for Python
        let close_series = Py::new(py, env.close.clone())?;
        let high_series = Py::new(py, env.high.clone())?;
        let low_series = Py::new(py, env.low.clone())?;
        let open_series = Py::new(py, env.open.clone())?;
        let volume_series = Py::new(py, env.volume.clone())?;

        env_dict.set_item("close", close_series)?;
        env_dict.set_item("high", high_series)?;
        env_dict.set_item("low", low_series)?;
        env_dict.set_item("open", open_series)?;
        env_dict.set_item("volume", volume_series)?;
        env_dict.set_item("symbol", env.symbol.as_str())?;
        env_dict.set_item("timeframe", env.timeframe.as_str())?;

        // Inject parameters
        let params_dict = PyDict::new_bound(py);
        for (k, v) in &env.parameters {
            params_dict.set_item(k, v)?;
        }
        env_dict.set_item("parameters", params_dict)?;

        globals.set_item("env", env_dict)?;

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

    /// Get environment for a symbol+timeframe
    pub fn get_environment(&self, symbol: &str, timeframe: &str) -> Option<StrategyEnvironment> {
        let envs = self.environments.lock().unwrap();
        let key = format!("{}_{}", symbol, timeframe);
        envs.get(&key).cloned()
    }

    /// Clear environment for a symbol+timeframe
    pub fn clear_environment(&self, symbol: &str, timeframe: &str) {
        let mut envs = self.environments.lock().unwrap();
        let key = format!("{}_{}", symbol, timeframe);
        envs.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sigbot_types::modules::exchange::models::trade_market::KlineResult;

    #[test]
    fn test_strategy_environment() {
        let mut env = StrategyEnvironment::new(
            "BTCUSDT".to_string(),
            "1m".to_string(),
            "test_code".to_string(),
            HashMap::new(),
            Some(100),
        );

        let kline = KlineResult {
            open_time: 1000,
            open_price: 100.0,
            high_price: 105.0,
            low_price: 95.0,
            close_price: 102.0,
            volume: 1000.0,
            close_time: 2000,
        };

        env.on_bar(kline);
        assert_eq!(env.close.len(), 1);
        assert_eq!(env.klines.len(), 1);
    }
}
