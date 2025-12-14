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

use anyhow::{Context, Error, Result};
use common_telemetry::info;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::{PyDict, PyModule};
use sigbot_strategy_sdk::sdk::core::models::trade_signal::TradingSignal;
use sigbot_types::modules::exchange::models::trade_position::EntryTradePosition;
use sigbot_types::modules::strategy::models::strategy_sdk::{StrategyExecutionInput, StrategyExecutionResult};
use sigbot_types::modules::strategy::SigbotStrategyArgument;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Streaming strategy executor
/// Processes market data incrementally with a shared global module
/// All symbol+timeframe combinations share the same Python module, allowing
/// global variables to be accessed across all on_process() calls
pub struct StreamingStrategyExecutor {
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

impl StreamingStrategyExecutor {
    pub fn new(strategy_argument: Arc<SigbotStrategyArgument>) -> Self {
        Self {
            strategy_argument,
            environment: Arc::new(Mutex::new(HashMap::new())),
            initialized_flag: Arc::new(Mutex::new(false)),
            initialized_pymodule: Arc::new(Mutex::new(None)),
        }
    }

    pub fn init(&self) -> Result<(), Error> {
        self.setup_environment(self.strategy_argument.to_owned().sys_environment.as_ref());
        Ok(())
    }

    fn setup_environment(&self, sys_environment: Option<&HashMap<String, String>>) {
        // Merge the All environment variables.
        if let Some(env_vars) = sys_environment {
            let mut env = self.environment.lock().unwrap();
            for (key, value) in env_vars {
                env.insert(key.to_string(), value.to_string());
            }
        }
    }

    /// Process a new market data update (streaming mode)
    /// This is called for each new market data update
    /// All symbol+timeframe combinations share the same Python module,
    /// allowing global variables to be accessed across all calls
    pub fn process(
        &self,
        input: &StrategyExecutionInput,
    ) -> Result<(StrategyExecutionResult, Option<EntryTradePosition>), Error> {
        let start_time = Instant::now();

        // Check if we need to initialize (only once, shared across all symbol+timeframe)
        let need_init = {
            let mut initialized = self.initialized_flag.lock().expect("Failed to lock initialized");
            if !*initialized {
                *initialized = true;
                true
            } else {
                false
            }
        };

        // Execute strategy code
        // Note: Python::with_gil() only acquires the GIL, it doesn't create a new Python interpreter instance.
        // The Python interpreter is a global singleton that persists across all with_gil() calls.
        // The PyModule is shared across all symbol+timeframe combinations, so global variables
        // set in init() will be accessible in all on_process() calls.
        let result = Python::with_gil(|py| {
            if need_init {
                // First time: compile code and call init()
                // init() has no parameters, allowing strategy developers to set global variables directly
                self.call_init(py, input)?;
            }
            // Subsequent calls: use the compiled module and call on_process(context)
            // The module's globals dictionary is shared, so all global variables set in init()
            // are accessible here across all symbol+timeframe combinations
            self.call_on_process(py, input)
        });

        let duration_ms = start_time.elapsed().as_millis() as u64;

        match result {
            Ok(trade_signal) => Ok((
                StrategyExecutionResult {
                    success: true,
                    result: Some(if trade_signal.is_some() {
                        "Signal generated".to_string()
                    } else {
                        "No signal".to_string()
                    }),
                    error: None,
                    duration_ms,
                },
                trade_signal,
            )),
            Err(e) => Ok((
                StrategyExecutionResult {
                    success: false,
                    result: None,
                    error: Some(format!("{}", e)),
                    duration_ms,
                },
                None,
            )),
        }
    }

    /// Initialize strategy: compile code once and call init()
    ///
    /// This function:
    /// 1. Creates a shared PyModule that persists across Python::with_gil() calls
    /// 2. Compiles user strategy code into the module's globals dictionary
    /// 3. Calls init() function (no parameters), which can set global variables in the module's globals
    /// 4. Saves the module as Py<PyModule> for later use in on_process()
    ///
    /// The module's globals dictionary is shared across all symbol+timeframe combinations,
    /// so global variables set in init() will be accessible in all subsequent on_process() calls.
    fn call_init<'py>(&self, py: Python<'py>, input: &StrategyExecutionInput) -> Result<()> {
        // Check if module already exists (shouldn't happen due to needs_init check, but be safe)
        {
            let module = self.initialized_pymodule.lock().unwrap();
            if module.is_some() {
                // Module already exists, skip initialization
                return Ok(());
            }
        }

        // Register sigbotlib module if not already registered
        if py.import_bound("sigbotlib").is_err() {
            let sigbotlib_module = PyModule::new_bound(py, "sigbotlib")?;
            sigbot_strategy_sdk::sdk::core::lib::sigbotlib(&sigbotlib_module)?;
            let sys_modules = py.import_bound("sys")?.getattr("modules")?;
            let sys_modules_dict = sys_modules
                .downcast::<PyDict>()
                .map_err(|_| anyhow::anyhow!("Failed to downcast sys.modules to PyDict"))?;
            sys_modules_dict.set_item("sigbotlib", sigbotlib_module)?;
        }

        // Create shared strategy execution module
        // This module will persist across Python::with_gil() calls because we store it as Py<PyModule>
        // All symbol+timeframe combinations will use this same module
        let strategy_module = PyModule::new_bound(py, "strategy_global")?;
        let globals = strategy_module.dict();

        // Compile user strategy code
        py.run_bound(&input.code, Some(&globals), None).map_err(|e| {
            let error_msg = format!("Failed to compile strategy code: {}", e);
            eprintln!("Python compilation error: {}", error_msg);
            anyhow::anyhow!(error_msg)
        })?;

        // Check if init() and on_process() functions exist
        let check_code = r#"
has_init = callable(globals().get('init', None))
has_on_process = callable(globals().get('on_process', None))
"#;
        py.run_bound(check_code, Some(&globals), None)
            .context("Failed to check for functions")?;

        let has_init = globals
            .get_item("has_init")?
            .map(|obj| obj.extract::<bool>())
            .and_then(|result| result.ok())
            .unwrap_or(false);
        let has_on_process = globals
            .get_item("has_on_process")?
            .map(|obj| obj.extract::<bool>())
            .and_then(|result| result.ok())
            .unwrap_or(false);

        if !has_on_process {
            return Err(anyhow::anyhow!(
                "Strategy code must define on_process(context) function"
            ));
        }

        // Call init() if it exists
        // init() has no parameters, allowing strategy developers to set global variables directly
        // init() runs in the module's globals dictionary, so any global variables
        // set in init() (e.g., cfg_btcusdc_dict = {}, kline_btcusdc_dict = {}) will be stored
        // in the module's globals and persist across all subsequent on_process() calls
        if has_init {
            // Call init() directly - if it fails, PyErr will contain full traceback automatically
            // No need to call Python traceback module, PyErr's Display already includes it
            py.run_bound("init()", Some(&globals), None).map_err(|e| {
                // PyErr already contains full traceback, no need to call Python traceback module
                anyhow::anyhow!("init() failed: {}", e)
            })?;
        }

        // Save compiled module for later use
        // IMPORTANT: Py<PyModule> stores a reference to a persistent Python module object.
        // The module's globals dictionary (__dict__) persists across all Python::with_gil() calls
        // because Python::with_gil() only acquires the GIL - it doesn't create a new interpreter.
        // The Python interpreter is a global singleton that lives for the entire process lifetime.
        // Therefore, global variables set in init() will be accessible in all on_process() calls
        // across all symbol+timeframe combinations.
        let module_py = strategy_module.unbind();
        let mut module = self.initialized_pymodule.lock().unwrap();
        *module = Some(module_py);

        Ok(())
    }

    /// Call on_process(context) using the compiled module
    ///
    /// This function:
    /// 1. Retrieves the shared PyModule that was created and saved in call_init()
    /// 2. Binds the module to get access to its globals dictionary
    /// 3. The globals dictionary is the SAME one used in init(), so all global variables
    ///    set in init() (e.g., cfg_btcusdc_dict, kline_btcusdc_dict) are accessible here
    /// 4. Creates context dict with kline_data and market_data
    /// 5. Calls on_process(context) function
    ///
    /// Note: module_py.bind(py) returns a Bound reference to the SAME Python module object
    /// that was created in call_init(). The module's globals dictionary persists because
    /// Python::with_gil() doesn't create a new interpreter - it only acquires the GIL.
    fn call_on_process(&self, py: Python<'_>, input: &StrategyExecutionInput) -> Result<Option<EntryTradePosition>> {
        // Get compiled module reference
        let module_guard = self.initialized_pymodule.lock().unwrap();
        let module_py = module_guard.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Initialized the strategy pymodule not found. Make sure init() was called first.")
        })?;

        // Bind module to get globals (this creates a new Bound reference to the SAME module object)
        // This is the EXACT SAME module as used in call_init(), so global variables set in init()
        // are accessible here via the module's globals dictionary (__dict__)
        // The module object persists across Python::with_gil() calls because the Python interpreter
        // is a global singleton, and Py<PyModule> stores a reference to the persistent module object
        let module = module_py.bind(py);
        let globals = module.dict();
        // Drop the lock before continuing
        drop(module_guard);

        // Create context dict with kline_data and market_data
        let context_dict = self.build_context_dict(py, &input.context)?;

        // Get on_process function from module globals
        let on_process_func = globals
            .get_item("on_process")?
            .ok_or_else(|| anyhow::anyhow!("on_process(context) function not found in strategy code"))?;

        // Call on_process(context) directly using PyO3's call1 method
        // Strategy code should return TradingSignal object or None, errors will be caught here
        let result_value: Bound<'_, PyAny> = on_process_func.call1((context_dict,)).map_err(|e| {
            // PyErr already contains full traceback, no need to call Python traceback module
            anyhow::anyhow!("on_process() failed: {}", e)
        })?;

        // Check if result is None
        if result_value.is_none() {
            return Ok(None);
        }

        // Try to extract as TradingSignal
        if let Ok(trading_signal) = result_value.extract::<PyRef<TradingSignal>>() {
            // Convert TradingSignal to EntryTradePosition
            let entry_position = trading_signal.to_entry_trade_position();
            Ok(Some(entry_position))
        } else {
            // If not TradingSignal, return None (or could log a warning)
            Ok(None)
        }
    }

    /// Create context dict with kline_data and market_data
    ///
    /// Args:
    /// - py: Python interpreter
    /// - context: Strategy context
    ///
    /// Returns:
    /// - Context dictionary with kline_data and market_data
    ///   Example:
    ///   ```python
    ///   {
    ///     "kline_data": {"BTCUSDC::3m": [{"open": 100000, "high": 105000, "low": 99000, "close": 102000, "volume": 100000}]},
    ///     "market_data": {"truthsocial::posts::trump": {"2025-09-29T13:04:21.071Z": "In order to make North Carolina, which has completely lost its furniture business to China, and other Countries, GREAT again, I will be imposing substantial Tariffs on any Country that does not make its furniture in the United States. Details to follow!!! President DJT"}}
    ///   }
    /// ```
    fn build_context_dict<'py>(
        &self,
        py: Python<'py>,
        context: &sigbot_types::modules::strategy::models::strategy_sdk::StrategyContext,
    ) -> Result<Bound<'py, PyDict>> {
        // Import json module for parsing JSON strings
        let json_module = py.import_bound("json")?;
        let json_loads = json_module.getattr("loads")?;

        let context_dict = PyDict::new_bound(py);

        // Inject kline_data into context object
        // e.g: {"btcusdc:5m": [{"open": 100, "high": 105, "low": 95, "close": 102, "volume": 1000}, ...], ...}
        if let Some(kline_data_map) = &context.kline_data {
            let batch_kline_dict = PyDict::new_bound(py);
            for (key, klines) in kline_data_map {
                // Convert Vec<KlineModel> to Python list of dicts
                let kline_list = pyo3::types::PyList::empty_bound(py);
                for kline in klines {
                    let kline_dict = PyDict::new_bound(py);
                    kline_dict.set_item("open", kline.open_price)?;
                    kline_dict.set_item("high", kline.high_price)?;
                    kline_dict.set_item("low", kline.low_price)?;
                    kline_dict.set_item("close", kline.close_price)?;
                    kline_dict.set_item("volume", kline.volume)?;
                    kline_dict.set_item("open_time", kline.open_time)?;
                    kline_dict.set_item("close_time", kline.close_time)?;
                    kline_list.append(kline_dict)?;
                }
                batch_kline_dict.set_item(key, kline_list)?;
            }
            context_dict.set_item("kline_data", batch_kline_dict)?;
        } else {
            // If no kline_data, set empty dict
            context_dict.set_item("kline_data", PyDict::new_bound(py))?;
        }

        // Inject market_data into context object
        // e.g: {"truthsocial::posts::trump": {"2025-10-25T12:54:52.605Z": "In order to make North Carolina, which has completely lost its furniture business to China, and other Countries, GREAT again, I will be imposing substantial Tariffs on any Country that does not make its furniture in the United States. Details to follow!!! President DJT"}}
        if let Some(market_data_map) = &context.market_data {
            let market_data_dict = PyDict::new_bound(py);
            for (data_source_key, inner_map) in market_data_map {
                // inner_map is HashMap<String, String>
                let inner_dict = PyDict::new_bound(py);
                for (k, v) in inner_map {
                    // Try to parse JSON string, fallback to string if fails
                    let data_obj: Bound<'_, PyAny> = json_loads.call1((v,)).or_else(|_| {
                        // If the parsing fails, pass it as a string
                        Ok::<Bound<'_, PyAny>, PyErr>(pyo3::types::PyString::new_bound(py, v).as_any().clone())
                    })?;
                    inner_dict.set_item(k, data_obj)?;
                }
                market_data_dict.set_item(data_source_key, inner_dict)?;
            }
            context_dict.set_item("market_data", market_data_dict)?;
        } else {
            // If no market_data, set empty dict
            context_dict.set_item("market_data", PyDict::new_bound(py))?;
        }

        Ok(context_dict)
    }

    pub fn shutdown(&self) -> Result<(), Error> {
        info!("Shutting down Streaming Strategy Executor. Done.");

        let mut module = self.initialized_pymodule.lock().unwrap();
        *module = None;
        let mut initialized = self.initialized_flag.lock().unwrap();
        *initialized = false;

        self.environment.lock().unwrap().clear();
        info!("Shutdown Streaming Strategy Executor. Done.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sigbot_types::modules::{
        exchange::models::{
            trade_market::KlineModel,
            trade_position::{OrderType, TradeSide},
        },
        messaging::messaging::MessagingInfo,
        strategy::{models::strategy_sdk::StrategyContext, strategy::StrategyInfo},
    };
    use std::thread;

    const TEST_USER_CODE: &str = r#"
import time
from sigbotlib import indicators, risk, signals, utils, data
from sigbotlib import TradingSignal, PyTradeSide, PyOrderType, PyEntryPosition, PyExitPosition

btcusdc_cfg_dict = {}
btcusdc_3m_kline_series = {}
truthsocial_trump_dict = {}

def init():
    global btcusdc_cfg_dict, btcusdc_3m_kline_series, truthsocial_trump_dict
    btcusdc_cfg_dict["series_limit"] = 3000
    btcusdc_3m_kline_series["BTCUSDC::3m"] = []
    print(f"----- [DEBUG] Strategy initialized. -----")
    print(f"btcusdc_cfg_dict: {btcusdc_cfg_dict}")
    print(f"btcusdc_3m_kline_series: {btcusdc_3m_kline_series}")
    print(f"truthsocial_trump_dict: {truthsocial_trump_dict}")

def on_process(context):
    global btcusdc_cfg_dict, btcusdc_3m_kline_series, truthsocial_trump_dict
    batch_klines = context.get("kline_data", {}).get("BTCUSDC::3m")
    print(f"[DEBUG] Extracted latest klines series: {batch_klines}")

    if batch_klines:
        btcusdc_3m_kline_series["BTCUSDC::3m"].extend(batch_klines)
        print(f"----- [DEBUG] Updated cached klines series: {len(btcusdc_3m_kline_series['BTCUSDC::3m'])} -----")
        print(f"btcusdc_3m_kline_series: {btcusdc_3m_kline_series}")

        market_data = context.get("market_data", {})
    if market_data:
        trump_data = market_data.get("truthsocial::posts::trump", {})
        for key, value in trump_data.items():
            truthsocial_trump_dict[key] = value

    print(f"----- [DEBUG] Calculating cost time ... -----")
    time.sleep(0.01)
    print(f"----- [DEBUG] Calculated cost time ... done -----")

    return TradingSignal.long(
        symbol="BTCUSDC",
        quantity=0.1,
        price=99000.0,
        stop_loss_price=98000.0,
        stop_profit_price=105000.0,
        time=0,
        description="Mock Long signal"
    )
    "#;

    const TEST_KLINE_1: KlineModel = KlineModel {
        open_price: 100000.0,
        high_price: 105000.0,
        low_price: 99000.0,
        close_price: 102000.0,
        volume: 100000.0,
        open_time: 1717000000000,
        close_time: 1717000000000,
    };

    const TEST_KLINE_2: KlineModel = KlineModel {
        open_price: 101000.0,
        high_price: 105000.0,
        low_price: 99000.0,
        close_price: 103000.0,
        volume: 100000.0,
        open_time: 1717000000001,
        close_time: 1717000000001,
    };

    fn build_test_context() -> StrategyContext {
        StrategyContext {
            kline_data: Some( HashMap::from([("BTCUSDC::3m".to_string(), vec![TEST_KLINE_1, TEST_KLINE_2])])),
            market_data: Some( HashMap::from([("truthsocial::posts::trump".to_string(), HashMap::from([("2025-09-29T13:04:21.071Z".to_string(), "In order to make North Carolina, which has completely lost its furniture business to China, and other Countries, GREAT again, I will be imposing substantial Tariffs on any Country that does not make its furniture in the United States. Details to follow!!! President DJT".to_string())]))])),
        }
    }

    #[test]
    fn test_streaming_executor() {
        let executor = StreamingStrategyExecutor::new(Arc::new(SigbotStrategyArgument {
            strategy_config: Arc::new(StrategyInfo::default()),
            messaging_config: Arc::new(MessagingInfo::default()),
            sys_environment: Some(HashMap::new()),
            run_mode: "STREAMING".to_string(),
        }));

        let result = executor
            .process(&StrategyExecutionInput {
                code: TEST_USER_CODE.to_string(),
                context: build_test_context(),
                run_mode: "STREAMING".to_string(),
            })
            .expect("Failed to process strategy");

        eprintln!("Strategy execution result success: {:?}", result.0.success);
        eprintln!("Strategy execution result result: {:?}", result.0.result);
        eprintln!("Strategy execution result error: {:?}", result.0.error);
        eprintln!("Strategy execution result duration: {:?}ms", result.0.duration_ms);

        assert!(
            result.0.success,
            "Strategy execution should succeed. Error: {:?}",
            result.0.error
        );
        assert!(result.1.is_some());
        let entry_position = result.1.as_ref().unwrap();
        assert!(entry_position.open_pos.symbol == "BTCUSDC");
        assert!(entry_position.open_pos.side == TradeSide::LONG);
        assert!(entry_position.open_pos.order_type == OrderType::LIMITED);
        assert!(entry_position.open_pos.quantity == 0.1);
        assert!(entry_position.open_pos.price.is_some());
        assert!(entry_position.open_pos.price.unwrap() == 99000.0);
        assert!(entry_position.stop_loss.as_ref().unwrap().symbol == "BTCUSDC");
        assert!(entry_position.stop_loss.as_ref().unwrap().quantity_percent == 1.0);
        assert!(entry_position.stop_loss.as_ref().unwrap().price.is_some());
        assert!(entry_position.stop_loss.as_ref().unwrap().price.unwrap() == 98000.0);
        assert!(entry_position.stop_profit.as_ref().unwrap().symbol == "BTCUSDC");
        assert!(entry_position.stop_profit.as_ref().unwrap().quantity_percent == 1.0);
        assert!(entry_position.stop_profit.as_ref().unwrap().price.is_some());
        assert!(entry_position.stop_profit.as_ref().unwrap().price.unwrap() == 105000.0);
        assert!(entry_position.description == "Mock Long signal");
        assert!(entry_position.stop_loss.is_some());
        assert!(entry_position.stop_profit.is_some());
    }

    #[test]
    fn test_streaming_executor_multi_thread_and_mock_cost() {
        let executor = Arc::new(StreamingStrategyExecutor::new(Arc::new(SigbotStrategyArgument {
            strategy_config: Arc::new(StrategyInfo::default()),
            messaging_config: Arc::new(MessagingInfo::default()),
            sys_environment: Some(HashMap::new()),
            run_mode: "STREAMING".to_string(),
        })));

        let threads = (0..10).map(|i| {
            let executor0 = executor.to_owned();
            thread::spawn(move || {
                println!("[DEBUG] Thread {} starting...", i);
                let result = executor0
                    .process(&StrategyExecutionInput {
                        code: TEST_USER_CODE.to_string(),
                        context: build_test_context(),
                        run_mode: "STREAMING".to_string(),
                    })
                    .expect("Failed to process strategy");
                println!("[DEBUG] Thread {} done. - result: {:?}\n", i, result);
            })
        });
        for thread in threads {
            thread.join().unwrap();
        }
        println!("[DEBUG] All threads done.");
    }
}
