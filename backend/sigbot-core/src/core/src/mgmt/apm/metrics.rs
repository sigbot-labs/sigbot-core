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

use crate::config::config::AppConfig;
use common_telemetry::{error, info};
use lazy_static::lazy_static;
use prometheus::{CounterVec, Gauge, GaugeVec, HistogramVec};
use std::sync::{Arc, Mutex};

/// MetricRegistrar: Metrics registration extension point trait
/// Other modules can implement this trait to register custom metrics
///
/// # Example
///
/// ```rust,no_run
/// use sigbot_core::mgmt::apm::metrics::MetricRegistrar;
/// use prometheus::CounterVec;
/// use lazy_static::lazy_static;
/// // Define custom metrics
/// lazy_static! {
///     static ref MY_CUSTOM_COUNTER: CounterVec = CounterVec::new(
///         prometheus::Opts::new("my_custom_counter", "My custom counter"),
///         &["label1", "label2"]
///     ).unwrap();
/// }
/// // Implement MetricRegistrar trait
/// struct MyCustomMetrics;
///
/// impl MetricRegistrar for MyCustomMetrics {
///     fn register_metrics(&self) -> Result<(), prometheus::Error> {
///         prometheus::register(Box::new(MY_CUSTOM_COUNTER.clone()))?;
///         Ok(())
///     }
/// }
/// // Register during initialization (usually called during application startup)
/// register_custom_registrar(Box::new(MyCustomMetrics));
/// ```
pub trait MetricRegistrar {
    /// Register custom metrics to the global prometheus registry
    fn register_metrics(&self) -> Result<(), prometheus::Error>;
}

lazy_static! {
    // System health metrics
    /// Number of active connections
    pub static ref SYSTEM_ACTIVE_CONNECTIONS: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "sigbot_system_active_connections",
            "Number of active connections"
        ),
        &["component"] // component: datafeed, exchange, messaging, etc.
    ).expect("system_active_connections metric can be created");

    /// Number of messages in queue backlog
    pub static ref SYSTEM_QUEUE_BACKLOG: GaugeVec = GaugeVec::new(
        prometheus::Opts::new(
            "sigbot_system_queue_backlog",
            "Number of messages in queue backlog"
        ),
        &["queue_name"]
    ).expect("system_queue_backlog metric can be created");

    // Data stream related metrics
    /// Total number of market data received
    pub static ref DATAFEED_MESSAGES_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "sigbot_datafeed_messages_total",
            "Total number of market data messages received"
        ),
        &["source", "symbol", "type"] // source: binance, kafka, etc.; type: kline, tick, etc.
    ).expect("datafeed_messages_total metric can be created");

    /// Data processing delay (seconds)
    pub static ref DATAFEED_PROCESSING_DURATION: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new(
            "sigbot_datafeed_processing_duration_seconds",
            "Data processing duration in seconds"
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["source", "type"]
    ).expect("datafeed_processing_duration metric can be created");

    // Strategy execution related metrics
    /// Total number of strategy executions
    pub static ref STRATEGY_EXECUTIONS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "sigbot_strategy_executions_total",
            "Total number of strategy executions"
        ),
        &["strategy_id", "status"] // status: success, error
    ).expect("strategy_executions_total metric can be created");

    /// Strategy execution duration (seconds)
    pub static ref STRATEGY_EXECUTION_DURATION: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new(
            "sigbot_strategy_execution_duration_seconds",
            "Strategy execution duration in seconds"
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        &["strategy_id", "mode"] // mode: streaming, batch
    ).expect("strategy_execution_duration metric can be created");

    /// Number of active strategies
    pub static ref STRATEGY_ACTIVE_COUNT: Gauge = Gauge::new(
        "sigbot_strategy_active_count",
        "Number of active strategies"
    ).expect("strategy_active_count metric can be created");

    // Trading related metrics
    /// Total number of orders
    pub static ref TRADING_ORDERS_TOTAL: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "sigbot_trading_orders_total",
            "Total number of trading orders"
        ),
        &["strategy_id", "side", "status"] // side: buy, sell; status: filled, rejected, cancelled
    ).expect("trading_orders_total metric can be created");

    /// Trading volume (by symbol)
    pub static ref TRADING_VOLUME: CounterVec = CounterVec::new(
        prometheus::Opts::new(
            "sigbot_trading_volume",
            "Total trading volume"
        ),
        &["strategy_id", "symbol", "side"]
    ).expect("trading_volume metric can be created");
}

/// -------------------------
/// Example of using metrics
/// -------------------------
/// ```rust,no_run
/// use sigbot_core::mgmt::apm::metrics::*;
///
/// // Record strategy execution
/// STRATEGY_EXECUTIONS_TOTAL
///     .with_label_values(&["strategy_123", "success"])
///     .inc();
///
/// // Record execution duration
/// let timer = STRATEGY_EXECUTION_DURATION
///     .with_label_values(&["strategy_123", "streaming"])
///     .start_timer();
/// // ... execute strategy code ...
/// drop(timer); // automatically record duration
/// ```
///
fn register_default_metrics() -> Result<(), prometheus::Error> {
    prometheus::register(Box::new(STRATEGY_EXECUTIONS_TOTAL.clone()))?;
    prometheus::register(Box::new(STRATEGY_EXECUTION_DURATION.clone()))?;
    prometheus::register(Box::new(STRATEGY_ACTIVE_COUNT.clone()))?;
    prometheus::register(Box::new(DATAFEED_MESSAGES_TOTAL.clone()))?;
    prometheus::register(Box::new(DATAFEED_PROCESSING_DURATION.clone()))?;
    prometheus::register(Box::new(TRADING_ORDERS_TOTAL.clone()))?;
    prometheus::register(Box::new(TRADING_VOLUME.clone()))?;
    prometheus::register(Box::new(SYSTEM_ACTIVE_CONNECTIONS.clone()))?;
    prometheus::register(Box::new(SYSTEM_QUEUE_BACKLOG.clone()))?;

    Ok(())
}

static REGISTRARS: once_cell::sync::Lazy<Mutex<Vec<Box<dyn MetricRegistrar + Send + Sync>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(Vec::new()));

pub async fn init_metrics(config: &Arc<AppConfig>) {
    if config.mgmt.enabled {
        info!("Registering metrics to Prometheus registry ...");

        if let Err(e) = register_default_metrics() {
            error!(e; "Failed to register default metrics");
        } else {
            info!("Default metrics registered successfully");
        }

        // Call all custom registrars
        if let Ok(registrars) = REGISTRARS.lock() {
            for registrar in registrars.iter() {
                if let Err(e) = registrar.register_metrics() {
                    error!(e; "Failed to register custom metrics");
                }
            }
        }

        info!("All metrics registered successfully");
    }
}

/// Register custom metric registrar
/// Other modules can call this function to register their own metrics
///
/// # Example
///
/// ```rust,no_run
/// use sigbot_core::mgmt::apm::metrics::{MetricRegistrar, register_custom_registrar};
/// use prometheus::Counter;
/// use lazy_static::lazy_static;
///
/// lazy_static! {
///     static ref MY_METRIC: Counter = Counter::new("my_metric", "My metric").unwrap();
/// }
///
/// struct MyRegistrar;
/// impl MetricRegistrar for MyRegistrar {
///     fn register_metrics(&self) -> Result<(), prometheus::Error> {
///         prometheus::register(Box::new(MY_METRIC.clone()))?;
///         Ok(())
///     }
/// }
///
/// // Register during application startup
/// register_custom_registrar(Box::new(MyRegistrar));
/// ```
pub fn register_custom_registrar(registrar: Box<dyn MetricRegistrar + Send + Sync>) {
    if let Ok(mut registrars) = REGISTRARS.lock() {
        registrars.push(registrar);
    }
}
