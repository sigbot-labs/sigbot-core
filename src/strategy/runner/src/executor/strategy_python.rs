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

use crate::executor::{
    pyexec::{batch_executor::BatchStrategyExecutor, streaming_executor::StreamingStrategyExecutor},
    strategy_factory::ISigbotStrategyExecutor,
};
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info, warn};
use sigbot_exchange::client::exchange_factory::SigbotExchangeClientFactory;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::{
    messager::{TOPIC_CONFIG_STRATEGY, TOPIC_MARKET_STREAMS},
    strategy::{
        models::strategy_execution::StrategyExecutionInput,
        strategy::{StrategyInfo, StrategyProvider},
        SigbotStrategyArgument,
    },
};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub struct SigbotPythonStrategyExecutor {
    argument: Arc<SigbotStrategyArgument>,
    #[allow(unused)]
    running_batch_executors: Arc<Mutex<HashMap<String, Arc<BatchStrategyExecutor>>>>,
    running_streaming_executors: Arc<Mutex<HashMap<String, Arc<StreamingStrategyExecutor>>>>,
}

impl SigbotPythonStrategyExecutor {
    pub async fn new(argument: Arc<SigbotStrategyArgument>) -> Arc<Self> {
        Arc::new(Self {
            argument,
            running_batch_executors: Arc::new(Mutex::new(HashMap::new())),
            running_streaming_executors: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    #[allow(unused_variables)]
    pub async fn execute_batch(&self, messager: Arc<dyn ISigbotMessagerClient + Send + Sync>) {
        unimplemented!()
    }

    #[allow(unused_variables)]
    pub async fn execute_streaming(
        &self,
        messager: Arc<dyn ISigbotMessagerClient + Send + Sync>,
        executor: Arc<StreamingStrategyExecutor>,
    ) {
        let strategy = executor.to_owned().configuration.to_owned();

        let strategy_id = strategy
            .to_owned()
            .base
            .id
            .expect("Failed to get the strategy id.")
            .to_string();

        let market_data_handler: Arc<
            dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let executor0 = executor.to_owned();
            let strategy_id0 = strategy_id.to_owned();
            debug!("Received message: {:?}", data);

            Box::pin(async move {
                let data0 = data.to_owned();
                let input: StrategyExecutionInput =
                    serde_json::from_slice(&data0).context("Failed to parse the data from the message.")?;
                debug!("Parsed input: {:?}", input);

                // Metrics: start timer for strategy execution duration
                let timer = sigbot_core::mgmt::apm::metrics::STRATEGY_EXECUTION_DURATION
                    .with_label_values(&[strategy_id0.as_str(), "streaming"])
                    .start_timer();

                let result: Result<String, Error> = {
                    match executor0.process(&input) {
                        Ok((execution_result, trade_signal)) => {
                            if execution_result.success {
                                // Metrics: strategy executions total.
                                sigbot_core::mgmt::apm::metrics::STRATEGY_EXECUTIONS_TOTAL
                                    .with_label_values(&[strategy_id0.as_str(), "success"])
                                    .inc();

                                // If there's a trading signal, execute the trade
                                if let Some(signal) = trade_signal {
                                    info!("Received trading signal: {:?}", signal);
                                    // Get exchange client (assuming BINANCE for now, can be made configurable)
                                    match SigbotExchangeClientFactory::get_implementation("BINANCE").await {
                                        Ok(exchange_client) => match exchange_client.entry_position(signal).await {
                                            Ok(trade_result) => {
                                                info!(
                                                    "Trade executed successfully: order_id={}, success={}",
                                                    trade_result.order_id, trade_result.success
                                                );
                                                Ok(format!("Trade executed: order_id={}", trade_result.order_id))
                                            }
                                            Err(e) => {
                                                warn!("Failed to execute trade: {}", e);
                                                Err(anyhow::anyhow!("Failed to execute trade: {}", e))
                                            }
                                        },
                                        Err(e) => {
                                            warn!("Failed to get exchange client: {}", e);
                                            Err(anyhow::anyhow!("Failed to get exchange client: {}", e))
                                        }
                                    }
                                } else {
                                    debug!("No trading signal generated");
                                    Ok("No signal".to_string())
                                }
                            } else {
                                // Statistics to executions total metrics.
                                sigbot_core::mgmt::apm::metrics::STRATEGY_EXECUTIONS_TOTAL
                                    .with_label_values(&[strategy_id0.as_str(), "error"])
                                    .inc();

                                let error_msg = execution_result
                                    .error
                                    .clone()
                                    .unwrap_or_else(|| "Unknown error".to_string());
                                warn!("Strategy execution failed: {}", error_msg);
                                Err(anyhow::anyhow!("Strategy execution failed: {}", error_msg))
                            }
                        }
                        Err(e) => {
                            warn!("Failed to execute strategy: {}", e);
                            Err(anyhow::anyhow!("Failed to execute strategy: {}", e))
                        }
                    }
                };

                // Metrics: stop timer (automatically records duration when dropped)
                drop(timer);

                if let Err(ref e) = result {
                    warn!("Failed to execute strategy: {}", e);
                    // TODO: statistics the error metrics.
                } else {
                    debug!("Executed strategy successfully. Result: {:?}", result);
                    // TODO: statistics the success metrics.
                }
                result
            })
        });

        let topic = TOPIC_MARKET_STREAMS.replace("{tenant_id}", "+"); // MQTT single level wildcard
        let _ = messager
            .subscribe(&topic, market_data_handler) // TODO: configuable
            .await
            .expect("Failed to subscribe to market data topic.");
        info!("Subscribed to market data topic: {:?}.", TOPIC_MARKET_STREAMS);
    }
}

#[async_trait]
impl ISigbotStrategyExecutor for SigbotPythonStrategyExecutor {
    fn provider(&self) -> StrategyProvider {
        StrategyProvider::PYTHON
    }

    async fn startup(&self, messager: Arc<dyn ISigbotMessagerClient + Send + Sync>) {
        if self.argument.run_mode == "BATCH" {
            unimplemented!()
        } else if self.argument.run_mode == "STREAMING" {
            let messager0 = messager.to_owned();
            let executors0 = self.running_streaming_executors.to_owned();
            // SAFETY: We know that `self` is actually an Arc<Self> because `startup` is called
            // through `Arc<dyn ISigbotStrategyExecutor>` (see strategy_runner.rs:47). We create
            // a new Arc from the raw pointer and immediately clone it, then forget the original
            // to avoid double-dropping. This is safe because:
            // 1. The original Arc is managed by the caller and will outlive this method
            // 2. We're only creating a new Arc handle, not taking ownership
            // 3. The cloned Arc will be used in the closure which requires 'static lifetime
            let self_arc = unsafe {
                let ptr = self as *const Self;
                Arc::from_raw(ptr)
            };
            let self_arc0 = self_arc.to_owned();
            std::mem::forget(self_arc); // Don't drop the original, it's managed elsewhere

            // Subscribe to dynamic strategies updated messages.
            let dynamic_strategy_update_handler: Arc<
                dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync,
            > = Arc::new(move |data: Vec<u8>| {
                debug!("Received strategy updated message: {:?}", data);

                let messager1 = messager0.to_owned();
                let executors1 = executors0.to_owned();
                let self_arc1 = self_arc0.to_owned();

                Box::pin(async move {
                    let data0 = data.to_owned();

                    // parse to strategy info from subscribe config message.
                    let strategy: Arc<StrategyInfo> = Arc::new(
                        serde_json::from_slice(&data0)
                            .context("Failed to parse the strategy info from the config message.")?,
                    );

                    let strategy_id = strategy
                        .to_owned()
                        .base
                        .id
                        .expect("Failed to get the strategy id.")
                        .to_string();

                    // Check if executor exists and handle shutdown if needed
                    let should_create = {
                        let mut executors = executors1.lock().unwrap();
                        if let Some(executor) = executors.get(&strategy_id) {
                            executor
                                .to_owned()
                                .shutdown()
                                .expect("Failed to shutdown streaming executor.");
                            executors.remove(&strategy_id);

                            // Metrics: strategy inactive count
                            sigbot_core::mgmt::apm::metrics::STRATEGY_ACTIVE_COUNT
                                .with_label_values(&[strategy_id.as_str(), "inactive"])
                                .inc();

                            false // Found existing, don't create new
                        } else {
                            true // Not found, create new
                        }
                    };

                    if should_create {
                        info!("Initializing Python Strategy Streaming Executor.");
                        let executor = Arc::new(StreamingStrategyExecutor::new(
                            self_arc1.argument.to_owned(),
                            strategy.to_owned(),
                        ));
                        executor.init().expect("Failed to initializing streaming executor.");
                        info!("Initialized Python Strategy Streaming Executor.");

                        {
                            let mut executors = executors1.lock().unwrap();
                            executors.insert(strategy_id.to_owned(), executor.to_owned());
                        }

                        info!("Starting Python Streaming Strategy Runner.");
                        self_arc1
                            .execute_streaming(messager1.to_owned(), executor.to_owned())
                            .await;
                        info!("Started Python Streaming Strategy Runner.");

                        // Metrics: strategy active count.
                        sigbot_core::mgmt::apm::metrics::STRATEGY_ACTIVE_COUNT
                            .with_label_values(&[strategy_id.as_str(), "active"])
                            .inc();
                    }

                    Ok(format!("Dynamic strategy updated: {}", strategy_id))
                })
            });

            let _ = messager
                .to_owned()
                .subscribe(TOPIC_CONFIG_STRATEGY, dynamic_strategy_update_handler.to_owned())
                .await
                .expect("Failed to subscribe to strategy config topic.");
            info!("Subscribed to strategy config topic: {:?}.", TOPIC_CONFIG_STRATEGY);
        } else {
            panic!("Unsupported run mode: {}", self.argument.run_mode);
        }
    }

    async fn shutdown(&self) {
        info!("Shutdown Embed Strategy Runner.");
        for (_, executor) in self.running_batch_executors.lock().unwrap().iter() {
            executor.shutdown().expect("Failed to shutdown batch executor.");
        }
        for (strategy_id, executor) in self.running_streaming_executors.lock().unwrap().iter() {
            executor
                .to_owned()
                .shutdown()
                .expect("Failed to shutdown streaming executor.");

            // Metrics: strategy inactive count
            sigbot_core::mgmt::apm::metrics::STRATEGY_ACTIVE_COUNT
                .with_label_values(&[strategy_id.as_str(), "inactive"])
                .inc();
        }
        info!("Shutdown Embed Strategy Runner.");
    }
}

#[cfg(test)]
mod tests {}
