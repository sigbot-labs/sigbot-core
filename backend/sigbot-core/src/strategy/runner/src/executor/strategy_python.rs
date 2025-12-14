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
use sigbot_messaging::client::messaging_factory::ISigbotMessagingClient;
use sigbot_types::modules::{
    messaging::TOPIC_MARKET_DATA,
    strategy::{models::strategy_sdk::StrategyExecutionInput, SigbotStrategyArgument},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub struct SigbotPythonStrategyExecutor {
    argument: Arc<SigbotStrategyArgument>,
    #[allow(unused)]
    batch_executor: Mutex<Option<Arc<BatchStrategyExecutor>>>,
    streaming_executor: Mutex<Option<Arc<StreamingStrategyExecutor>>>,
}

impl SigbotPythonStrategyExecutor {
    pub const NAME: &'static str = "PYTHON";

    pub async fn new(argument: Arc<SigbotStrategyArgument>) -> Arc<Self> {
        Arc::new(Self {
            argument,
            batch_executor: Mutex::new(None),
            streaming_executor: Mutex::new(None),
        })
    }

    #[allow(unused_variables)]
    pub async fn execute_batch(&self, messaging: Arc<dyn ISigbotMessagingClient + Send + Sync>) {
        // Clone executor Arc before moving into closure to avoid holding MutexGuard across await
        let batch_executor = self
            .batch_executor
            .lock()
            .expect("Failed to lock batch executor.")
            .to_owned();

        unimplemented!()
    }

    #[allow(unused_variables)]
    pub async fn execute_streaming(&self, messaging: Arc<dyn ISigbotMessagingClient + Send + Sync>) {
        let streaming_executor = self
            .streaming_executor
            .lock()
            .expect("Failed to lock streaming executor.")
            .to_owned();

        let strategy_id = self
            .argument
            .strategy_config
            .base
            .id
            .as_ref()
            .map(|id| id.to_string())
            .unwrap_or_default();

        let handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync> =
            Arc::new(move |data: Vec<u8>| {
                let executor0 = streaming_executor.clone();
                let strategy_id0 = strategy_id.to_owned();
                debug!("Received message: {:?}", data);

                Box::pin(async move {
                    let data0 = data.to_owned();
                    let input: StrategyExecutionInput =
                        serde_json::from_slice(&data0).context("Failed to parse the data from the message.")?;
                    debug!("Parsed input: {:?}", input);

                    let result: Result<String, Error> = {
                        match executor0
                            .as_ref()
                            .expect("Streaming executor is not initialized.")
                            .process(&input)
                        {
                            Ok((execution_result, trade_signal)) => {
                                if execution_result.success {
                                    // Statistics to strategy execution total metrics.
                                    sigbot_core::mgmt::apm::metrics::STRATEGY_EXECUTIONS_TOTAL
                                        .with_label_values(&[strategy_id0.as_str(), "success"])
                                        .inc();

                                    // If there's a trading signal, execute the trade
                                    if let Some(signal) = trade_signal {
                                        info!("Received trading signal: {:?}", signal);
                                        // Get exchange client (assuming BINANCE for now, can be made configurable)
                                        match SigbotExchangeClientFactory::get_implementation("BINANCE".to_string())
                                            .await
                                        {
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
                                    // Statistics to strategy execution total metrics.
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

        let _ = messaging
            .subscribe(TOPIC_MARKET_DATA, handler) // TODO: configuable
            .await
            .expect("Failed to subscribe to the messaging topic.");

        info!("Subscribed to the messaging topic: {:?}.", TOPIC_MARKET_DATA);
    }
}

#[async_trait]
impl ISigbotStrategyExecutor for SigbotPythonStrategyExecutor {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn startup(&self, messaging: Arc<dyn ISigbotMessagingClient + Send + Sync>) {
        if self.argument.run_mode == "BATCH" {
            let batch_executor = Arc::new(BatchStrategyExecutor::new(self.argument.clone()));
            *self.batch_executor.lock().unwrap() = Some(batch_executor.clone());

            info!("Initializing Python Strategy Batch Executor.");
            batch_executor.init().expect("Failed to initializing batch executor.");
            info!("Initialized Python Strategy Batch Executor.");

            info!("Starting Python Batch Strategy Runner.");
            self.execute_batch(messaging.to_owned()).await;
            info!("Started Python Batch Strategy Runner.");
        } else if self.argument.run_mode == "STREAMING" {
            let streaming_executor = Arc::new(StreamingStrategyExecutor::new(self.argument.clone()));
            *self.streaming_executor.lock().unwrap() = Some(streaming_executor.clone());

            info!("Initializing Python Strategy Streaming Executor.");
            streaming_executor
                .init()
                .expect("Failed to initializing streaming executor.");
            info!("Initialized Python Strategy Streaming Executor.");

            info!("Starting Python Streaming Strategy Runner.");
            self.execute_streaming(messaging.to_owned()).await;
            info!("Started Python Streaming Strategy Runner.");
        } else {
            panic!("Unsupported run mode: {}", self.argument.run_mode);
        }
    }

    async fn shutdown(&self) {
        info!("Shutdown Embed Strategy Runner.");
        if let Some(executor) = self.batch_executor.lock().unwrap().as_ref() {
            executor.shutdown().expect("Failed to shutdown batch executor.");
        }
        if let Some(executor) = self.streaming_executor.lock().unwrap().as_ref() {
            executor.shutdown().expect("Failed to shutdown streaming executor.");
        }
        info!("Shutdown Embed Strategy Runner.");
    }
}

#[cfg(test)]
mod tests {}
