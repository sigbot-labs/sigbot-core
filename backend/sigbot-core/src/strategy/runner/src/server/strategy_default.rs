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
    embed::{batch_executor::BatchStrategyExecutor, streaming_executor::StreamingStrategyExecutor},
    strategy_factory::ISigbotStrategyRunner,
};
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info, warn};
use sigbot_exchange::client::exchange_factory::SigbotExchangeClientFactory;
use sigbot_messaging::client::messaging_factory::SigbotMessagingClientFactory;
use sigbot_types::modules::{
    messaging::TOPIC_MARKET_DATA,
    strategy::{models::strategy_embed::StrategyExecutionInput, SigbotStrategyArgument},
};
use std::{future::Future, pin::Pin, sync::Arc};

#[derive(Clone)]
pub struct SigbotDefaultStrategyRunner {
    #[allow(unused)]
    batch_executor: Arc<BatchStrategyExecutor>,
    streaming_executor: Arc<StreamingStrategyExecutor>,
}

impl SigbotDefaultStrategyRunner {
    pub const NAME: &'static str = "DEFAULT";

    pub async fn new(strategy_argument: Arc<SigbotStrategyArgument>) -> Arc<Self> {
        let batch_executor = Arc::new(BatchStrategyExecutor::new(strategy_argument.clone()));
        let streaming_executor = Arc::new(StreamingStrategyExecutor::new(strategy_argument.clone()));

        Arc::new(Self {
            batch_executor,
            streaming_executor,
        })
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

        // Subscribe market data from messaging with topics.
        // let batch_executor = self.batch_executor.clone();
        let streaming_executor = self.streaming_executor.clone();

        let handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync> =
            Arc::new(move |data: Vec<u8>| {
                debug!("Received message: {:?}", data);
                // let batch_executor0 = batch_executor.clone();
                let streaming_executor0 = streaming_executor.clone();

                Box::pin(async move {
                    let data0 = data.to_owned();
                    let input: StrategyExecutionInput =
                        serde_json::from_slice(&data0).context("Failed to parse the data from the message.")?;
                    debug!("Parsed input: {:?}", input);

                    // Determine execution mode from input.run_mode (default to STREAMING)
                    let mode = if input.run_mode.is_empty() {
                        "STREAMING"
                    } else {
                        input.run_mode.as_str()
                    };

                    let result: Result<String, Error> = match mode {
                        "BATCH" => {
                            // Batch mode: process all market data entries
                            unimplemented!("Unsupported the strategy batch runtime.")
                        }
                        // Streaming mode
                        _ => {
                            // Pass the entire StrategyExecutionInput object directly
                            let result = streaming_executor0.process(&input);
                            match result {
                                Ok((execution_result, trade_signal)) => {
                                    if execution_result.success {
                                        // If there's a trading signal, execute the trade
                                        if let Some(signal) = trade_signal {
                                            info!("Received trading signal: {:?}", signal);
                                            // Get exchange client (assuming BINANCE for now, can be made configurable)
                                            match SigbotExchangeClientFactory::get_implementation("BINANCE".to_string())
                                                .await
                                            {
                                                Ok(exchange_client) => {
                                                    match exchange_client.entry_position(signal).await {
                                                        Ok(trade_result) => {
                                                            info!(
                                                                "Trade executed successfully: order_id={}, success={}",
                                                                trade_result.order_id, trade_result.success
                                                            );
                                                            Ok(format!(
                                                                "Trade executed: order_id={}",
                                                                trade_result.order_id
                                                            ))
                                                        }
                                                        Err(e) => {
                                                            warn!("Failed to execute trade: {}", e);
                                                            Err(anyhow::anyhow!("Failed to execute trade: {}", e))
                                                        }
                                                    }
                                                }
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
        self.batch_executor
            .init()
            .expect("Failed to initializing batch executor.");

        self.streaming_executor
            .init()
            .expect("Failed to initializing streaming executor.");
    }

    async fn shutdown(&self) {
        info!("Shutting down Embed Strategy Runner.");
    }
}

#[cfg(test)]
mod tests {}
