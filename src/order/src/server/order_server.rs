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

use crate::manager::order_factory::SigbotOrderManagerFactory;
use anyhow::Error;
use common_telemetry::{debug, error, info};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use sigbot_types::modules::{messager::TOPIC_TRADING_SIGNALS, order::events::SigbotTradeSignal};
use std::sync::Arc;

pub struct SigbotOrderServer {}

impl SigbotOrderServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        debug!("Initializing Order manager.");
        let (manager, argument) = SigbotOrderManagerFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Order manager.");
        info!("Initialized Order manager. {}", manager.provider().as_str());

        debug!("Initializing Messager client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {}", messager.provider().as_str());

        // Subscribe to trading signal topics.
        let manager0 = manager.to_owned();
        let messager0 = messager.to_owned();

        // Subscribe to all tenant's signal topics (using wildcard).
        // Should subscribe to specific tenant's topics based on configuration.
        let topic = TOPIC_TRADING_SIGNALS.replace("{tenant_id}", "+"); // +: MQTT single-level wildcard

        let handler: Arc<
            dyn Fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, Error>> + Send>>
                + Send
                + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let manager1 = manager0.clone();
            Box::pin(async move {
                let signal: SigbotTradeSignal = match serde_json::from_slice(&data) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to deserialize SigbotTradeSignal: {}", e);
                        return Err(Error::msg(format!("Failed to deserialize SigbotTradeSignal: {}", e)));
                    }
                };

                info!(
                    "Received SigbotTradeSignal: signal_id={}, tenant_id={}",
                    signal.signal_id, signal.tenant_id
                );

                let manager2 = manager1.to_owned();
                tokio::spawn(async move {
                    if let Err(e) = manager2.process_signal(signal).await {
                        error!("Failed to process signal: {}", e);
                    }
                });

                Ok(String::from_utf8_lossy(&data).to_string())
            })
        });

        messager0
            .subscribe(&topic, handler)
            .await
            .expect("Failed to subscribe to signal topic");

        info!("Subscribed to Order manager.");
    }

    pub async fn shutdown() {
        info!("Shutting down Order manager.");
        SigbotOrderManagerFactory::close().await;
        info!("Shutdown Order manager.");

        info!("Shutting down Messager client.");
        SigbotMessagerClientFactory::close().await;
        info!("Shutdown Messager client.");
    }
}

#[cfg(test)]
mod tests {}
