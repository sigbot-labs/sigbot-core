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
use common_telemetry::{error, info};
use sigbot_messaging::client::messaging_factory::SigbotMessagingClientFactory;
use sigbot_types::modules::{messaging::TOPIC_TRADING_SIGNALS, order::events::SigbotTradeSignal};
use std::sync::Arc;

pub struct SigbotOrderServer {}

impl SigbotOrderServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        info!("Initializing Order manager.");
        let (ordermgr, argument) = SigbotOrderManagerFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Order manager.");
        info!("Initialized Order manager. {:?}", ordermgr.len());

        info!("Initializing Messaging client.");
        let messaging = SigbotMessagingClientFactory::init(matches, argument.messaging_config.to_owned())
            .await
            .expect("Failed to initialize Messaging client.");
        info!("Initialized Messaging client. {:?}", messaging.name());

        // Subscribe to trading signal topics.
        for manager in ordermgr.iter() {
            let manager0 = manager.clone();
            let messaging0 = messaging.clone();

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

                    let manager2 = manager1.clone();
                    tokio::spawn(async move {
                        if let Err(e) = manager2.process_signal(signal).await {
                            error!("Failed to process signal: {}", e);
                        }
                    });

                    Ok(String::from_utf8_lossy(&data).to_string())
                })
            });

            messaging0
                .subscribe(&topic, handler)
                .await
                .expect("Failed to subscribe to signal topic");
        }

        info!("Subscribed to Order manager.");
    }

    pub async fn shutdown() {
        info!("Shutting down Order manager.");
        SigbotOrderManagerFactory::close().await;
        info!("Shutdown Order manager.");

        info!("Shutting down Messaging client.");
        SigbotMessagingClientFactory::close().await;
        info!("Shutdown Messaging client.");
    }
}

#[cfg(test)]
mod tests {}
