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

use crate::manager::wallet_factory::SigbotWalletManagerFactory;
use anyhow::Error;
use common_telemetry::{error, info};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use sigbot_types::modules::{
    messager::TOPIC_TRADING_RESULTS, order::events::SigbotTradeEvent, wallet::wallet::WalletProvider,
};
use std::sync::Arc;

pub struct SigbotWalletServer {}

impl SigbotWalletServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        info!("Initializing Wallet manager.");
        let (walletmgr, argument) = SigbotWalletManagerFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Wallet server.");
        info!("Initialized Wallet manager. {:?}", walletmgr.provider());

        info!("Initializing Messager client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {:?}", messager.provider());

        // Subscribe to trade event topics.
        let manager0 = walletmgr.to_owned();
        let messager0 = messager.to_owned();

        // Subscribe to all tenant's trade topics (using wildcard).
        let topic = TOPIC_TRADING_RESULTS.replace("{tenant_id}", "+"); // MQTT single level wildcard

        let handler: Arc<
            dyn Fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, Error>> + Send>>
                + Send
                + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let manager1 = manager0.to_owned();
            Box::pin(async move {
                let event: SigbotTradeEvent = match serde_json::from_slice(&data) {
                    Ok(e) => e,
                    Err(e) => {
                        error!("Failed to deserialize SigbotTradeEvent: {}", e);
                        return Err(Error::msg(format!("Failed to deserialize SigbotTradeEvent: {}", e)));
                    }
                };

                info!(
                    "Received SigbotTradeEvent: trade_id={}, order_id={}, tenant_id={}",
                    event.trade_id, event.order_id, event.tenant_id
                );

                let manager2 = manager1.to_owned();
                tokio::spawn(async move {
                    if let Err(e) = manager2.handle_trade_event(event).await {
                        error!("Failed to process trade event: {}", e);
                    }
                });

                Ok(String::from_utf8_lossy(&data).to_string())
            })
        });

        messager0
            .subscribe(&topic, handler)
            .await
            .expect("Failed to subscribe to trade topic");

        info!("Subscribed to Wallet manager.");
    }

    pub async fn shutdown() {
        info!("Shutting down Wallet manager.");
        SigbotWalletManagerFactory::close().await;
        info!("Shutdown Wallet manager.");

        info!("Shutting down Messager client.");
        SigbotMessagerClientFactory::close().await;
        info!("Shutdown Messager client.");
    }
}

#[cfg(test)]
mod tests {}
