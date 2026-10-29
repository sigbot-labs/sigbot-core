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

use super::wallet_runtime::WalletRuntime;
use crate::manager::wallet_factory::SigbotWalletManagerFactory;
use anyhow::Error;
use common_telemetry::{debug, error, info};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use sigbot_types::modules::{messager::TOPIC_WF_TRADING_PLACED, order::events::SigbotTradeEvent};
use std::sync::{Arc, OnceLock};

static V2_RUNTIME: OnceLock<tokio::sync::Mutex<Option<Arc<WalletRuntime>>>> = OnceLock::new();

pub struct SigbotWalletServer {}

impl SigbotWalletServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        debug!("Initializing Wallet manager.");
        let (walletmgr, argument) = SigbotWalletManagerFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Wallet server.");
        info!("Initialized Wallet manager. {:?}", walletmgr.provider());

        debug!("Initializing Messager client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {:?}", messager.provider());

        if argument
            .properties
            .as_ref()
            .and_then(|properties| properties.get("runtime_v2"))
            .is_some_and(|value| value == "true")
        {
            let runtime = WalletRuntime::start(&argument, messager)
                .await
                .expect("Failed to start scoped wallet runtime");
            *V2_RUNTIME.get_or_init(Default::default).lock().await = Some(runtime);
            return;
        }

        // Subscribe to trade event topics.
        let manager0 = walletmgr.to_owned();
        let messager0 = messager.to_owned();

        // Subscribe to all tenant's trade topics (using wildcard).
        let topic = TOPIC_WF_TRADING_PLACED
            .replace("{TENANT_ID}", "+")
            .replace("{WORKFLOW_ID}", "+");

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

                // Returning success is the transport's durable ACK boundary.
                manager1.handle_trade_event(event).await?;

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
        if let Some(runtime) = V2_RUNTIME.get_or_init(Default::default).lock().await.take() {
            runtime.close();
        }
        info!("Shutting down Wallet manager.");
        SigbotWalletManagerFactory::shutdown().await;
        info!("Shutdown Wallet manager.");

        info!("Shutting down Messager client.");
        SigbotMessagerClientFactory::shutdown().await;
        info!("Shutdown Messager client.");
    }
}

#[cfg(test)]
mod tests {}
