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

use crate::manager::wallet_factory::ISigbotWalletManager;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{error, info, warn};
use sigbot_core::modules::wallet::store::transaction::IWalletUpdater;
use sigbot_types::modules::order::events::SigbotTradeEvent;
use sigbot_types::modules::wallet::SigbotWalletManagerArgument;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct SigbotDefaultWalletManager {
    trade_handler: Arc<dyn IWalletUpdater>,
}

impl SigbotDefaultWalletManager {
    pub const NAME: &'static str = "DEFAULT";

    pub async fn new(trade_handler: Arc<dyn IWalletUpdater>) -> Arc<Self> {
        Arc::new(Self { trade_handler })
    }

    async fn process_trade_event(&self, event: &SigbotTradeEvent) -> Result<(), Error> {
        // 直接写入交易记录，依赖数据库的 ON CONFLICT 保证幂等性
        // ledger/balance/position 表都已经设置了 ON CONFLICT，可以安全地处理重复消息
        self.trade_handler.upsert(event).await?;

        info!(
            "Trade event processed successfully: wallet_id={}, order_id={}, trade_id={}",
            event.wallet_id, event.order_id, event.trade_id
        );

        Ok(())
    }

    async fn handle_trade_event_internal(&self, event: SigbotTradeEvent) -> Result<(), Error> {
        let mut retries = 3;
        let mut delay = Duration::from_secs(1);

        loop {
            match self.process_trade_event(&event).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    retries -= 1;
                    if retries == 0 {
                        error!("Failed to process trade event after retries: {}", e);
                        return Err(e);
                    }
                    warn!("Trade event processing failed, retrying... Error: {}", e);
                    sleep(delay).await;
                    delay *= 2; // Exponential backoff.
                }
            }
        }
    }
}

#[async_trait]
impl ISigbotWalletManager for SigbotDefaultWalletManager {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn init(&self, _argument: Arc<SigbotWalletManagerArgument>) {
        info!("Initializing Wallet manager");
        info!("Wallet manager initialized");
    }

    async fn close(&self) {
        info!("Shutting down Wallet manager");
    }

    async fn subscribe(
        &self,
        _handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    ) {
        info!("Subscribed to trades orders");
        // 实际的订阅逻辑在 wallet_server.rs 中处理
    }

    async fn handle_trade_event(
        &self,
        event: sigbot_types::modules::order::events::SigbotTradeEvent,
    ) -> Result<(), Error> {
        self.handle_trade_event_internal(event).await
    }
}

#[cfg(test)]
mod tests {}
