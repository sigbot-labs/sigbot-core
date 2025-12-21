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

use crate::client::cex::exchange_binance::SigbotBinanceClient;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_types::modules::exchange::exchange::{ExchangeInfo, ExchangeProvider};
use sigbot_types::modules::exchange::models::trade_market::{KlineModel, PriceModel};
use sigbot_types::modules::exchange::models::trade_position::{EntryTradePosition, ExitTradePosition, TradeResult};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotExchangeClient: Send + Sync {
    fn provider(&self) -> ExchangeProvider;
    async fn init(&self);
    async fn close(&self);
    async fn get_current_price(&self, symbol: &str) -> Result<PriceModel, Error>;
    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Result<Vec<KlineModel>, Error>;
    async fn entry_position(&self, signal: EntryTradePosition) -> Result<TradeResult, Error>;
    async fn exit_loss_position(
        &self,
        original_order_id: u64,
        position: &ExitTradePosition,
    ) -> Result<TradeResult, Error>;
    async fn exit_profit_position(
        &self,
        original_order_id: u64,
        position: &ExitTradePosition,
    ) -> Result<TradeResult, Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotExchangeClientFactory> = RwLock::new(SigbotExchangeClientFactory::new());
}

pub struct SigbotExchangeClientFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotExchangeClient + Send + Sync>>,
}

impl SigbotExchangeClientFactory {
    fn new() -> Self {
        SigbotExchangeClientFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotExchangeClientFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(exchange: Arc<ExchangeInfo>) -> Result<Arc<dyn ISigbotExchangeClient + Send + Sync>, Error> {
        let provider = exchange.provider.to_owned().context("Exchange provider is required")?;
        info!("Registering exchange with provider: {:?}", &provider);

        match provider {
            ExchangeProvider::BINANCE => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        ExchangeProvider::BINANCE.as_str(),
                        SigbotBinanceClient::new(exchange).await, // TODO: set up run configuration?
                    )
                    .expect(&format!("Failed to register the exchange with provider: {:?}.", &provider).as_str());
            }
            _ => panic!("Unsupported exchange provider : '{:?}'.", &provider),
        };

        let registered = Self::get_implementation(provider.as_str())
            .await
            .expect(&format!("Failed to get the registered exchange with provider: {:?}.", &provider).as_str());

        info!("Initializing the exchange with provider: {:?}", &provider);
        registered.init().await;
        info!("Initialized the exchange with provider: {:?}.", &provider);

        Ok(registered)
    }

    fn register0(
        &mut self,
        name: &str,
        handler: Arc<dyn ISigbotExchangeClient + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotExchangeClient + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the sigbot exchange client '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: &str) -> Result<Arc<dyn ISigbotExchangeClient + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotExchangeClientFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered sigbot exchange manager '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotExchangeClientFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}

#[cfg(test)]
mod tests {}
