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
use crate::client::stock::exchange_ibkr::SigbotIBKRClient;
use anyhow::{Context, Error};
use async_trait::async_trait;
use lazy_static::lazy_static;
use sigbot_types::modules::exchange::exchange::{ExchangeInfo, ExchangeProvider};
use sigbot_types::modules::exchange::models::trade_market::{OrderInfo, SymbolInfo};
use sigbot_types::modules::exchange::models::trade_position::{ExitTradePosition, PlaceTradeSignal, TradeResult};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotExchangeClient: Send + Sync {
    fn provider(&self) -> ExchangeProvider;
    async fn init(&self);
    async fn close(&self);
    async fn enter_position(&self, signal: PlaceTradeSignal) -> Result<TradeResult, Error>;
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
    /// Search for symbols/stocks by query string
    /// Returns a list of matching symbols with their contract IDs and metadata
    async fn search_symbols(&self, query: &str, sec_type: Option<&str>) -> Result<Vec<SymbolInfo>, Error>;
    /// Get current order list from the exchange
    /// Used for syncing historical orders to wallet manager's orders table
    async fn get_orders(&self, account_id: Option<&str>, filters: Option<&str>) -> Result<Vec<OrderInfo>, Error>;
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

    pub async fn init(exchange: Arc<ExchangeInfo>) -> Result<Arc<dyn ISigbotExchangeClient + Send + Sync>, Error> {
        let provider = exchange.provider.clone().context("Exchange provider is required")?;
        let properties = exchange.properties.as_ref().context("exchange scope is required")?;
        let env = properties.get("env").context("exchange env is required")?;
        let tenant_id = properties.get("tenant_id").context("exchange tenant_id is required")?;
        let account_id = properties
            .get("account_id")
            .context("exchange account_id is required")?;
        let key = Self::scope_key(env, tenant_id, account_id, provider.as_str())?;
        if let Some(client) = Self::get()
            .read()
            .map_err(|_| Error::msg("exchange registry poisoned"))?
            .implementations
            .get(&key)
            .cloned()
        {
            return Ok(client);
        }
        let client: Arc<dyn ISigbotExchangeClient + Send + Sync> = match provider {
            ExchangeProvider::BINANCE => SigbotBinanceClient::new(exchange).await,
            ExchangeProvider::IBKR => SigbotIBKRClient::new(exchange).await,
            _ => {
                return Err(Error::msg(
                    "exchange provider is not configured with a supported SDK adapter",
                ))
            }
        };
        client.init().await;
        let registered = {
            let mut registry = Self::get()
                .write()
                .map_err(|_| Error::msg("exchange registry poisoned"))?;
            registry
                .implementations
                .entry(key)
                .or_insert_with(|| client.clone())
                .clone()
        };
        if !Arc::ptr_eq(&client, &registered) {
            client.close().await;
        }
        Ok(registered)
    }

    pub fn scope_key(env: &str, tenant_id: &str, account_id: &str, provider: &str) -> Result<String, Error> {
        for value in [env, tenant_id, account_id, provider] {
            anyhow::ensure!(
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
                "invalid exchange isolation key"
            );
        }
        Ok(format!("{env}:{tenant_id}:{account_id}:{provider}"))
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
        let clients: Vec<_> = {
            let mut registry = Self::get().write().expect("exchange registry poisoned");
            registry.implementations.drain().map(|(_, client)| client).collect()
        };
        for client in clients {
            client.close().await;
        }
    }
}

#[cfg(test)]
mod tests {}
