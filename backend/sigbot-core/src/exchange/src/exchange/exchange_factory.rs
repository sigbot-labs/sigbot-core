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

use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_types::modules::exchange::models::trade_market::{KlineResult, PriceResult};
use sigbot_types::modules::exchange::models::trade_signal::{EntryTradeSignal, ExitTradePosition, TradeResult};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotExchangeOperation: Send + Sync {
    async fn init(&self);
    async fn close(&self);
    async fn get_current_price(&self, symbol: &str) -> Result<PriceResult, Error>;
    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Result<Vec<KlineResult>, Error>;
    async fn entry_position(&self, signal: EntryTradeSignal) -> Result<TradeResult, Error>;
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
    static ref SINGLE_INSTANCE: RwLock<SigbotExchangeFactory> = RwLock::new(SigbotExchangeFactory::new());
}

pub struct SigbotExchangeFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotExchangeOperation + Send + Sync>>,
}

impl SigbotExchangeFactory {
    fn new() -> Self {
        SigbotExchangeFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotExchangeFactory> {
        &SINGLE_INSTANCE
    }

    pub async fn init() {
        info!("Register to All Sigbot exchange operators ...");

        unimplemented!()
    }

    fn register<T: ISigbotExchangeOperation + Send + Sync + 'static>(
        &mut self,
        name: String,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(&name) {
            debug!("Already register the sigbot operator '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name, handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotExchangeOperation + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotExchangeFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered sigbot exchange manager '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotExchangeFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}

#[cfg(test)]
mod tests {}
