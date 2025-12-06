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

use anyhow::{Error, Ok};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_types::modules::datafeed::datafeed::{DatafeedInfo, DatafeedProvider};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::datafeed::{
    market::datafeed_binance::SigbotBinanceDatafeedExecutor, news::datafeed_twitter::SigbotTwitterDatafeedExecutor,
};

#[async_trait]
pub trait ISigbotDatafeedExecutor: Send + Sync {
    async fn init(&self);
    async fn shutdown(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotDatafeedFactory> = RwLock::new(SigbotDatafeedFactory::new());
}

pub struct SigbotDatafeedFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotDatafeedExecutor + Send + Sync>>,
}

impl SigbotDatafeedFactory {
    fn new() -> Self {
        SigbotDatafeedFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotDatafeedFactory> {
        &SINGLE_INSTANCE
    }

    pub async fn init() {
        unimplemented!()
    }

    pub async fn register(
        datafeed: Arc<DatafeedInfo>,
    ) -> Result<Arc<dyn ISigbotDatafeedExecutor + Send + Sync>, Error> {
        info!("Register Sigbot Datafeed ...");
        let handler: Arc<dyn ISigbotDatafeedExecutor + Send + Sync> =
            match datafeed.to_owned().provider.clone().unwrap() {
                DatafeedProvider::BINANCE => SigbotBinanceDatafeedExecutor::new(datafeed.to_owned()).await,
                DatafeedProvider::TWITTER => SigbotTwitterDatafeedExecutor::new(datafeed.to_owned()).await,
            };
        let name = datafeed.name.clone().unwrap_or_default();
        let result = {
            let mut factory = SigbotDatafeedFactory::get().write().unwrap();
            factory.register0(name, handler.to_owned())
        };
        result
    }

    fn register0(
        &mut self,
        name: String,
        handler: Arc<dyn ISigbotDatafeedExecutor + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotDatafeedExecutor + Send + Sync>, Error> {
        if self.implementations.contains_key(&name) {
            debug!("Already register the Datafeed '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name, handler.clone());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotDatafeedExecutor + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotDatafeedFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Datafeed '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }
}
