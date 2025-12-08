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
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::client::{
    market::datafeed_binance::SigbotBinanceDatafeedClient,
    news::{datafeed_trushsocial::SigbotTrushSocialDatafeedClient, datafeed_twitter::SigbotTwitterDatafeedClient},
};

#[async_trait]
pub trait ISigbotDatafeedClient: Send + Sync {
    fn name(&self) -> &'static str;
    async fn init(&self);
    async fn close(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotDatafeedClientFactory> = RwLock::new(SigbotDatafeedClientFactory::new());
}

pub struct SigbotDatafeedClientFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotDatafeedClient + Send + Sync>>,
}

impl SigbotDatafeedClientFactory {
    fn new() -> Self {
        SigbotDatafeedClientFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotDatafeedClientFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<Arc<Vec<Arc<dyn ISigbotDatafeedClient + Send + Sync>>>, Error> {
        // e.g '--datafeed=binance'
        let datafeed_provider = matches
            .try_get_one::<String>("datafeed")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| SigbotBinanceDatafeedClient::NAME.to_owned())
            })
            .expect("Failed to parse the datafeed provider from the command line arguments.");

        info!("Registering Sigbot Datafeed: {}", &datafeed_provider);

        let mut datafeeds: Vec<Arc<dyn ISigbotDatafeedClient + Send + Sync>> = Vec::new();
        for provider in datafeed_provider.split(',').collect::<Vec<&str>>() {
            match provider.to_uppercase().as_str() {
                SigbotBinanceDatafeedClient::NAME => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            &SigbotBinanceDatafeedClient::NAME.to_owned(),
                            SigbotBinanceDatafeedClient::new().await, // TODO: set up run configuration?
                        )
                        .expect("Failed to register the Binance datafeed.");
                }
                SigbotTwitterDatafeedClient::NAME => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            &SigbotTwitterDatafeedClient::NAME.to_owned(),
                            SigbotTwitterDatafeedClient::new().await, // TODO: set up run configuration?
                        )
                        .expect("Failed to register the Twitter datafeed.");
                }
                SigbotTrushSocialDatafeedClient::NAME => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            &SigbotTrushSocialDatafeedClient::NAME.to_owned(),
                            SigbotTrushSocialDatafeedClient::new().await, // TODO: set up run configuration?
                        )
                        .expect("Failed to register the Trush Social datafeed.");
                }
                _ => panic!("Unsupported sigbot datafeed provider : '{}'.", provider),
            };

            let registered = Self::get_implementation(provider.to_owned())
                .await
                .expect("Failed to get the registered datafeed.");

            info!("Initializing the datafeed with name: {}", &provider);
            registered.init().await;
            datafeeds.push(registered);
            info!("Initialized the datafeed with name: {}.", &provider);
        }

        Ok(Arc::new(datafeeds))
    }

    fn register0(
        &mut self,
        name: &String,
        handler: Arc<dyn ISigbotDatafeedClient + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotDatafeedClient + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the Datafeed '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.clone());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotDatafeedClient + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotDatafeedClientFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Datafeed '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotDatafeedClientFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}
