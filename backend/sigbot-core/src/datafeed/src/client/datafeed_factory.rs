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

use crate::client::{
    market::datafeed_binance::SigbotBinanceDatafeedClient,
    news::{datafeed_trushsocial::SigbotTrushSocialDatafeedClient, datafeed_twitter::SigbotTwitterDatafeedClient},
};
use anyhow::{Context, Error, Ok};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_types::modules::datafeed::{datafeed::DatafeedProvider, SigbotDatefeedArgument};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotDatafeedClient: Send + Sync {
    fn provider(&self) -> DatafeedProvider;
    async fn init(&self, argument: Arc<SigbotDatefeedArgument>);
    async fn close(&self);
    async fn subscribe(
        &self,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    );
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
    ) -> Result<
        (
            Arc<Vec<Arc<dyn ISigbotDatafeedClient + Send + Sync>>>,
            Arc<SigbotDatefeedArgument>,
        ),
        Error,
    > {
        // e.g '--datafeed-provider=binance'
        let providers = matches
            .try_get_one::<String>("datafeed-provider")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| DatafeedProvider::BINANCE.as_str().to_owned())
            })
            .expect("Failed to parse the datafeed providers from the command line arguments.")
            .to_uppercase();

        info!("Registering Sigbot Datafeed: {}", &providers);

        // e.g '--datafeed-config=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("configuration")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| DatafeedProvider::BINANCE.as_str().to_owned())
            })
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            SigbotDatefeedArgument::from_json(&configuration)
                .context("Failed to parse the configuration from the command line arguments.")?,
        );

        let mut datafeeds: Vec<Arc<dyn ISigbotDatafeedClient + Send + Sync>> = Vec::new();
        for provider in providers
            .split(',')
            .map(DatafeedProvider::of)
            .collect::<Result<Vec<DatafeedProvider>, anyhow::Error>>()?
        {
            match provider {
                DatafeedProvider::BINANCE => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            provider.as_str(),
                            SigbotBinanceDatafeedClient::new().await, // TODO: set up run configuration?
                        )
                        .expect("Failed to register the Binance datafeed.");
                }
                DatafeedProvider::TWITTER => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            provider.as_str(),
                            SigbotTwitterDatafeedClient::new().await, // TODO: set up run configuration?
                        )
                        .expect("Failed to register the Twitter datafeed.");
                }
                DatafeedProvider::TRUSHSOCIAL => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            provider.as_str(),
                            SigbotTrushSocialDatafeedClient::new().await, // TODO: set up run configuration?
                        )
                        .expect("Failed to register the Trush Social datafeed.");
                }
            };

            let registered = Self::get_implementation(provider.as_str().to_owned())
                .await
                .expect("Failed to get the registered datafeed.");

            info!("Initializing the datafeed with provider: {}", &provider.as_str());
            registered.init(argument.to_owned()).await;
            datafeeds.push(registered);
            info!("Initialized the datafeed with provider: {}.", &provider.as_str());
        }

        Ok((Arc::new(datafeeds), argument))
    }

    fn register0(
        &mut self,
        name: &str,
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
