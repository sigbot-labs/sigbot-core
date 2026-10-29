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
    news::{
        datafeed_truthsocial::SigbotTruthSocialDatafeedClient, datafeed_twitter::SigbotTwitterDatafeedClient,
        policy_feed::SigbotPolicyDatafeedClient,
    },
};
use anyhow::{Context, Error, Ok};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_types::modules::{
    datafeed::{datafeed::DatafeedProvider, SigbotDatefeedArgument},
    decode_arg_config,
};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotDatafeedClient: Send + Sync {
    fn provider(&self) -> DatafeedProvider;
    async fn init(&self, argument: Arc<SigbotDatefeedArgument>) -> Result<(), Error>;
    async fn close(&self);
    async fn subscribe(
        &self,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    ) -> Result<(), Error>;
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
        // e.g '--datafeed-providers=binance'
        let providers = matches
            .try_get_one::<String>("DATAFEED_PROVIDERS")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| DatafeedProvider::BINANCE.as_str().to_owned())
            })
            .expect("Failed to parse the datafeed providers from the command line arguments.")
            .to_uppercase();

        debug!("Registering Datafeed: {}", &providers);

        // e.g '--datafeed-configuration=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("DATAFEED_CONFIGURATION")
            .map(|s| s.map(|s| s.to_owned()).unwrap_or_default())
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            SigbotDatefeedArgument::from_json(
                &decode_arg_config(&configuration).context("Failed to decode datafeed configuration")?,
            )
            .context("Failed to parse datafeed configuration")?,
        );

        let properties = argument
            .datafeed_config
            .properties
            .as_ref()
            .context("datafeed properties required")?;
        let query_only = properties
            .get("query_only")
            .map(|value| value.parse::<bool>())
            .transpose()
            .context("query_only must be true or false")?
            .unwrap_or(false);
        if query_only {
            anyhow::ensure!(
                properties.contains_key("snapshot_input_node_ids"),
                "query_only requires declared query consumers"
            );
            return Ok((Arc::new(Vec::new()), argument));
        }

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
                DatafeedProvider::TRUTHSOCIAL => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            provider.as_str(),
                            SigbotTruthSocialDatafeedClient::new().await, // TODO: set up run configuration?
                        )
                        .expect("Failed to register the Trush Social datafeed.");
                }
                DatafeedProvider::FED | DatafeedProvider::TREASURY => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(provider.as_str(), SigbotPolicyDatafeedClient::new(provider.clone()))?;
                }
            };

            let registered = Self::get_implementation(provider.as_str().to_owned())
                .await
                .expect("Failed to get the registered datafeed.");

            debug!("Initializing the datafeed with provider: {}", &provider.as_str());
            registered.init(argument.to_owned()).await?;
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

    /// Unregister and shutdown a specific datafeed client by name
    pub async fn close(name: String) -> Result<(), Error> {
        let client = {
            let mut this = SigbotDatafeedClientFactory::get().write().unwrap();
            this.implementations.remove(&name)
        };
        if let Some(client) = client {
            client.close().await;
            info!("Unregistered and shutdown Datafeed client: {}", name);
            Ok(())
        } else {
            Err(Error::msg(format!("Datafeed client '{}' not found", name)))
        }
    }

    /// Shutdown all datafeed clients
    pub async fn shutdown() {
        info!("Shutting down all datafeed clients...");
        let clients: Vec<_> = {
            let this = SigbotDatafeedClientFactory::get().read().unwrap();
            this.implementations.values().cloned().collect()
        };

        for client in clients {
            client.close().await;
        }

        // Clear all implementations
        {
            let mut this = SigbotDatafeedClientFactory::get().write().unwrap();
            this.implementations.clear();
        }

        info!("Shutdown all datafeed clients.");
    }
}
