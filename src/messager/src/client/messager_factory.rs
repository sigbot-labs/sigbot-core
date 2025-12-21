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
    messager_local::{SigbotLocalMessagerClient, SigbotLocalMessagerClientConfig},
    messager_mqtt::{SigbotMqttMessagerClient, SigbotMqttMessagerClientConfig},
};
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::debug;
use lazy_static::lazy_static;
use sigbot_types::modules::messager::messager::{MessagerConfiguration, MessagerProvider};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotMessagerClient: Send + Sync {
    fn provider(&self) -> MessagerProvider;
    async fn init(&self);
    async fn close(&self);
    async fn publish(&self, to: &str, message: &str) -> Result<String, Error>;
    async fn subscribe(
        &self,
        topic: &str,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync>,
    ) -> Result<(), Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotMessagerClientFactory> = RwLock::new(SigbotMessagerClientFactory::new());
}

pub struct SigbotMessagerClientFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotMessagerClient + Send + Sync>>,
}

impl SigbotMessagerClientFactory {
    fn new() -> Self {
        SigbotMessagerClientFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotMessagerClientFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        config: Arc<MessagerConfiguration>,
    ) -> Result<Arc<dyn ISigbotMessagerClient + Send + Sync>, Error> {
        // e.g '--messager-provider=mqtt'
        let provider = MessagerProvider::of(
            &matches
                .get_one::<String>("MESSAGER_PROVIDER")
                .unwrap_or(&MessagerProvider::MQTT.as_str().to_owned()),
        )?;

        debug!("Registering Sigbot Messager: {}", &provider.as_str());

        match provider {
            MessagerProvider::LOCAL => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        &provider.as_str().to_owned(),
                        SigbotLocalMessagerClient::new(Arc::new(SigbotLocalMessagerClientConfig::from_config(config)))
                            .await,
                    )
                    .context("Failed to register the Local messager.")?;
            }
            MessagerProvider::MQTT => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        &provider.as_str().to_owned(),
                        SigbotMqttMessagerClient::new(Arc::new(SigbotMqttMessagerClientConfig::from_config(config)))
                            .await,
                    )
                    .context("Failed to register the MQTT messager.")?;
            }
        };

        let registered = Self::get_implementation(provider.as_str().to_owned())
            .await
            .context("Failed to get the registered messager.")?;

        debug!("Initializing the messager with provider: {}", &provider.as_str());
        registered.init().await;

        Ok(registered)
    }

    fn register0(
        &mut self,
        name: &String,
        handler: Arc<dyn ISigbotMessagerClient + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotMessagerClient + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the sigbot messager client '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotMessagerClient + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotMessagerClientFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered sigbot messager operation '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotMessagerClientFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}

#[cfg(test)]
mod tests {}
