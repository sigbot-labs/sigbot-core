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

use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

use crate::client::messaging_mqtt::SigbotMqttClient;

#[async_trait]
pub trait ISigbotMessagingClient: Send + Sync {
    fn name(&self) -> &'static str;
    async fn init(&self);
    async fn close(&self);
    async fn publish(&self, to: &str, message: &str) -> Result<String, Error>;
    async fn subscribe(
        &self,
        topic: &str,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    ) -> Result<(), Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotMessagingClientFactory> = RwLock::new(SigbotMessagingClientFactory::new());
}

pub struct SigbotMessagingClientFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotMessagingClient + Send + Sync>>,
}

impl SigbotMessagingClientFactory {
    fn new() -> Self {
        SigbotMessagingClientFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotMessagingClientFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<Arc<dyn ISigbotMessagingClient + Send + Sync>, Error> {
        // e.g '--provider=mqtt'
        let provider = matches
            .try_get_one::<String>("messaging")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| SigbotMqttClient::NAME.to_owned())
            })
            .context("Failed to parse the messaging provider from the command line arguments.")?;

        info!("Registering Sigbot Messaging: {}", &provider);

        match provider.to_uppercase().as_str() {
            SigbotMqttClient::NAME => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        &SigbotMqttClient::NAME.to_owned(),
                        SigbotMqttClient::new(None).await, // TODO: set up run configuration?
                    )
                    .context("Failed to register the MQTT messaging.")?;
            }
            _ => panic!("Unsupported sigbot messaging provider : '{}'.", provider),
        };

        let registered = Self::get_implementation(provider.to_owned())
            .await
            .context("Failed to get the registered messaging.")?;

        info!("Initializing the messaging with provider: {}", &provider);
        registered.init().await;
        info!("Initialized the messaging with provider: {}.", &provider);

        Ok(registered)
    }

    fn register0(
        &mut self,
        name: &String,
        handler: Arc<dyn ISigbotMessagingClient + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotMessagingClient + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the sigbot messaging operation '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotMessagingClient + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotMessagingClientFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered sigbot messaging operation '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotMessagingClientFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}

#[cfg(test)]
mod tests {}
