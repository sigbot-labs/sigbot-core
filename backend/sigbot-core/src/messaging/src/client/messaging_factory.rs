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
use sigbot_types::modules::messaging::messaging::{MessagingInfo, MessagingProvider};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::client::messaging_mqtt::SigbotMqttClient;

#[async_trait]
pub trait ISigbotMessagingClient: Send + Sync {
    async fn init(&self);
    async fn shutdown(&self);
    async fn publish(&self, to: &str, message: &str) -> Result<String, Error>;
    async fn subscribe(&self, topic: &str) -> Result<String, Error>;
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

    pub async fn init() {
        unimplemented!()
    }

    pub async fn register(
        messaaging: Arc<MessagingInfo>,
    ) -> Result<Arc<dyn ISigbotMessagingClient + Send + Sync>, Error> {
        info!("Register Sigbot Datafeed ...");
        let handler: Arc<dyn ISigbotMessagingClient + Send + Sync> =
            match messaaging.to_owned().provider.clone().unwrap() {
                MessagingProvider::MQTT => SigbotMqttClient::new(messaaging.to_owned()).await,
            };
        let name = messaaging.name.clone().unwrap_or_default();
        let result = {
            let mut factory = SigbotMessagingClientFactory::get().write().unwrap();
            factory.register0(name, handler.to_owned())
        };
        result
    }

    fn register0(
        &mut self,
        name: String,
        handler: Arc<dyn ISigbotMessagingClient + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotMessagingClient + Send + Sync>, Error> {
        if self.implementations.contains_key(&name) {
            debug!("Already register the sigbot messaging operation '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name, handler.to_owned());
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
            implementation.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {}
