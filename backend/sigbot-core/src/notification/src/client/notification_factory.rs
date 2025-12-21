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
    notification_email::{SigbotEmailClient, SigbotEmailClientConfig},
    notification_telegram::{SigbotTelegramClient, SigbotTelegramConfig},
};
use anyhow::{Context, Error, Ok};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_types::modules::{
    decode_arg_configuration,
    notification::{notification::NotificationProvider, SigbotNotificationArgument},
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotNotificationClient: Send + Sync {
    fn provider(&self) -> NotificationProvider;
    async fn init(&self);
    async fn close(&self);
    async fn send_simple_message(&self, to: Option<Vec<String>>, message: &str) -> Result<String, Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotNotificationClientFactory> =
        RwLock::new(SigbotNotificationClientFactory::new());
}

pub struct SigbotNotificationClientFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotNotificationClient + Send + Sync>>,
}

impl SigbotNotificationClientFactory {
    fn new() -> Self {
        SigbotNotificationClientFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotNotificationClientFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<
        (
            Arc<Vec<Arc<dyn ISigbotNotificationClient + Send + Sync>>>,
            Arc<SigbotNotificationArgument>,
        ),
        Error,
    > {
        // e.g '--notification-providers=email'
        let providers = matches
            .try_get_one::<String>("NOTIFICATION_PROVIDERS")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| NotificationProvider::EMAIL.as_str().to_owned())
            })
            .context("Failed to parse the notification providers from the command line arguments.")?
            .to_uppercase();

        info!("Registering Sigbot Notification: {}", &providers);

        // e.g '--notification-configuration=base64_encoded_json_string'
        let configuration = matches
            .try_get_one::<String>("NOTIFICATION_CONFIGURATION")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| NotificationProvider::EMAIL.as_str().to_owned())
            })
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            SigbotNotificationArgument::from_json(
                &decode_arg_configuration(&configuration)
                    .context(format!("Failed to decode the configuration: {}", configuration))?,
            )
            .context(format!("Failed to parse the configuration: {}", configuration))?,
        );

        let mut notifications: Vec<Arc<dyn ISigbotNotificationClient + Send + Sync>> = Vec::new();
        for provider in providers
            .split(',')
            .map(NotificationProvider::of)
            .collect::<Result<Vec<NotificationProvider>, anyhow::Error>>()?
        {
            match provider {
                NotificationProvider::EMAIL => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            provider.as_str(),
                            SigbotEmailClient::new(Some(SigbotEmailClientConfig::from_config(
                                argument.notification_config.to_owned(),
                            )))
                            .await, // TODO: set up run configuration?
                        )
                        .context("Failed to register the Email notification.")?;
                }
                NotificationProvider::TELEGRAM => {
                    Self::get()
                        .write()
                        .unwrap()
                        .register0(
                            provider.as_str(),
                            SigbotTelegramClient::new(Some(SigbotTelegramConfig::from_config(
                                argument.notification_config.to_owned(),
                            )))
                            .await,
                        )
                        .context("Failed to register the Telegram notification.")?;
                }
            };

            let registered = Self::get_implementation(provider.as_str().to_owned())
                .await
                .context("Failed to get the registered notification.")?;

            info!("Initializing the notification with provider: {}", &provider.as_str());
            registered.init().await;
            info!("Initialized the notification with provider: {}.", &provider.as_str());

            notifications.push(registered.clone());
        }

        Ok((Arc::new(notifications), argument))
    }

    fn register0(
        &mut self,
        name: &str,
        handler: Arc<dyn ISigbotNotificationClient + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotNotificationClient + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the Notification '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.clone());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotNotificationClient + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotNotificationClientFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered sigbot notification client '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotNotificationClientFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}

#[cfg(test)]
mod tests {}
