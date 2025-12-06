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

use crate::controller::{
    datafeed::controller_datafeed::SigbotDatafeedRunnerController,
    messaging::controller_messaging::SigbotMessagingController,
    notification::controller_notification::SigbotNotificationController,
    strategy::controller_strategy::SigbotStrategyRunnerController,
};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use lazy_static::lazy_static;
use sigbot_core::config::config;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotController: Send + Sync {
    async fn init(&self);
    async fn close(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotControllerFactory> = RwLock::new(SigbotControllerFactory::new());
}

pub struct SigbotControllerFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotController + Send + Sync>>,
}

impl SigbotControllerFactory {
    fn new() -> Self {
        SigbotControllerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotControllerFactory> {
        &SINGLE_INSTANCE
    }

    pub async fn init() {
        let config = config::get_config();

        if config.services.controllers.datafeed.inner.enabled {
            info!("Registering Sigbot Datafeed Controller ...");
            match Self::get()
                .write() // If acquire fails, then it block until acquired.
                .unwrap() // If acquire fails, then it should panic.
                .register(
                    SigbotDatafeedRunnerController::NAME.to_owned(),
                    SigbotDatafeedRunnerController::new(
                        Some(config.services.controllers.datafeed.inner.cron.to_owned()),
                        Some(config.services.controllers.datafeed.inner.channel_size),
                    )
                    .await,
                ) {
                Ok(registered) => {
                    info!("Initializing Sigbot Datafeed Controller ...");
                    let _ = registered.init().await;
                }
                Err(e) => panic!("Failed to register Sigbot Datafeed Controller : {}", e),
            }
        } else {
            info!("Disabled the Datafeed Controller.")
        }

        if config.services.controllers.messaging.inner.enabled {
            info!("Registering Sigbot Messaging Controller ...");
            match Self::get()
                .write() // If acquire fails, then it block until acquired.
                .unwrap() // If acquire fails, then it should panic.
                .register(
                    SigbotMessagingController::NAME.to_owned(),
                    SigbotMessagingController::new(
                        Some(config.services.controllers.messaging.inner.cron.to_owned()),
                        Some(config.services.controllers.messaging.inner.channel_size),
                    )
                    .await,
                ) {
                Ok(registered) => {
                    info!("Initializing Sigbot Messaging Controller  ...");
                    let _ = registered.init().await;
                }
                Err(e) => panic!("Failed to register Sigbot Messaging Controller : {}", e),
            }
        } else {
            info!("Disabled the Messaging Controller.")
        }

        if config.services.controllers.notification.inner.enabled {
            info!("Registering Sigbot Notification Controller ...");
            match Self::get()
                .write() // If acquire fails, then it block until acquired.
                .unwrap() // If acquire fails, then it should panic.
                .register(
                    SigbotNotificationController::NAME.to_owned(),
                    SigbotNotificationController::new(
                        Some(config.services.controllers.notification.inner.cron.to_owned()),
                        Some(config.services.controllers.notification.inner.channel_size),
                    )
                    .await,
                ) {
                Ok(registered) => {
                    info!("Initializing Sigbot Notification Controller ...");
                    let _ = registered.init().await;
                }
                Err(e) => panic!("Failed to register Sigbot Notification Controller : {}", e),
            }
        } else {
            info!("Disabled the Notification Controller.")
        }

        if config.services.controllers.strategy.inner.enabled {
            info!("Registering Sigbot Strategy Controller ...");
            match Self::get()
                .write() // If acquire fails, then it block until acquired.
                .unwrap() // If acquire fails, then it should panic.
                .register(
                    SigbotStrategyRunnerController::NAME.to_owned(),
                    SigbotStrategyRunnerController::new(
                        Some(config.services.controllers.strategy.inner.cron.to_owned()),
                        Some(config.services.controllers.strategy.inner.channel_size),
                    )
                    .await,
                ) {
                Ok(registered) => {
                    info!("Initializing Sigbot Strategy Controller ...");
                    let _ = registered.init().await;
                }
                Err(e) => panic!("Failed to register Sigbot Strategy Controller : {}", e),
            }
        } else {
            info!("Disabled the Strategy Controller.")
        }
    }

    fn register<T: ISigbotController + Send + Sync + 'static>(
        &mut self,
        name: String,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(&name) {
            tracing::debug!("Already register the Controller '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name, handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotController + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotControllerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Controller '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotControllerFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}
