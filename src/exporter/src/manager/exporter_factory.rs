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

use crate::manager::{
    exporter_googlestreet::SigbotGoogleSheetsExporterManager, exporter_kafka::SigbotKafkaExporterManager,
};
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::{
    decode_arg_config,
    exporter::{ExporterMgrProvider, SigbotExporterManagerArgument},
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotExporterManager: Send + Sync {
    fn provider(&self) -> ExporterMgrProvider;
    async fn init(&self, argument: Arc<SigbotExporterManagerArgument>);
    async fn close(&self);
    /// Subscribe to messager topics and handle data export
    async fn subscribe(&self, messager: Arc<dyn ISigbotMessagerClient>) -> Result<(), Error>;
    /// Export data (batch mode).
    async fn export_batch(&self, data: Vec<u8>) -> Result<(), Error>;
    /// Export data (stream mode).
    async fn export_stream(&self, data: Vec<u8>) -> Result<(), Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotExporterManagerFactory> = RwLock::new(SigbotExporterManagerFactory::new());
}

pub struct SigbotExporterManagerFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotExporterManager + Send + Sync>>,
}

impl SigbotExporterManagerFactory {
    fn new() -> Self {
        SigbotExporterManagerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotExporterManagerFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<
        (
            Arc<dyn ISigbotExporterManager + Send + Sync>,
            Arc<SigbotExporterManagerArgument>,
        ),
        Error,
    > {
        // e.g '--exporter-manager-provider=googlesheets' or '--exporter-manager-provider=kafka'
        let provider = ExporterMgrProvider::of(
            &matches
                .get_one::<String>("EXPORTER_MANAGER_PROVIDER")
                .unwrap_or(&ExporterMgrProvider::GOOGLESHEETS.as_str().to_owned()),
        )?;

        debug!("Registering Exporter manager: {}", &provider.as_str());

        // e.g '--exporter-manager-configuration=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("EXPORTER_MANAGER_CONFIGURATION")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| ExporterMgrProvider::GOOGLESHEETS.as_str().to_owned())
            })
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            SigbotExporterManagerArgument::from_json(
                &decode_arg_config(&configuration)
                    .context(format!("Failed to decode the configuration: {}", configuration))?,
            )
            .context(format!("Failed to parse the configuration: {}", configuration))?,
        );

        match provider {
            ExporterMgrProvider::GOOGLESHEETS => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(provider.as_str(), SigbotGoogleSheetsExporterManager::new().await)
                    .expect("Failed to register the GoogleSheets Exporter manager.");
            }
            ExporterMgrProvider::KAFKA => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(provider.as_str(), SigbotKafkaExporterManager::new().await)
                    .expect("Failed to register the Kafka Exporter manager.");
            }
        };

        let registered = Self::get_implementation(provider.as_str().to_owned())
            .await
            .expect("Failed to get the registered Exporter manager.");

        debug!(
            "Initializing the Exporter manager with provider: {}",
            &provider.as_str()
        );
        registered.init(argument.to_owned()).await;
        info!(
            "Initialized the Exporter manager with provider: {}.",
            &provider.as_str()
        );

        Ok((registered, argument))
    }

    fn register0(
        &mut self,
        name: &str,
        handler: Arc<dyn ISigbotExporterManager + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotExporterManager + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the Exporter manager '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.clone());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotExporterManager + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotExporterManagerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Exporter manager '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    /// Unregister and shutdown a specific exporter manager by name
    pub async fn close(name: String) -> Result<(), Error> {
        let manager = {
            let mut this = SigbotExporterManagerFactory::get().write().unwrap();
            this.implementations.remove(&name)
        };
        if let Some(manager) = manager {
            manager.close().await;
            info!("Unregistered and shutdown Exporter manager: {}", name);
            Ok(())
        } else {
            Err(Error::msg(format!("Exporter manager '{}' not found", name)))
        }
    }

    /// Shutdown all exporter managers
    pub async fn shutdown() {
        info!("Shutting down all exporter managers...");
        let managers: Vec<_> = {
            let this = SigbotExporterManagerFactory::get().read().unwrap();
            this.implementations.values().cloned().collect()
        };

        for manager in managers {
            manager.close().await;
        }

        // Clear all implementations
        {
            let mut this = SigbotExporterManagerFactory::get().write().unwrap();
            this.implementations.clear();
        }

        info!("Shutdown all exporter managers.");
    }
}
