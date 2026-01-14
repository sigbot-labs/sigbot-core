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
// This includes derived works.

// LogManager factory - placeholder for future extension if needed
// Currently, we use SigbotLogManagerDefault directly

use anyhow::Context;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::decode_arg_config;
use sigbot_types::sys::log::{LogManagerArgument, LogMgrProvider};
use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use crate::manager::logmanager_default::SigbotDefaultLogManager;

#[async_trait]
pub trait ISigbotLogManager: Send + Sync {
    fn provider(&self) -> LogMgrProvider;
    async fn init(&self, argument: Arc<LogManagerArgument>);
    async fn close(&self);
    async fn start_archiving(&self, messager: Arc<dyn ISigbotMessagerClient + Send + Sync>) -> Result<(), Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotLogManagerFactory> = RwLock::new(SigbotLogManagerFactory::new());
}

pub struct SigbotLogManagerFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotLogManager + Send + Sync>>,
}

impl SigbotLogManagerFactory {
    fn new() -> Self {
        SigbotLogManagerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotLogManagerFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<(Arc<dyn ISigbotLogManager + Send + Sync>, Arc<LogManagerArgument>), Error> {
        // e.g '--order-manager-provider=default'
        let provider = LogMgrProvider::of(
            &matches
                .get_one::<String>("LOG_MANAGER_PROVIDER")
                .unwrap_or(&LogMgrProvider::DEFAULT.as_str().to_owned()),
        )?;

        debug!("Registering Log manager: {}", &provider.as_str());

        // e.g '--order-manager-configuration=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("LOG_MANAGER_CONFIGURATION")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| LogMgrProvider::DEFAULT.as_str().to_owned())
            })
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            LogManagerArgument::from_json(
                &decode_arg_config(&configuration)
                    .context(format!("Failed to decode the configuration: {}", configuration))?,
            )
            .context(format!("Failed to parse the configuration: {}", configuration))?,
        );

        match provider {
            LogMgrProvider::DEFAULT => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(provider.as_str(), SigbotDefaultLogManager::new().await)
                    .expect("Failed to register the Default Log manager.");
            }
        };

        let registered = Self::get_implementation(provider.as_str().to_owned())
            .await
            .expect("Failed to get the registered Log manager.");

        debug!("Initializing the Log manager with provider: {}", &provider.as_str());
        registered.init(argument.to_owned()).await;
        info!("Initialized the Log manager with provider: {}.", &provider.as_str());

        Ok((registered, argument))
    }

    fn register0(
        &mut self,
        name: &str,
        handler: Arc<dyn ISigbotLogManager + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotLogManager + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the Log manager '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.clone());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotLogManager + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotLogManagerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Log manager '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotLogManagerFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}

#[cfg(test)]
mod tests {}
