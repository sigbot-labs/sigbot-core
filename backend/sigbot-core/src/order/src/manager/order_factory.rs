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

use crate::manager::order_default::SigbotDefaultOrderManager;
use anyhow::{Context, Error, Ok};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_types::modules::order::{OrderMgrProvider, SigbotOrderManagerArgument};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotOrderManager: Send + Sync {
    fn provider(&self) -> OrderMgrProvider;
    async fn init(&self, argument: Arc<SigbotOrderManagerArgument>);
    async fn close(&self);
    async fn subscribe(
        &self,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    );
    /// Process trade signal.
    async fn process_signal(
        &self,
        signal: sigbot_types::modules::order::events::SigbotTradeSignal,
    ) -> Result<(), Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotOrderManagerFactory> = RwLock::new(SigbotOrderManagerFactory::new());
}

pub struct SigbotOrderManagerFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotOrderManager + Send + Sync>>,
}

impl SigbotOrderManagerFactory {
    fn new() -> Self {
        SigbotOrderManagerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotOrderManagerFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<
        (
            Arc<dyn ISigbotOrderManager + Send + Sync>,
            Arc<SigbotOrderManagerArgument>,
        ),
        Error,
    > {
        // e.g '--provider=default'
        let provider = OrderMgrProvider::of(
            &matches
                .get_one::<String>("provider")
                .unwrap_or(&OrderMgrProvider::DEFAULT.as_str().to_owned()),
        )?;

        info!("Registering Sigbot Order manager: {}", &provider.as_str());

        // e.g '--configuration=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("configuration")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| OrderMgrProvider::DEFAULT.as_str().to_owned())
            })
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            SigbotOrderManagerArgument::from_json(&configuration)
                .context("Failed to parse the configuration from the command line arguments.")?,
        );

        match provider {
            OrderMgrProvider::DEFAULT => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(provider.as_str(), SigbotDefaultOrderManager::new().await)
                    .expect("Failed to register the Default Order manager.");
            }
        };

        let registered = Self::get_implementation(provider.as_str().to_owned())
            .await
            .expect("Failed to get the registered Order manager.");

        info!("Initializing the Order manager with provider: {}", &provider.as_str());
        registered.init(argument.to_owned()).await;
        info!("Initialized the Order manager with provider: {}.", &provider.as_str());

        Ok((registered, argument))
    }

    fn register0(
        &mut self,
        name: &str,
        handler: Arc<dyn ISigbotOrderManager + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotOrderManager + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the Order manager '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.clone());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotOrderManager + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotOrderManagerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Order manager '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotOrderManagerFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}
