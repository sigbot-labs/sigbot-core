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

use crate::server::strategy_default::SigbotDefaultStrategyRunner;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotStrategyRunner: Send + Sync {
    fn name(&self) -> &'static str;
    async fn startup(&self);
    async fn shutdown(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotStrategyRunnerFactory> = RwLock::new(SigbotStrategyRunnerFactory::new());
}

pub struct SigbotStrategyRunnerFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotStrategyRunner + Send + Sync>>,
}

impl SigbotStrategyRunnerFactory {
    fn new() -> Self {
        SigbotStrategyRunnerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotStrategyRunnerFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        info!("Starting strategy runners ...");

        // e.g '--provider=default'
        let provider = matches
            .try_get_one::<String>("provider")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| SigbotDefaultStrategyRunner::NAME.to_owned())
            })
            .expect("Failed to parse the strategy runner provider from the command line arguments.")
            .to_uppercase();

        info!("Registering strategy runner with provider: {}", &provider);
        match provider.as_str() {
            SigbotDefaultStrategyRunner::NAME => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        &SigbotDefaultStrategyRunner::NAME.to_owned(),
                        SigbotDefaultStrategyRunner::new().await, // TODO: set up run configuration?
                    )
                    .expect(&format!("Failed to register the strategy runner with provider: {}.", &provider).as_str());
            }
            _ => panic!("Unsupported strategy runner provider : '{}'.", provider),
        };

        let registered = Self::get_implementation(provider.to_owned()).await.expect(&format!(
            "Failed to get the registered strategy runner with provider: {}.",
            &provider
        ));

        info!("Starting the strategy runner with provider: {}", &provider);
        registered.startup().await;
        info!("Started the strategy runner with provider: {}.", &provider);
    }

    fn register0<T: ISigbotStrategyRunner + Send + Sync + 'static>(
        &mut self,
        name: &String,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(name) {
            tracing::debug!("Already register the Strategy Runner '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotStrategyRunner + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotStrategyRunnerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Strategy Runner '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn shutdown() {
        let this = SigbotStrategyRunnerFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {}
