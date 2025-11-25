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

use super::backtest_simple_execution::SimpleExecuteBasedVerifier;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use lazy_static::lazy_static;
use sigbot_server::config::config;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigBotVerifier: Send + Sync {
    async fn init(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigBotVerifierManager> = RwLock::new(SigBotVerifierManager::new());
}

pub struct SigBotVerifierManager {
    pub implementations: HashMap<String, Arc<dyn ISigBotVerifier + Send + Sync>>,
}

impl SigBotVerifierManager {
    fn new() -> Self {
        SigBotVerifierManager {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigBotVerifierManager> {
        &SINGLE_INSTANCE
    }

    pub async fn init() {
        info!("Register All SigBot executors ...");

        for config in &config::get_config().services.backtests {
            if !config.enabled {
                info!("Skipping implementation executor: {}", config.name);
                continue;
            }
            // TODO: Full use similar java spi provider mechanism.
            if config.kind == SimpleExecuteBasedVerifier::KIND {
                match Self::get()
                    .write() // If acquire fails, then it block until acquired.
                    .unwrap() // If acquire fails, then it should panic.
                    .register(config.kind.to_owned(), SimpleExecuteBasedVerifier::new(config).await)
                {
                    Ok(registered) => {
                        info!("Initializing SigBot Verifier ...");
                        let _ = registered.init().await;
                    }
                    Err(e) => panic!("Failed to register SigBot Verifier: {}", e),
                }
            }
        }
    }

    fn register<T: ISigBotVerifier + Send + Sync + 'static>(
        &mut self,
        name: String,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(&name) {
            tracing::debug!("Already register the Verifier '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name, handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigBotVerifier + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigBotVerifierManager::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Verifier '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }
}
