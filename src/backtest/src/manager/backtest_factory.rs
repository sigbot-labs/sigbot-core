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

use crate::manager::kline::backtest_kline::SigbotKlineBacktestManager;
use crate::manager::trades::backtest_trades::SigbotTradesBacktestManager;
use anyhow::{Context, Error, Ok};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::{
    backtest::{BacktestMgrProvider, SigbotBacktestManagerArgument},
    decode_arg_config,
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotBacktestManager: Send + Sync {
    fn provider(&self) -> BacktestMgrProvider;
    async fn startup(
        &self,
        argument: Arc<SigbotBacktestManagerArgument>,
        messager: Arc<dyn ISigbotMessagerClient + Send + Sync>,
    );
    async fn shutdown(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotBacktestManagerFactory> = RwLock::new(SigbotBacktestManagerFactory::new());
}

pub struct SigbotBacktestManagerFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotBacktestManager + Send + Sync>>,
}

impl SigbotBacktestManagerFactory {
    fn new() -> Self {
        SigbotBacktestManagerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotBacktestManagerFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<
        (
            Arc<dyn ISigbotBacktestManager + Send + Sync>,
            Arc<SigbotBacktestManagerArgument>,
        ),
        Error,
    > {
        // e.g '--backtest-manager-provider=kline'
        let provider = BacktestMgrProvider::of(
            &matches
                .get_one::<String>("BACKTEST_MANAGER_PROVIDER")
                .unwrap_or(&BacktestMgrProvider::KLINE.as_str().to_owned()),
        )?;

        info!("Registering Sigbot Backtest Manager: {}", &provider.as_str());

        // e.g '--backtest-manager-configuration=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("BACKTEST_MANAGER_CONFIGURATION")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| BacktestMgrProvider::KLINE.as_str().to_owned())
            })
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            SigbotBacktestManagerArgument::from_json(
                &decode_arg_config(&configuration)
                    .context(format!("Failed to decode the configuration: {}", configuration))?,
            )
            .context(format!("Failed to parse the configuration: {}", configuration))?,
        );

        match provider {
            BacktestMgrProvider::KLINE => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(provider.as_str(), SigbotKlineBacktestManager::new(None, None).await)
                    .expect("Failed to register the Kline Backtest manager.");
            }
            BacktestMgrProvider::TRADES => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(provider.as_str(), SigbotTradesBacktestManager::new(None, None).await)
                    .expect("Failed to register the Trades Backtest manager.");
            }
        };

        let registered = Self::get_implementation(provider.as_str().to_owned())
            .await
            .expect("Failed to get the registered Backtest manager.");

        Ok((registered, argument))
    }

    fn register0(
        &mut self,
        name: &str,
        handler: Arc<dyn ISigbotBacktestManager + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotBacktestManager + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the Backtest manager '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.clone());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotBacktestManager + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotBacktestManagerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Backtest manager '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotBacktestManagerFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.shutdown().await;
        }
    }
}
