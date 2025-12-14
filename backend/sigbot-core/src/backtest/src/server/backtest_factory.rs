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

use crate::server::{
    kline::backtest_kline::SigbotKlineBacktestRunner, trades::backtest_trades::SigbotTradesBacktestRunner,
};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotBacktestRunner: Send + Sync {
    fn name(&self) -> &'static str;
    async fn startup(&self);
    async fn shutdown(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotBacktestRunnerFactory> = RwLock::new(SigbotBacktestRunnerFactory::new());
}

pub struct SigbotBacktestRunnerFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotBacktestRunner + Send + Sync>>,
}

impl SigbotBacktestRunnerFactory {
    fn new() -> Self {
        SigbotBacktestRunnerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotBacktestRunnerFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        info!("Starting backtest runners ...");

        // e.g '--provider=TICKER_BASED'
        let provider = matches
            .try_get_one::<String>("provider")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| SigbotTradesBacktestRunner::NAME.to_owned())
            })
            .expect("Failed to parse the backtest runner provider from the command line arguments.")
            .to_uppercase();

        info!("Registering backtest runner with provider: {}", &provider);
        match provider.as_str() {
            SigbotTradesBacktestRunner::NAME => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        &SigbotTradesBacktestRunner::NAME.to_owned(),
                        SigbotTradesBacktestRunner::new(None, None).await, // TODO: set up run configuration?
                    )
                    .expect(&format!("Failed to register the backtest runner with provider: {}.", &provider).as_str());
            }
            SigbotKlineBacktestRunner::NAME => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        &SigbotKlineBacktestRunner::NAME.to_owned(),
                        SigbotKlineBacktestRunner::new(None, None).await, // TODO: set up run configuration?
                    )
                    .expect(&format!("Failed to register the backtest runner with provider: {}.", &provider).as_str());
            }
            _ => panic!("Unsupported backtest runner provider : '{}'.", provider),
        };

        let registered = Self::get_implementation(provider.to_owned()).await.expect(&format!(
            "Failed to get the registered backtest runner with provider: {}.",
            &provider
        ));

        info!("Starting the backtest runner with provider: {}", &provider);
        registered.startup().await;
        info!("Started the backtest runner with provider: {}.", &provider);
    }

    fn register0<T: ISigbotBacktestRunner + Send + Sync + 'static>(
        &mut self,
        name: &String,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the backtest runner '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotBacktestRunner + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotBacktestRunnerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered backtest runner '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn shutdown() {
        let this = SigbotBacktestRunnerFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.shutdown().await;
        }
    }
}
