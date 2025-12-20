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

use crate::manager::wallet_default::SigbotDefaultWalletManager;
use anyhow::{Context, Error, Ok};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_core::{
    config::config::PostgresAppDBProperties,
    modules::wallet::store::transaction::{trade_postgres::PostgresWalletUpdater, IWalletUpdater},
};
use sigbot_types::modules::wallet::{SigbotWalletManagerArgument, WalletMgrProvider};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotWalletManager: Send + Sync {
    fn provider(&self) -> WalletMgrProvider;
    async fn init(&self, argument: Arc<SigbotWalletManagerArgument>);
    async fn close(&self);
    async fn subscribe(
        &self,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    );
    /// 处理交易事件
    async fn handle_trade_event(
        &self,
        event: sigbot_types::modules::order::events::SigbotTradeEvent,
    ) -> Result<(), Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotWalletManagerFactory> = RwLock::new(SigbotWalletManagerFactory::new());
}

pub struct SigbotWalletManagerFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotWalletManager + Send + Sync>>,
}

impl SigbotWalletManagerFactory {
    fn new() -> Self {
        SigbotWalletManagerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotWalletManagerFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn init(
        matches: &clap::ArgMatches,
        verbose: bool,
    ) -> Result<
        (
            Arc<dyn ISigbotWalletManager + Send + Sync>,
            Arc<SigbotWalletManagerArgument>,
        ),
        Error,
    > {
        // e.g '--wallet-manager-provider=default'
        let provider = WalletMgrProvider::of(
            &matches
                .get_one::<String>("wallet-manager-provider")
                .unwrap_or(&WalletMgrProvider::DEFAULT.as_str().to_owned()),
        )?;

        info!("Registering Sigbot Wallet manager: {}", &provider.as_str());

        // e.g '--wallet-manager-config=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("wallet-manager-config")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| WalletMgrProvider::DEFAULT.as_str().to_owned())
            })
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            SigbotWalletManagerArgument::from_json(&configuration)
                .context("Failed to parse the configuration from the command line arguments.")?,
        );

        match provider {
            WalletMgrProvider::DEFAULT => {
                // Create trade handler from config
                // Note: Idempotency is now handled at database level via ON CONFLICT
                let db_config = PostgresAppDBProperties::default();
                let trade_handler: Arc<dyn IWalletUpdater> = Arc::new(
                    // TODO: using wallet updater factory
                    PostgresWalletUpdater::new(&db_config)
                        .await
                        .expect("Failed to create trade handler"),
                );
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        WalletMgrProvider::DEFAULT.as_str(),
                        SigbotDefaultWalletManager::new(trade_handler).await,
                    )
                    .expect("Failed to register the Default Wallet manager.");
            }
        };

        let registered = Self::get_implementation(provider.as_str())
            .await
            .expect("Failed to get the registered Wallet manager.");

        info!("Initializing the Wallet manager with provider: {}", &provider.as_str());
        registered.init(argument.to_owned()).await;
        info!("Initialized the Wallet manager with provider: {}.", &provider.as_str());

        Ok((registered, argument))
    }

    fn register0(
        &mut self,
        name: &str,
        handler: Arc<dyn ISigbotWalletManager + Send + Sync>,
    ) -> Result<Arc<dyn ISigbotWalletManager + Send + Sync>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the Wallet manager '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_string(), handler.clone());
        Ok(handler)
    }

    pub async fn get_implementation(name: &str) -> Result<Arc<dyn ISigbotWalletManager + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotWalletManagerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Wallet manager '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotWalletManagerFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}
