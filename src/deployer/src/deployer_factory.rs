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

use crate::{
    docker::deployer_docker::SigbotDockerDeployer, kubernetes::deployer_kubernetes::SigbotKubernetesDeployer,
    standalone::deployer_standalone::SigbotStandaloneDeployer,
};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_core::context::state::SigbotState;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotDeployer: Send + Sync {
    fn name(&self) -> &'static str;
    async fn startup(&self);
    async fn shutdown(&self);
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotDeployerFactory> = RwLock::new(SigbotDeployerFactory::new());
}

pub struct SigbotDeployerFactory {
    implementations: HashMap<String, Arc<dyn ISigbotDeployer + Send + Sync>>,
}

impl SigbotDeployerFactory {
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    /// The API host embeds the shared controller; it is not an independently deployed service.
    pub async fn embedded(state: Arc<SigbotState>) -> Arc<dyn ISigbotDeployer> {
        use sigbot_core::config::config::WorkerDeployProvider;
        let cron = Some(state.config.services.deployer.inner.cron.clone());
        let channels = Some(state.config.services.deployer.inner.channel_size);
        let controller: Arc<dyn ISigbotDeployer> = match state.config.services.deployer.workers.provider {
            WorkerDeployProvider::Kubernetes => SigbotKubernetesDeployer::new(cron, channels, Some(state)).await,
            WorkerDeployProvider::Docker => SigbotDockerDeployer::new(cron, channels, Some(state)).await,
        };
        controller.startup().await;
        controller
    }

    fn new() -> Self {
        SigbotDeployerFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotDeployerFactory> {
        &SINGLE_INSTANCE
    }

    #[allow(unused_variables)]
    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        // Initialize SigbotState
        let config = sigbot_core::config::config::get_config();
        let app_state = Arc::new(SigbotState::new(&config).await);

        // Parse provider from command line arguments
        // Support both '--runtime-mode' and '--deploy' for backward compatibility
        let provider = matches
            .try_get_one::<String>("runtime-mode")
            .or_else(|_| matches.try_get_one::<String>("deploy"))
            .ok()
            .flatten()
            .map(|s| s.to_owned())
            .unwrap_or_else(|| SigbotKubernetesDeployer::NAME.to_owned())
            .to_uppercase();

        debug!("Registering Deployer: {}", &provider);

        // Create deployer instance based on provider
        match provider.as_str() {
            SigbotKubernetesDeployer::NAME => {
                let deployer = SigbotKubernetesDeployer::new(None, None, Some(app_state.clone())).await;
                Self::get()
                    .write()
                    .unwrap()
                    .register0(&SigbotKubernetesDeployer::NAME.to_owned(), deployer)
                    .expect("Failed to register the Kubernetes deployer.");
            }
            SigbotDockerDeployer::NAME => {
                let deployer = SigbotDockerDeployer::new(None, None, Some(app_state.clone())).await;
                Self::get()
                    .write()
                    .unwrap()
                    .register0(&SigbotDockerDeployer::NAME.to_owned(), deployer)
                    .expect("Failed to register the Docker deployer.");
            }
            SigbotStandaloneDeployer::NAME => {
                let deployer = SigbotStandaloneDeployer::new(None, None, Some(app_state.clone())).await;
                Self::get()
                    .write()
                    .unwrap()
                    .register0(&SigbotStandaloneDeployer::NAME.to_owned(), deployer)
                    .expect("Failed to register the Standalone deployer.");
            }
            _ => panic!("Unsupported sigbot deployer provider : '{}'.", provider),
        };

        let registered = Self::get_implementation(provider.to_owned())
            .await
            .expect("Failed to get the registered deployer.");

        info!("Starting the deployer with provider: {}", &provider);
        registered.startup().await;
        info!("Started the deployer with provider: {}.", &provider);
    }

    fn register0<T: ISigbotDeployer + Send + Sync + 'static>(
        &mut self,
        name: &String,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(name) {
            debug!("Already register the Deployer '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name.to_owned(), handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(name: String) -> Result<Arc<dyn ISigbotDeployer + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotDeployerFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered Sigbot Deployer '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn shutdown() {
        let this = SigbotDeployerFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.shutdown().await;
        }
        info!("Shutdown the deployers successfully.");
    }
}
