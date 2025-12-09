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

use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use sigbot_core::sys::handler::tenant_handler::ITenantHandler;
use sigbot_types::{
    sys::tenant::{QueryTenantRequest, Tenant},
    PageRequest, PageResponse,
};
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, RwLock},
};

use crate::deployer::{
    kubernetes::deployer_kubernetes::SigbotKubernetesDeployer, standalone::deployer_hosted::SigbotHostedDeployer,
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
        // e.g '--deploy=kubernetes'
        let provider = matches
            .try_get_one::<String>("deploy")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| SigbotKubernetesDeployer::NAME.to_owned())
            })
            .expect("Failed to parse the deployer provider from the command line arguments.")
            .to_uppercase();

        info!("Registering Sigbot Deployer: {}", &provider);

        match provider.as_str() {
            SigbotKubernetesDeployer::NAME => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        &SigbotKubernetesDeployer::NAME.to_owned(),
                        SigbotKubernetesDeployer::new(None, None).await,
                    )
                    .expect("Failed to register the Kubernetes deployer.");
            }
            SigbotHostedDeployer::NAME => {
                Self::get()
                    .write()
                    .unwrap()
                    .register0(
                        &SigbotHostedDeployer::NAME.to_owned(),
                        SigbotHostedDeployer::new(None, None).await,
                    )
                    .expect("Failed to register the Hosted deployer.");
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

    pub(crate) async fn do_scan_process<F, FutF, G, FutG>(
        tenant_handler: Arc<dyn ITenantHandler + Send + Sync>,
        startup_handler: F,
        shutdown_handler: G,
    ) where
        F: Fn(Arc<Tenant>) -> FutF + Send + Sync,
        FutF: Future<Output = ()> + Send + Sync,
        G: Fn(Arc<Tenant>) -> FutG + Send + Sync,
        FutG: Future<Output = ()> + Send + Sync,
    {
        info!("Scanning Tenants components lifecycle process ...");

        let mut gatekeeper_counter = 0 as u16;
        let mut last_page = PageResponse::new(None, None, None);
        while gatekeeper_counter > Self::DEFAULT_SAFETY_THRESHOLD
            && (last_page.total.is_none() || last_page.total.unwrap_or(0) > 0)
        {
            gatekeeper_counter += 1;
            info!("Loading Tenants : {}", last_page.num.unwrap_or(1));

            let (current_page, tenants) = tenant_handler
                .find(
                    QueryTenantRequest {
                        name: None,
                        shared: None,
                        admin_id: None,
                        properties: None,
                        description: None,
                    },
                    PageRequest::new(last_page.num.unwrap_or(1) as u32, last_page.limit.unwrap_or(10) as u32),
                )
                .await
                .expect("Failed to find Tenants.");
            last_page = current_page;

            info!("Loaded Tenants {} : {}", tenants.len(), last_page.num.unwrap_or(0));

            for tenant in tenants {
                let t = Arc::new(tenant);
                if t.base.status.unwrap_or(0) == 1 {
                    info!("Initializing Components for : {:?}/{:?}", t.base.id, t.name);
                    startup_handler(t.to_owned()).await;
                } else {
                    info!("Shutting down Components for : {:?}/{:?}", t.base.id, t.name);
                    shutdown_handler(t.to_owned()).await;
                }
            }
        }
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
