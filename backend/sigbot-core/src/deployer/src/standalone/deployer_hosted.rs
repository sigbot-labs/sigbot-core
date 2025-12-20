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

use crate::deployer_factory::{ISigbotDeployer, SigbotDeployerFactory};
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_core::sys::handler::{dlock_handler::IDLockHandler, tenant_handler::ITenantHandler};
use sigbot_types::{
    modules::{
        datafeed::datafeed::{DatafeedInfo, DatafeedProvider},
        strategy::strategy::StrategyInfo,
    },
    sys::tenant::{QueryTenantRequest, Tenant},
    EntityBase, PageRequest, PageResponse,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotHostedDeployer {
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    tenant_handler: Option<Arc<dyn ITenantHandler + Send + Sync>>,
    dlock_handler: Option<Arc<dyn IDLockHandler + Send + Sync>>,
}

impl SigbotHostedDeployer {
    pub const NAME: &'static str = "HOSTED";
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    pub async fn new(schedule_cron: Option<String>, schedule_channels: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            tenant_handler: None, // TODO: Inject the tenant handler.
            dlock_handler: None,  // TODO: Inject the dlock handler.
        })
    }

    pub(super) async fn execute(&self) {
        info!("Executing Host deployer ...");

        // Acquire to distrbuted lock.
        let dlock_name = "HOST_DEPLOYER";
        let acquired = self
            .dlock_handler
            .clone()
            .expect("Dlock handler is not injected.")
            .acquire(dlock_name.to_string(), Duration::from_secs(10))
            .await;
        match acquired {
            Ok(true) => {
                self.process().await;
            }
            Ok(false) => {
                info!("Unable to acquire distributed lock with {}", dlock_name);
            }
            Err(e) => {
                info!("Failed to acquire distributed lock: {:?}", e.to_string());
            }
        }

        info!("Executed Host deployer.");
    }

    /// Implement the logic to scan to activate tenants and deploy to All components.
    /// 1. Scan the tenants from the database.
    /// 2. If the tenant is activated, then to deploy the middleware(e.g EMQX, PostgreSQL, TimescaleDB) components
    ///    and such as datafeed ingestor, strategy runner, notification forwarder, backtest runner, etc.
    /// 3. If the tenant is deactivated, then to undeploy the middleware(e.g EMQX, PostgreSQL, TimescaleDB) components
    ///    and such as datafeed ingestor, strategy runner, notification forwarder, backtest runner, etc.
    async fn process(&self) {
        SigbotDeployerFactory::do_scan_process(
            self.tenant_handler.to_owned().expect("Tenant handler is not injected."),
            |tenant| async move {
                self.startup_middleware_components(tenant.to_owned()).await;
                self.startup_datafeed_runner(tenant.to_owned()).await;
                self.startup_strategy_runner(tenant.to_owned()).await;
            },
            |tenant| async move {
                self.shutdown_middleware_components(tenant.to_owned()).await;
                self.shutdown_datafeed_runner(tenant.to_owned()).await;
                self.shutdown_strategy_runner(tenant.to_owned()).await;
            },
        )
        .await;
    }

    async fn startup_middleware_components(&self, tenant: Arc<Tenant>) {
        // let messager = Arc::new(MessagerInfo {
        //     base: EntityBase::new_with_id(Some(1)),
        //     name: tenant.name.clone(),
        //     provider: Some(MessagerProvider::MQTT),
        //     configuration: None,
        //     secrets: None,
        //     description: Some(format!(
        //         "Datafeed Runner for tenant {}",
        //         tenant.name.clone().unwrap_or_default()
        //     )),
        // });
        // TODO Generate to K8S deployment yaml file with EMQX container runtime args.
        // (e.g: emqx operator - deployment mainfest yaml)

        // TODO Generate to K8S deployment yaml file with PostgreSQL container runtime args.
        // (e.g: postgresql - deployment mainfest yaml)

        // TODO Generate to K8S deployment yaml file with TimescaleDB container runtime args.
        // (e.g: timescale - deployment mainfest yaml)
    }

    async fn shutdown_middleware_components(&self, tenant: Arc<Tenant>) {
        // TODO Obtain the EMQX dpeloyment name from tenant properties statistics and call k8s client delete deployment.
    }

    async fn startup_datafeed_runner(&self, tenant: Arc<Tenant>) {
        let datafeed = Arc::new(DatafeedInfo {
            base: EntityBase::new_with_id(Some(1)), // TODO: Get the datafeed id from the tenant.
            name: tenant.name.clone(),              // TODO: Get the datafeed name from the tenant.
            provider: Some(DatafeedProvider::BINANCE), // TODO: Get the provider from the tenant.
            configuration: None,
            secrets: None,
            description: Some(format!(
                "Datafeed Runner for tenant {}",
                tenant.name.clone().unwrap_or_default()
            )),
        });
        // TODO Generate to K8S deployment yaml file with container runtime args.
        // (e.g: sigbot datafeed --id=1 --storage-type=postgres --storage-url=postgresql://host:5432/sigbot --messager-type=emqx --messager-url=emqx://host:18083)
    }

    async fn shutdown_datafeed_runner(&self, tenant: Arc<Tenant>) {
        // TODO Obtain the datafeed dpeloyment name from tenant properties statistics and call k8s client delete deployment.
    }

    async fn startup_strategy_runner(&self, tenant: Arc<Tenant>) {
        let strategy = Arc::new(StrategyInfo {
            base: EntityBase::new_with_id(Some(1)), // TODO: Get the strategy id from the tenant.
            name: tenant.name.clone(),              // TODO: Get the strategy name from the tenant.
            provider: Some("MJMA20".to_string()),   // TODO: Get the strategy provider from the tenant.
            parameters: None,
            description: Some(format!(
                "Strategy Runner for tenant {}",
                tenant.name.clone().unwrap_or_default()
            )),
        });
        // TODO Generate to K8S deployment yaml file with container runtime args.
        // (e.g: sigbot strategy --id=1 --storage-type=postgres --storage-url=postgresql://host:5432/sigbot --messager-type=emqx --messager-url=emqx://host:18083)
    }

    async fn shutdown_strategy_runner(&self, tenant: Arc<Tenant>) {
        // TODO Obtain the strategy dpeloyment name from tenant properties statistics and call k8s client delete deployment.
    }
}

#[async_trait]
impl ISigbotDeployer for SigbotHostedDeployer {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn startup(&self) {
        let this = self.clone();
        let cron_expression = self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION);
        let channel_size = self.schedule_channels.unwrap_or(Self::DEFAULT_CHANNELS);

        // Validate the cron expression.
        let cron = match Job::new_async(cron_expression, |_uuid, _lock| Box::pin(async {})) {
            Ok(_) => cron_expression,
            Err(e) => {
                tracing::warn!(
                    "Invalid cron expression '{}': {}. Using default '{}'",
                    cron_expression,
                    e,
                    Self::DEFAULT_CRON_EXPRESSION
                );
                Self::DEFAULT_CRON_EXPRESSION
            }
        };

        info!("Starting Datafeed controller with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running Datafeed controller ...", chrono::Utc::now());
                that.execute().await;
            })
        })
        .expect("Failed to create Datafeed controller job");

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create Datafeed controller scheduler");
        scheduler.add(job).await.expect("Failed to add Datafeed controller job");
        scheduler
            .start()
            .await
            .expect("Failed to start Datafeed controller scheduler");

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started Datafeed controller with cron '{}', channels '{}'",
            cron, channel_size
        );
    }

    async fn shutdown(&self) {
        info!(
            "Closing Datafeed controller with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown datafeed controller scheduler");
        }
        info!(
            "Closed Datafeed controller with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
    }
}

#[cfg(test)]
mod tests {}
