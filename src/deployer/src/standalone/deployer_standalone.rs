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
use sigbot_core::context::state::SigbotState;
use sigbot_core::sys::handler::dlock_handler::IDLockHandler;
use sigbot_types::sys::tenant::Tenant;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotStandaloneDeployer {
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    state: Arc<SigbotState>,
    // Track initialized tenants in standalone mode
    initialized_tenants: Arc<Mutex<HashMap<i64, bool>>>,
}

impl SigbotStandaloneDeployer {
    pub const NAME: &'static str = "STANDALONE";
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    pub async fn new(
        schedule_cron: Option<String>,
        schedule_channels: Option<usize>,
        state: Option<Arc<SigbotState>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            state: state.expect("SigbotState is required"),
            initialized_tenants: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(super) async fn execute(&self) {
        info!("Executing Host deployer ...");

        // Acquire distributed lock using core module handler
        let dlock_name = "STANDALONE_DEPLOYER";
        let dlock_handler = sigbot_core::sys::handler::dlock_handler::DLockHandler::new(&self.state);
        let acquired = dlock_handler
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
        let this_startup = self.clone();
        let this_shutdown = self.clone();
        SigbotDeployerFactory::do_scan_process(
            self.state.clone(),
            move |tenant| {
                let this = this_startup.clone();
                async move {
                    this.startup_middleware_components(tenant.to_owned()).await;
                    this.startup_datafeed_runner(tenant.to_owned()).await;
                    this.startup_strategy_runner(tenant.to_owned()).await;
                    this.startup_notification_forwarder(tenant.to_owned()).await;
                    this.startup_backtest_runner(tenant.to_owned()).await;
                }
            },
            move |tenant| {
                let this = this_shutdown.clone();
                async move {
                    this.shutdown_middleware_components(tenant.to_owned()).await;
                    this.shutdown_datafeed_runner(tenant.to_owned()).await;
                    this.shutdown_strategy_runner(tenant.to_owned()).await;
                    this.shutdown_notification_forwarder(tenant.to_owned()).await;
                    this.shutdown_backtest_runner(tenant.to_owned()).await;
                }
            },
        )
        .await;
    }

    async fn startup_middleware_components(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let tenant_name = tenant.name.as_deref().unwrap_or("unknown");

        info!(
            "Initializing middleware components for tenant {} ({}) in standalone mode",
            tenant_id, tenant_name
        );

        // In standalone mode, middleware components are shared across all tenants
        // We just need to ensure they are initialized once
        // This is a no-op in standalone mode as components are already running

        let mut initialized = self.initialized_tenants.lock().await;
        initialized.insert(tenant_id, true);

        info!(
            "Middleware components initialized for tenant {} (standalone mode)",
            tenant_id
        );
    }

    async fn shutdown_middleware_components(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);

        info!(
            "Shutting down middleware components for tenant {} in standalone mode",
            tenant_id
        );

        // In standalone mode, we don't actually shutdown shared middleware
        // Just mark tenant as not initialized
        let mut initialized = self.initialized_tenants.lock().await;
        initialized.remove(&tenant_id);

        info!(
            "Middleware components shutdown for tenant {} (standalone mode)",
            tenant_id
        );
    }

    async fn startup_datafeed_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        info!(
            "Initializing datafeed runner for tenant {} in standalone mode",
            tenant_id
        );
        // In standalone mode, components run in-process, no actual deployment needed
        // Components are initialized when needed by the main application
    }

    async fn shutdown_datafeed_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        info!(
            "Shutting down datafeed runner for tenant {} in standalone mode",
            tenant_id
        );
        // In standalone mode, components are shared, no actual shutdown needed
    }

    async fn startup_strategy_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        info!(
            "Initializing strategy runner for tenant {} in standalone mode",
            tenant_id
        );
        // In standalone mode, components run in-process, no actual deployment needed
    }

    async fn shutdown_strategy_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        info!(
            "Shutting down strategy runner for tenant {} in standalone mode",
            tenant_id
        );
        // In standalone mode, components are shared, no actual shutdown needed
    }

    async fn startup_notification_forwarder(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        info!(
            "Initializing notification forwarder for tenant {} in standalone mode",
            tenant_id
        );
        // In standalone mode, components run in-process, no actual deployment needed
    }

    async fn shutdown_notification_forwarder(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        info!(
            "Shutting down notification forwarder for tenant {} in standalone mode",
            tenant_id
        );
        // In standalone mode, components are shared, no actual shutdown needed
    }

    async fn startup_backtest_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        info!(
            "Initializing backtest runner for tenant {} in standalone mode",
            tenant_id
        );
        // In standalone mode, components run in-process, no actual deployment needed
    }

    async fn shutdown_backtest_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        info!(
            "Shutting down backtest runner for tenant {} in standalone mode",
            tenant_id
        );
        // In standalone mode, components are shared, no actual shutdown needed
    }
}

#[async_trait]
impl ISigbotDeployer for SigbotStandaloneDeployer {
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

        info!("Starting Standalone deployer with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running Standalone deployer ...", chrono::Utc::now());
                that.execute().await;
            })
        })
        .expect("Failed to create Standalone deployer job");

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create Standalone deployer scheduler");
        scheduler.add(job).await.expect("Failed to add Standalone deployer job");
        scheduler
            .start()
            .await
            .expect("Failed to start Standalone deployer scheduler");

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started Standalone deployer with cron '{}', channels '{}'",
            cron, channel_size
        );
    }

    async fn shutdown(&self) {
        info!(
            "Closing Standalone deployer with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown Standalone deployer scheduler");
        }
        info!(
            "Closed Standalone deployer with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
    }
}

#[cfg(test)]
mod tests {}
