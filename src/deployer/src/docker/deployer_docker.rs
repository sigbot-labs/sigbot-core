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
use anyhow::{Context, Result};
use async_trait::async_trait;
use bollard::container::{Config, CreateContainerOptions, StartContainerOptions};
use bollard::network::CreateNetworkOptions;
use bollard::Docker;
use common_telemetry::{error, info, warn};
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use sigbot_core::sys::handler::dlock_handler::IDLockHandler;
use sigbot_types::sys::tenant::Tenant;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotDockerDeployer {
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    state: Arc<SigbotState>,
    docker_client: Arc<Mutex<Option<Docker>>>,
}

impl SigbotDockerDeployer {
    pub const NAME: &'static str = "DOCKER";
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    pub async fn new(
        schedule_cron: Option<String>,
        schedule_channels: Option<usize>,
        state: Option<Arc<SigbotState>>,
    ) -> Arc<Self> {
        // Initialize Docker client
        let docker_url = std::env::var("DOCKER_HOST").unwrap_or_else(|_| "unix:///var/run/docker.sock".to_string());

        let docker_client = if docker_url.starts_with("unix://") {
            // Unix socket connection
            let socket_path = docker_url.replace("unix://", "");
            match Docker::connect_with_socket(&socket_path, 120, bollard::API_DEFAULT_VERSION) {
                Ok(client) => {
                    info!("Connected to Docker daemon at {}", docker_url);
                    Some(client)
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to Docker daemon at {}: {}. Deployer will run in limited mode.",
                        docker_url, e
                    );
                    None
                }
            }
        } else {
            // TCP connection (e.g., tcp://localhost:2375)
            match Docker::connect_with_http(&docker_url, 120, bollard::API_DEFAULT_VERSION) {
                Ok(client) => {
                    info!("Connected to Docker daemon at {}", docker_url);
                    Some(client)
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to Docker daemon at {}: {}. Deployer will run in limited mode.",
                        docker_url, e
                    );
                    None
                }
            }
        };

        Arc::new(Self {
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            state: state.expect("SigbotState is required"),
            docker_client: Arc::new(Mutex::new(docker_client)),
        })
    }

    pub(super) async fn execute(&self) {
        info!("Executing Docker deployer ...");

        // Acquire distributed lock using core module handler
        let dlock_name = "DOCKER_DEPLOYER";
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

        info!("Executed Docker deployer.");
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
        let config = get_config();
        let network_name = format!(
            "{}{}-network",
            config.services.deployer.network.tenant_network_prefix, tenant_id
        );

        info!(
            "Starting middleware components for tenant {} ({})",
            tenant_id, tenant_name
        );

        let guard = self.docker_client.lock().await;
        let docker_client = match guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Docker client not available. Skipping middleware deployment for tenant {}",
                    tenant_id
                );
                return;
            }
        };

        // Create network
        if let Err(e) = self.create_network(docker_client, &network_name).await {
            error!("Failed to create network {}: {}", network_name, e);
            return;
        }

        // Deploy EMQX
        if let Err(e) = self
            .deploy_emqx_container(docker_client, &network_name, tenant_id, tenant_name)
            .await
        {
            error!("Failed to deploy EMQX for tenant {}: {}", tenant_id, e);
        }

        // Deploy PostgreSQL
        if let Err(e) = self
            .deploy_postgresql_container(docker_client, &network_name, tenant_id, tenant_name)
            .await
        {
            error!("Failed to deploy PostgreSQL for tenant {}: {}", tenant_id, e);
        }

        // Deploy TimescaleDB
        if let Err(e) = self
            .deploy_timescaledb_container(docker_client, &network_name, tenant_id, tenant_name)
            .await
        {
            error!("Failed to deploy TimescaleDB for tenant {}: {}", tenant_id, e);
        }

        info!("Completed middleware components startup for tenant {}", tenant_id);
    }

    async fn shutdown_middleware_components(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let config = get_config();
        let network_name = format!(
            "{}{}-network",
            config.services.deployer.network.tenant_network_prefix, tenant_id
        );

        info!("Shutting down middleware components for tenant {}", tenant_id);

        let docker_client_guard = self.docker_client.lock().await;
        let docker_client = match docker_client_guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Docker client not available. Skipping middleware shutdown for tenant {}",
                    tenant_id
                );
                return;
            }
        };

        // Stop and remove containers
        let components = vec!["emqx", "postgresql", "timescaledb"];
        for component in components {
            let container_name = format!("{}-{}", component, tenant_id);
            self.stop_and_remove_container(docker_client, &container_name).await;
        }

        // Remove network
        if let Err(e) = docker_client.remove_network(&network_name).await {
            warn!("Failed to remove network {}: {}", network_name, e);
        } else {
            info!("Removed network {} for tenant {}", network_name, tenant_id);
        }
    }

    async fn startup_datafeed_runner(&self, tenant: Arc<Tenant>) {
        self.startup_microservice_container(tenant, "datafeed").await;
    }

    async fn shutdown_datafeed_runner(&self, tenant: Arc<Tenant>) {
        self.shutdown_microservice_container(tenant, "datafeed").await;
    }

    async fn startup_strategy_runner(&self, tenant: Arc<Tenant>) {
        self.startup_microservice_container(tenant, "strategy").await;
    }

    async fn shutdown_strategy_runner(&self, tenant: Arc<Tenant>) {
        self.shutdown_microservice_container(tenant, "strategy").await;
    }

    async fn startup_notification_forwarder(&self, tenant: Arc<Tenant>) {
        self.startup_microservice_container(tenant, "notification").await;
    }

    async fn shutdown_notification_forwarder(&self, tenant: Arc<Tenant>) {
        self.shutdown_microservice_container(tenant, "notification").await;
    }

    async fn startup_backtest_runner(&self, tenant: Arc<Tenant>) {
        self.startup_microservice_container(tenant, "backtest").await;
    }

    async fn shutdown_backtest_runner(&self, tenant: Arc<Tenant>) {
        self.shutdown_microservice_container(tenant, "backtest").await;
    }

    async fn create_network(&self, docker: &Docker, name: &str) -> Result<()> {
        // Check if network already exists
        use bollard::network::InspectNetworkOptions;
        match docker
            .inspect_network(name, None::<InspectNetworkOptions<String>>)
            .await
        {
            Ok(_) => {
                info!("Network {} already exists", name);
                return Ok(());
            }
            Err(_) => {
                // Network doesn't exist, create it
            }
        }

        let mut network_config = CreateNetworkOptions {
            name: name.to_string(),
            driver: "bridge".to_string(),
            ..Default::default()
        };
        network_config.driver = "bridge".to_string();

        docker
            .create_network(network_config)
            .await
            .context("Failed to create network")?;

        info!("Created network {}", name);
        Ok(())
    }

    async fn deploy_emqx_container(
        &self,
        docker: &Docker,
        network: &str,
        tenant_id: i64,
        tenant_name: &str,
    ) -> Result<()> {
        let container_name = format!("emqx-{}", tenant_id);
        let config = get_config();

        use bollard::container::InspectContainerOptions;
        if docker
            .inspect_container(&container_name, Some(InspectContainerOptions { size: false }))
            .await
            .is_ok()
        {
            info!("EMQX container {} already exists", container_name);
            return Ok(());
        }

        let container_config = Config {
            image: Some(config.services.deployer.images.emqx.clone()),
            env: Some(vec![
                format!("TENANT_ID={}", tenant_id),
                format!("TENANT_NAME={}", tenant_name),
            ]),
            exposed_ports: Some({
                let mut ports = HashMap::new();
                ports.insert("1883/tcp".to_string(), HashMap::new());
                ports.insert("8083/tcp".to_string(), HashMap::new());
                ports
            }),
            host_config: Some(bollard::models::HostConfig {
                network_mode: Some(network.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: container_name.clone(),
            platform: None,
        };

        docker
            .create_container(Some(options), container_config)
            .await
            .context("Failed to create EMQX container")?;

        docker
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
            .context("Failed to start EMQX container")?;

        info!(
            "Created and started EMQX container {} on network {}",
            container_name, network
        );
        Ok(())
    }

    async fn deploy_postgresql_container(
        &self,
        docker: &Docker,
        network: &str,
        tenant_id: i64,
        tenant_name: &str,
    ) -> Result<()> {
        let container_name = format!("postgresql-{}", tenant_id);
        let config = get_config();

        use bollard::container::InspectContainerOptions;
        if docker
            .inspect_container(&container_name, Some(InspectContainerOptions { size: false }))
            .await
            .is_ok()
        {
            info!("PostgreSQL container {} already exists", container_name);
            return Ok(());
        }

        let container_config = Config {
            image: Some(config.services.deployer.images.postgresql.clone()),
            env: Some(vec![
                format!("POSTGRES_DB=sigbot_{}", tenant_id),
                "POSTGRES_USER=postgres".to_string(),
                "POSTGRES_PASSWORD=changeit".to_string(),
            ]),
            exposed_ports: Some({
                let mut ports = HashMap::new();
                ports.insert("5432/tcp".to_string(), HashMap::new());
                ports
            }),
            host_config: Some(bollard::models::HostConfig {
                network_mode: Some(network.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: container_name.clone(),
            platform: None,
        };

        docker
            .create_container(Some(options), container_config)
            .await
            .context("Failed to create PostgreSQL container")?;

        docker
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
            .context("Failed to start PostgreSQL container")?;

        info!(
            "Created and started PostgreSQL container {} on network {}",
            container_name, network
        );
        Ok(())
    }

    async fn deploy_timescaledb_container(
        &self,
        docker: &Docker,
        network: &str,
        tenant_id: i64,
        tenant_name: &str,
    ) -> Result<()> {
        let container_name = format!("timescaledb-{}", tenant_id);
        let config = get_config();

        use bollard::container::InspectContainerOptions;
        if docker
            .inspect_container(&container_name, Some(InspectContainerOptions { size: false }))
            .await
            .is_ok()
        {
            info!("TimescaleDB container {} already exists", container_name);
            return Ok(());
        }

        let container_config = Config {
            image: Some(config.services.deployer.images.timescaledb.clone()),
            env: Some(vec![
                format!("POSTGRES_DB=sigbot_ts_{}", tenant_id),
                "POSTGRES_USER=postgres".to_string(),
                "POSTGRES_PASSWORD=changeit".to_string(),
            ]),
            exposed_ports: Some({
                let mut ports = HashMap::new();
                ports.insert("5432/tcp".to_string(), HashMap::new());
                ports
            }),
            host_config: Some(bollard::models::HostConfig {
                network_mode: Some(network.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: container_name.clone(),
            platform: None,
        };

        docker
            .create_container(Some(options), container_config)
            .await
            .context("Failed to create TimescaleDB container")?;

        docker
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
            .context("Failed to start TimescaleDB container")?;

        info!(
            "Created and started TimescaleDB container {} on network {}",
            container_name, network
        );
        Ok(())
    }

    async fn startup_microservice_container(&self, tenant: Arc<Tenant>, component: &str) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let tenant_name = tenant.name.as_deref().unwrap_or("unknown");
        let config = get_config();
        let network_name = format!(
            "{}{}-network",
            config.services.deployer.network.tenant_network_prefix, tenant_id
        );
        let container_name = format!("{}-{}", component, tenant_id);

        let docker_client_guard = self.docker_client.lock().await;
        let docker_client = match docker_client_guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Docker client not available. Skipping {} deployment for tenant {}",
                    component, tenant_id
                );
                return;
            }
        };

        use bollard::container::InspectContainerOptions;
        if docker_client
            .inspect_container(&container_name, Some(InspectContainerOptions { size: false }))
            .await
            .is_ok()
        {
            info!("{} container {} already exists", component, container_name);
            return;
        }

        let container_config = Config {
            image: Some(config.services.deployer.images.sigbot.clone()),
            cmd: Some(vec![component.to_string()]),
            env: Some(vec![
                format!("TENANT_ID={}", tenant_id),
                format!("TENANT_NAME={}", tenant_name),
            ]),
            host_config: Some(bollard::models::HostConfig {
                network_mode: Some(network_name.clone()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: container_name.clone(),
            platform: None,
        };

        if let Err(e) = docker_client.create_container(Some(options), container_config).await {
            error!(
                "Failed to create {} container for tenant {}: {}",
                component, tenant_id, e
            );
            return;
        }

        if let Err(e) = docker_client
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
        {
            error!(
                "Failed to start {} container for tenant {}: {}",
                component, tenant_id, e
            );
            return;
        }

        info!(
            "Created and started {} container {} for tenant {} on network {}",
            component, container_name, tenant_id, network_name
        );
    }

    async fn shutdown_microservice_container(&self, tenant: Arc<Tenant>, component: &str) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let container_name = format!("{}-{}", component, tenant_id);

        let docker_client_guard = self.docker_client.lock().await;
        let docker_client = match docker_client_guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Docker client not available. Skipping {} container shutdown for tenant {}",
                    component, tenant_id
                );
                return;
            }
        };

        self.stop_and_remove_container(docker_client, &container_name).await;
    }

    async fn stop_and_remove_container(&self, docker: &Docker, container_name: &str) {
        // Stop container
        if let Err(e) = docker.stop_container(container_name, None).await {
            warn!("Failed to stop container {}: {}", container_name, e);
        } else {
            info!("Stopped container {}", container_name);
        }

        // Remove container
        if let Err(e) = docker.remove_container(container_name, None).await {
            warn!("Failed to remove container {}: {}", container_name, e);
        } else {
            info!("Removed container {}", container_name);
        }
    }
}

#[async_trait]
impl ISigbotDeployer for SigbotDockerDeployer {
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

        info!("Starting Docker deployer with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running Docker deployer ...", chrono::Utc::now());
                that.execute().await;
            })
        })
        .expect("Failed to create Docker deployer job");

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create Docker deployer scheduler");
        scheduler.add(job).await.expect("Failed to add Docker deployer job");
        scheduler
            .start()
            .await
            .expect("Failed to start Docker deployer scheduler");

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started Docker deployer with cron '{}', channels '{}'",
            cron, channel_size
        );
    }

    async fn shutdown(&self) {
        info!(
            "Closing Docker deployer with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown Docker deployer scheduler");
        }
        info!(
            "Closed Docker deployer with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
    }
}

#[cfg(test)]
mod tests {}
