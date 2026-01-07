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
use common_telemetry::{error, info, warn};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client, Config,
};
use sigbot_core::context::state::SigbotState;
use sigbot_core::sys::handler::dlock_handler::IDLockHandler;
use sigbot_types::sys::tenant::Tenant;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

#[derive(Clone)]
pub struct SigbotKubernetesDeployer {
    schedule_cron: Option<String>,
    schedule_channels: Option<usize>,
    scheduler: Arc<Mutex<Option<JobScheduler>>>,
    state: Arc<SigbotState>,
    kube_client: Arc<Mutex<Option<Client>>>,
}

impl SigbotKubernetesDeployer {
    pub const NAME: &'static str = "KUBERNETES";
    pub const DEFAULT_CRON_EXPRESSION: &'static str = "0/30 * * * * *";
    pub const DEFAULT_CHANNELS: usize = 5;
    pub const DEFAULT_SAFETY_THRESHOLD: u16 = 1000;

    pub async fn new(
        schedule_cron: Option<String>,
        schedule_channels: Option<usize>,
        state: Option<Arc<SigbotState>>,
    ) -> Arc<Self> {
        // Initialize Kubernetes client
        let kube_client = match Config::infer().await {
            Ok(config) => match Client::try_from(config) {
                Ok(client) => Some(client),
                Err(e) => {
                    warn!(
                        "Failed to create Kubernetes client: {}. Deployer will run in limited mode.",
                        e
                    );
                    None
                }
            },
            Err(e) => {
                warn!(
                    "Failed to infer Kubernetes config: {}. Deployer will run in limited mode.",
                    e
                );
                None
            }
        };

        Arc::new(Self {
            schedule_cron,
            schedule_channels,
            scheduler: Arc::new(Mutex::new(None)),
            state: state.expect("SigbotState is required"),
            kube_client: Arc::new(Mutex::new(kube_client)),
        })
    }

    pub(super) async fn execute(&self) {
        info!("Executing Kubernetes deployer ...");

        // Acquire distributed lock using core module handler
        let dlock_name = "KUBERNETES_DEPLOYER";
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

        info!("Executed Kubernetes deployer.");
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
        let namespace_name = format!("sigbot-tenant-{}", tenant_id);

        info!(
            "Starting middleware components for tenant {} ({})",
            tenant_id, tenant_name
        );

        // Check if Kubernetes client is available
        let guard = self.kube_client.lock().await;
        let kube_client = match guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Kubernetes client not available. Skipping middleware deployment for tenant {}",
                    tenant_id
                );
                return;
            }
        };

        // Create namespace
        if let Err(e) = self.create_namespace(kube_client, &namespace_name).await {
            error!("Failed to create namespace {}: {}", namespace_name, e);
            return;
        }

        // Deploy EMQX
        if let Err(e) = self
            .deploy_emqx(kube_client, &namespace_name, tenant_id, tenant_name)
            .await
        {
            error!("Failed to deploy EMQX for tenant {}: {}", tenant_id, e);
        }

        // Deploy PostgreSQL
        if let Err(e) = self
            .deploy_postgresql(kube_client, &namespace_name, tenant_id, tenant_name)
            .await
        {
            error!("Failed to deploy PostgreSQL for tenant {}: {}", tenant_id, e);
        }

        // Deploy TimescaleDB
        if let Err(e) = self
            .deploy_timescaledb(kube_client, &namespace_name, tenant_id, tenant_name)
            .await
        {
            error!("Failed to deploy TimescaleDB for tenant {}: {}", tenant_id, e);
        }

        info!("Completed middleware components startup for tenant {}", tenant_id);
    }

    async fn shutdown_middleware_components(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let namespace_name = format!("sigbot-tenant-{}", tenant_id);

        info!("Shutting down middleware components for tenant {}", tenant_id);

        let guard = self.kube_client.lock().await;
        let kube_client = match guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Kubernetes client not available. Skipping middleware shutdown for tenant {}",
                    tenant_id
                );
                return;
            }
        };

        // Delete namespace (this will cascade delete all resources)
        let namespaces: Api<Namespace> = Api::all(kube_client.clone());
        if let Err(e) = namespaces.delete(&namespace_name, &DeleteParams::default()).await {
            warn!("Failed to delete namespace {}: {}", namespace_name, e);
        } else {
            info!("Deleted namespace {} for tenant {}", namespace_name, tenant_id);
        }
    }

    async fn create_namespace(&self, client: &Client, name: &str) -> Result<()> {
        let namespaces: Api<Namespace> = Api::all(client.clone());

        // Check if namespace already exists
        if namespaces.get(name).await.is_ok() {
            info!("Namespace {} already exists", name);
            return Ok(());
        }

        let namespace = Namespace {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        namespaces
            .create(&PostParams::default(), &namespace)
            .await
            .context("Failed to create namespace")?;

        info!("Created namespace {}", name);
        Ok(())
    }

    async fn deploy_emqx(&self, client: &Client, namespace: &str, tenant_id: i64, tenant_name: &str) -> Result<()> {
        let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let deployment_name = format!("emqx-{}", tenant_id);

        // Check if deployment already exists
        if deployments.get(&deployment_name).await.is_ok() {
            info!("EMQX deployment {} already exists", deployment_name);
            return Ok(());
        }

        // Create EMQX deployment manifest
        let deployment = self.create_emqx_deployment(&deployment_name, tenant_id, tenant_name);

        deployments
            .create(&PostParams::default(), &deployment)
            .await
            .context("Failed to create EMQX deployment")?;

        info!("Created EMQX deployment {}", deployment_name);
        Ok(())
    }

    async fn deploy_postgresql(
        &self,
        client: &Client,
        namespace: &str,
        tenant_id: i64,
        tenant_name: &str,
    ) -> Result<()> {
        let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let deployment_name = format!("postgresql-{}", tenant_id);

        if deployments.get(&deployment_name).await.is_ok() {
            info!("PostgreSQL deployment {} already exists", deployment_name);
            return Ok(());
        }

        let deployment = self.create_postgresql_deployment(&deployment_name, tenant_id, tenant_name);

        deployments
            .create(&PostParams::default(), &deployment)
            .await
            .context("Failed to create PostgreSQL deployment")?;

        info!("Created PostgreSQL deployment {}", deployment_name);
        Ok(())
    }

    async fn deploy_timescaledb(
        &self,
        client: &Client,
        namespace: &str,
        tenant_id: i64,
        tenant_name: &str,
    ) -> Result<()> {
        let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
        let deployment_name = format!("timescaledb-{}", tenant_id);

        if deployments.get(&deployment_name).await.is_ok() {
            info!("TimescaleDB deployment {} already exists", deployment_name);
            return Ok(());
        }

        let deployment = self.create_timescaledb_deployment(&deployment_name, tenant_id, tenant_name);

        deployments
            .create(&PostParams::default(), &deployment)
            .await
            .context("Failed to create TimescaleDB deployment")?;

        info!("Created TimescaleDB deployment {}", deployment_name);
        Ok(())
    }

    fn create_emqx_deployment(&self, name: &str, tenant_id: i64, tenant_name: &str) -> Deployment {
        // Simplified EMQX deployment - in production, use Helm charts or more complete manifests
        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some({
                    let mut labels = BTreeMap::new();
                    labels.insert("app".to_string(), "emqx".to_string());
                    labels.insert("tenant-id".to_string(), tenant_id.to_string());
                    labels.insert("tenant-name".to_string(), tenant_name.to_string());
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(1),
                selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: Some({
                        let mut labels = BTreeMap::new();
                        labels.insert("app".to_string(), "emqx".to_string());
                        labels.insert("tenant-id".to_string(), tenant_id.to_string());
                        labels
                    }),
                    ..Default::default()
                },
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some({
                            let mut labels = BTreeMap::new();
                            labels.insert("app".to_string(), "emqx".to_string());
                            labels.insert("tenant-id".to_string(), tenant_id.to_string());
                            labels
                        }),
                        ..Default::default()
                    }),
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: "emqx".to_string(),
                            image: Some("emqx/emqx:latest".to_string()),
                            ports: Some(vec![
                                k8s_openapi::api::core::v1::ContainerPort {
                                    container_port: 1883,
                                    name: Some("mqtt".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::ContainerPort {
                                    container_port: 8083,
                                    name: Some("http".to_string()),
                                    ..Default::default()
                                },
                            ]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn create_postgresql_deployment(&self, name: &str, tenant_id: i64, tenant_name: &str) -> Deployment {
        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some({
                    let mut labels = BTreeMap::new();
                    labels.insert("app".to_string(), "postgresql".to_string());
                    labels.insert("tenant-id".to_string(), tenant_id.to_string());
                    labels.insert("tenant-name".to_string(), tenant_name.to_string());
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(1),
                selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: Some({
                        let mut labels = BTreeMap::new();
                        labels.insert("app".to_string(), "postgresql".to_string());
                        labels.insert("tenant-id".to_string(), tenant_id.to_string());
                        labels
                    }),
                    ..Default::default()
                },
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some({
                            let mut labels = BTreeMap::new();
                            labels.insert("app".to_string(), "postgresql".to_string());
                            labels.insert("tenant-id".to_string(), tenant_id.to_string());
                            labels
                        }),
                        ..Default::default()
                    }),
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: "postgresql".to_string(),
                            image: Some("postgres:16".to_string()),
                            env: Some(vec![
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_DB".to_string(),
                                    value: Some(format!("sigbot_{}", tenant_id)),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_USER".to_string(),
                                    value: Some("postgres".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("postgresql-secret-{}", tenant_id)),
                                            key: "password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ]),
                            ports: Some(vec![k8s_openapi::api::core::v1::ContainerPort {
                                container_port: 5432,
                                name: Some("postgresql".to_string()),
                                ..Default::default()
                            }]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn create_timescaledb_deployment(&self, name: &str, tenant_id: i64, tenant_name: &str) -> Deployment {
        // TimescaleDB is typically deployed as a PostgreSQL extension
        // For simplicity, we'll use the timescaledb/postgres image
        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some({
                    let mut labels = BTreeMap::new();
                    labels.insert("app".to_string(), "timescaledb".to_string());
                    labels.insert("tenant-id".to_string(), tenant_id.to_string());
                    labels.insert("tenant-name".to_string(), tenant_name.to_string());
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(1),
                selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: Some({
                        let mut labels = BTreeMap::new();
                        labels.insert("app".to_string(), "timescaledb".to_string());
                        labels.insert("tenant-id".to_string(), tenant_id.to_string());
                        labels
                    }),
                    ..Default::default()
                },
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some({
                            let mut labels = BTreeMap::new();
                            labels.insert("app".to_string(), "timescaledb".to_string());
                            labels.insert("tenant-id".to_string(), tenant_id.to_string());
                            labels
                        }),
                        ..Default::default()
                    }),
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: "timescaledb".to_string(),
                            image: Some("timescale/timescaledb:latest-pg16".to_string()),
                            env: Some(vec![
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_DB".to_string(),
                                    value: Some(format!("sigbot_ts_{}", tenant_id)),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_USER".to_string(),
                                    value: Some("postgres".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("timescaledb-secret-{}", tenant_id)),
                                            key: "password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ]),
                            ports: Some(vec![k8s_openapi::api::core::v1::ContainerPort {
                                container_port: 5432,
                                name: Some("postgresql".to_string()),
                                ..Default::default()
                            }]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn startup_datafeed_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let namespace_name = format!("sigbot-tenant-{}", tenant_id);
        let deployment_name = format!("datafeed-{}", tenant_id);

        let guard = self.kube_client.lock().await;
        let kube_client = match guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Kubernetes client not available. Skipping datafeed deployment for tenant {}",
                    tenant_id
                );
                return;
            }
        };

        let deployments: Api<Deployment> = Api::namespaced(kube_client.clone(), &namespace_name);
        if deployments.get(&deployment_name).await.is_ok() {
            info!("Datafeed deployment {} already exists", deployment_name);
            return;
        }

        let deployment = self.create_microservice_deployment(
            &deployment_name,
            "datafeed",
            tenant_id,
            tenant.name.as_deref().unwrap_or("unknown"),
            vec!["datafeed".to_string()],
        );

        if let Err(e) = deployments.create(&PostParams::default(), &deployment).await {
            error!("Failed to create datafeed deployment for tenant {}: {}", tenant_id, e);
        } else {
            info!(
                "Created datafeed deployment {} for tenant {}",
                deployment_name, tenant_id
            );
        }
    }

    async fn shutdown_datafeed_runner(&self, tenant: Arc<Tenant>) {
        self.delete_deployment(tenant.base.id.unwrap_or(0), "datafeed").await;
    }

    async fn startup_strategy_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let namespace_name = format!("sigbot-tenant-{}", tenant_id);
        let deployment_name = format!("strategy-{}", tenant_id);

        let guard = self.kube_client.lock().await;
        let kube_client = match guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Kubernetes client not available. Skipping strategy deployment for tenant {}",
                    tenant_id
                );
                return;
            }
        };

        let deployments: Api<Deployment> = Api::namespaced(kube_client.clone(), &namespace_name);
        if deployments.get(&deployment_name).await.is_ok() {
            info!("Strategy deployment {} already exists", deployment_name);
            return;
        }

        let deployment = self.create_microservice_deployment(
            &deployment_name,
            "strategy",
            tenant_id,
            tenant.name.as_deref().unwrap_or("unknown"),
            vec!["strategy".to_string()],
        );

        if let Err(e) = deployments.create(&PostParams::default(), &deployment).await {
            error!("Failed to create strategy deployment for tenant {}: {}", tenant_id, e);
        } else {
            info!(
                "Created strategy deployment {} for tenant {}",
                deployment_name, tenant_id
            );
        }
    }

    async fn shutdown_strategy_runner(&self, tenant: Arc<Tenant>) {
        self.delete_deployment(tenant.base.id.unwrap_or(0), "strategy").await;
    }

    async fn startup_notification_forwarder(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let namespace_name = format!("sigbot-tenant-{}", tenant_id);
        let deployment_name = format!("notification-{}", tenant_id);

        let guard = self.kube_client.lock().await;
        let kube_client = match guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Kubernetes client not available. Skipping notification deployment for tenant {}",
                    tenant_id
                );
                return;
            }
        };

        let deployments: Api<Deployment> = Api::namespaced(kube_client.clone(), &namespace_name);
        if deployments.get(&deployment_name).await.is_ok() {
            info!("Notification deployment {} already exists", deployment_name);
            return;
        }

        let deployment = self.create_microservice_deployment(
            &deployment_name,
            "notification",
            tenant_id,
            tenant.name.as_deref().unwrap_or("unknown"),
            vec!["notification".to_string()],
        );

        if let Err(e) = deployments.create(&PostParams::default(), &deployment).await {
            error!(
                "Failed to create notification deployment for tenant {}: {}",
                tenant_id, e
            );
        } else {
            info!(
                "Created notification deployment {} for tenant {}",
                deployment_name, tenant_id
            );
        }
    }

    async fn shutdown_notification_forwarder(&self, tenant: Arc<Tenant>) {
        self.delete_deployment(tenant.base.id.unwrap_or(0), "notification")
            .await;
    }

    async fn startup_backtest_runner(&self, tenant: Arc<Tenant>) {
        let tenant_id = tenant.base.id.unwrap_or(0);
        let namespace_name = format!("sigbot-tenant-{}", tenant_id);
        let deployment_name = format!("backtest-{}", tenant_id);

        let guard = self.kube_client.lock().await;
        let kube_client = match guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Kubernetes client not available. Skipping backtest deployment for tenant {}",
                    tenant_id
                );
                return;
            }
        };

        let deployments: Api<Deployment> = Api::namespaced(kube_client.clone(), &namespace_name);
        if deployments.get(&deployment_name).await.is_ok() {
            info!("Backtest deployment {} already exists", deployment_name);
            return;
        }

        let deployment = self.create_microservice_deployment(
            &deployment_name,
            "backtest",
            tenant_id,
            tenant.name.as_deref().unwrap_or("unknown"),
            vec!["backtest".to_string()],
        );

        if let Err(e) = deployments.create(&PostParams::default(), &deployment).await {
            error!("Failed to create backtest deployment for tenant {}: {}", tenant_id, e);
        } else {
            info!(
                "Created backtest deployment {} for tenant {}",
                deployment_name, tenant_id
            );
        }
    }

    async fn shutdown_backtest_runner(&self, tenant: Arc<Tenant>) {
        self.delete_deployment(tenant.base.id.unwrap_or(0), "backtest").await;
    }

    fn create_microservice_deployment(
        &self,
        name: &str,
        component: &str,
        tenant_id: i64,
        tenant_name: &str,
        command: Vec<String>,
    ) -> Deployment {
        let image = std::env::var("SIGBOT_IMAGE").unwrap_or_else(|_| "sigbot:latest".to_string());

        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some({
                    let mut labels = BTreeMap::new();
                    labels.insert("app".to_string(), component.to_string());
                    labels.insert("component".to_string(), component.to_string());
                    labels.insert("tenant-id".to_string(), tenant_id.to_string());
                    labels.insert("tenant-name".to_string(), tenant_name.to_string());
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(1),
                selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: Some({
                        let mut labels = BTreeMap::new();
                        labels.insert("app".to_string(), component.to_string());
                        labels.insert("tenant-id".to_string(), tenant_id.to_string());
                        labels
                    }),
                    ..Default::default()
                },
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some({
                            let mut labels = BTreeMap::new();
                            labels.insert("app".to_string(), component.to_string());
                            labels.insert("tenant-id".to_string(), tenant_id.to_string());
                            labels
                        }),
                        ..Default::default()
                    }),
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: component.to_string(),
                            image: Some(image),
                            command: Some(command),
                            env: Some(vec![k8s_openapi::api::core::v1::EnvVar {
                                name: "TENANT_ID".to_string(),
                                value: Some(tenant_id.to_string()),
                                ..Default::default()
                            }]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn delete_deployment(&self, tenant_id: i64, component: &str) {
        let namespace_name = format!("sigbot-tenant-{}", tenant_id);
        let deployment_name = format!("{}-{}", component, tenant_id);

        let guard = self.kube_client.lock().await;
        let kube_client = match guard.as_ref() {
            Some(client) => client,
            None => {
                warn!(
                    "Kubernetes client not available. Skipping {} deployment deletion for tenant {}",
                    component, tenant_id
                );
                return;
            }
        };

        let deployments: Api<Deployment> = Api::namespaced(kube_client.clone(), &namespace_name);
        if let Err(e) = deployments.delete(&deployment_name, &DeleteParams::default()).await {
            warn!("Failed to delete {} deployment {}: {}", component, deployment_name, e);
        } else {
            info!(
                "Deleted {} deployment {} for tenant {}",
                component, deployment_name, tenant_id
            );
        }
    }
}

#[async_trait]
impl ISigbotDeployer for SigbotKubernetesDeployer {
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

        info!("Starting Kubernetes deployer with cron '{}'", cron);
        let job = Job::new_async(cron, move |_uuid, _lock| {
            let that = this.clone();
            Box::pin(async move {
                info!("{:?} Running Kubernetes deployer ...", chrono::Utc::now());
                that.execute().await;
            })
        })
        .expect("Failed to create Kubernetes deployer job");

        let scheduler = JobScheduler::new_with_channel_size(channel_size)
            .await
            .expect("Failed to create Kubernetes deployer scheduler");
        scheduler.add(job).await.expect("Failed to add Kubernetes deployer job");
        scheduler
            .start()
            .await
            .expect("Failed to start Kubernetes deployer scheduler");

        *self.scheduler.lock().await = Some(scheduler);

        info!(
            "Started Kubernetes deployer with cron '{}', channels '{}'",
            cron, channel_size
        );
    }

    async fn shutdown(&self) {
        info!(
            "Closing Kubernetes deployer with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
        if let Some(mut scheduler) = self.scheduler.lock().await.take() {
            scheduler
                .shutdown()
                .await
                .expect("Failed to shutdown Kubernetes deployer scheduler");
        }
        info!(
            "Closed Kubernetes deployer with cron '{}'",
            self.schedule_cron.as_deref().unwrap_or(Self::DEFAULT_CRON_EXPRESSION)
        );
    }
}

#[cfg(test)]
mod tests {}
