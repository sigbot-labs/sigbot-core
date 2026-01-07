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
use chrono::Utc;
use common_telemetry::{error, info, warn};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Namespace, Secret};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::ByteString;
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client, Config,
};
use sigbot_core::config::config::{get_config, DeployMode};
use sigbot_core::context::state::SigbotState;
use sigbot_core::sys::handler::dlock_handler::IDLockHandler;
use sigbot_types::sys::tenant::Tenant;
use sigbot_types::sys::tenant::{
    ComponentConnectionConfig, ComponentInstance, ComponentType, ComponentsConfig, TenantEncryptionKeys,
};
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

        // Create secrets for all middleware components
        if let Err(e) = self.create_secrets(kube_client, &namespace_name, tenant_id).await {
            error!("Failed to create secrets for tenant {}: {}", tenant_id, e);
            // Continue deployment even if secret creation fails (secrets may already exist)
        }

        // Deploy EMQX
        let emqx_deployed = self
            .deploy_emqx(kube_client, &namespace_name, tenant_id, tenant_name)
            .await
            .is_ok();
        if !emqx_deployed {
            error!("Failed to deploy EMQX for tenant {}: {}", tenant_id, tenant_name);
        } else {
            self.save_component_config(
                &tenant,
                ComponentType::Emqx,
                &format!("emqx-{}", tenant_id),
                "emqx",
                1883,
                None, // EMQX doesn't use database
                "admin",
                None, // Password will be retrieved from secret
            )
            .await;
        }

        // Deploy PostgreSQL
        let postgresql_deployed = self
            .deploy_postgresql(kube_client, &namespace_name, tenant_id, tenant_name)
            .await
            .is_ok();
        if !postgresql_deployed {
            error!("Failed to deploy PostgreSQL for tenant {}: {}", tenant_id, tenant_name);
        } else {
            self.save_component_config(
                &tenant,
                ComponentType::Postgresql,
                &format!("postgresql-{}", tenant_id),
                "postgresql",
                5432,
                Some(format!("sigbot_{}", tenant_id)),
                "postgres",
                Some(format!("postgresql-secret-{}", tenant_id)),
            )
            .await;
        }

        // Deploy TimescaleDB
        let timescaledb_deployed = self
            .deploy_timescaledb(kube_client, &namespace_name, tenant_id, tenant_name)
            .await
            .is_ok();
        if !timescaledb_deployed {
            error!("Failed to deploy TimescaleDB for tenant {}: {}", tenant_id, tenant_name);
        } else {
            self.save_component_config(
                &tenant,
                ComponentType::Timescaledb,
                &format!("timescaledb-{}", tenant_id),
                "timescaledb",
                5432,
                Some(format!("sigbot_ts_{}", tenant_id)),
                "postgres",
                Some(format!("timescaledb-secret-{}", tenant_id)),
            )
            .await;
        }

        // Deploy Redis
        let redis_deployed = self
            .deploy_redis(kube_client, &namespace_name, tenant_id, tenant_name)
            .await
            .is_ok();
        if !redis_deployed {
            error!("Failed to deploy Redis for tenant {}: {}", tenant_id, tenant_name);
        } else {
            self.save_component_config(
                &tenant,
                ComponentType::Redis,
                &format!("redis-{}", tenant_id),
                "redis",
                6379,
                None, // Redis doesn't use database
                "default",
                Some(format!("redis-secret-{}", tenant_id)),
            )
            .await;
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

    async fn create_secrets(&self, client: &Client, namespace: &str, tenant_id: i64) -> Result<()> {
        let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

        // Generate secure passwords (in production, use proper password generation)
        // Using tenant_id + timestamp for uniqueness
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let postgresql_password = format!("pg_{}_{}", tenant_id, timestamp);
        let postgresql_postgres_password = format!("pg_postgres_{}_{}", tenant_id, timestamp);
        let postgresql_replication_password = format!("pg_repl_{}_{}", tenant_id, timestamp);
        let timescaledb_password = format!("ts_{}_{}", tenant_id, timestamp);
        let timescaledb_postgres_password = format!("ts_postgres_{}_{}", tenant_id, timestamp);
        let timescaledb_replication_password = format!("ts_repl_{}_{}", tenant_id, timestamp);
        let emqx_password = format!("emqx_{}_{}", tenant_id, timestamp);
        let redis_password = format!("redis_{}_{}", tenant_id, timestamp);

        // Create PostgreSQL secret (Bitnami standard)
        let postgresql_secret_name = format!("postgresql-secret-{}", tenant_id);
        if secrets.get(&postgresql_secret_name).await.is_err() {
            let postgresql_secret = Secret {
                metadata: ObjectMeta {
                    name: Some(postgresql_secret_name.clone()),
                    ..Default::default()
                },
                data: Some({
                    let mut data = BTreeMap::new();
                    // Bitnami PostgreSQL standard fields
                    data.insert(
                        "postgres-password".to_string(),
                        ByteString(postgresql_password.as_bytes().to_vec()),
                    );
                    data.insert(
                        "postgres-postgres-password".to_string(),
                        ByteString(postgresql_postgres_password.as_bytes().to_vec()),
                    );
                    data.insert("postgres-username".to_string(), ByteString(b"postgres".to_vec()));
                    data.insert(
                        "postgres-database".to_string(),
                        ByteString(format!("sigbot_{}", tenant_id).as_bytes().to_vec()),
                    );
                    // Enterprise replication fields
                    data.insert(
                        "postgres-replication-password".to_string(),
                        ByteString(postgresql_replication_password.as_bytes().to_vec()),
                    );
                    data.insert(
                        "postgres-replication-username".to_string(),
                        ByteString(b"replicator".to_vec()),
                    );
                    data
                }),
                ..Default::default()
            };
            secrets
                .create(&PostParams::default(), &postgresql_secret)
                .await
                .context("Failed to create PostgreSQL secret")?;
            info!("Created PostgreSQL secret {}", postgresql_secret_name);
        }

        // Create TimescaleDB secret (Bitnami standard)
        let timescaledb_secret_name = format!("timescaledb-secret-{}", tenant_id);
        if secrets.get(&timescaledb_secret_name).await.is_err() {
            let timescaledb_secret = Secret {
                metadata: ObjectMeta {
                    name: Some(timescaledb_secret_name.clone()),
                    ..Default::default()
                },
                data: Some({
                    let mut data = BTreeMap::new();
                    // Bitnami PostgreSQL/TimescaleDB standard fields
                    data.insert(
                        "postgres-password".to_string(),
                        ByteString(timescaledb_password.as_bytes().to_vec()),
                    );
                    data.insert(
                        "postgres-postgres-password".to_string(),
                        ByteString(timescaledb_postgres_password.as_bytes().to_vec()),
                    );
                    data.insert("postgres-username".to_string(), ByteString(b"postgres".to_vec()));
                    data.insert(
                        "postgres-database".to_string(),
                        ByteString(format!("sigbot_ts_{}", tenant_id).as_bytes().to_vec()),
                    );
                    // Enterprise replication fields
                    data.insert(
                        "postgres-replication-password".to_string(),
                        ByteString(timescaledb_replication_password.as_bytes().to_vec()),
                    );
                    data.insert(
                        "postgres-replication-username".to_string(),
                        ByteString(b"replicator".to_vec()),
                    );
                    data
                }),
                ..Default::default()
            };
            secrets
                .create(&PostParams::default(), &timescaledb_secret)
                .await
                .context("Failed to create TimescaleDB secret")?;
            info!("Created TimescaleDB secret {}", timescaledb_secret_name);
        }

        // Create EMQX secret
        let emqx_secret_name = format!("emqx-secret-{}", tenant_id);
        if secrets.get(&emqx_secret_name).await.is_err() {
            let emqx_secret = Secret {
                metadata: ObjectMeta {
                    name: Some(emqx_secret_name.clone()),
                    ..Default::default()
                },
                data: Some({
                    let mut data = BTreeMap::new();
                    data.insert("password".to_string(), ByteString(emqx_password.as_bytes().to_vec()));
                    data.insert("username".to_string(), ByteString(b"admin".to_vec()));
                    data
                }),
                ..Default::default()
            };
            secrets
                .create(&PostParams::default(), &emqx_secret)
                .await
                .context("Failed to create EMQX secret")?;
            info!("Created EMQX secret {}", emqx_secret_name);
        }

        // Create Redis secret (Bitnami standard)
        let redis_secret_name = format!("redis-secret-{}", tenant_id);
        if secrets.get(&redis_secret_name).await.is_err() {
            let redis_secret = Secret {
                metadata: ObjectMeta {
                    name: Some(redis_secret_name.clone()),
                    ..Default::default()
                },
                data: Some({
                    let mut data = BTreeMap::new();
                    data.insert("password".to_string(), ByteString(redis_password.as_bytes().to_vec()));
                    data
                }),
                ..Default::default()
            };
            secrets
                .create(&PostParams::default(), &redis_secret)
                .await
                .context("Failed to create Redis secret")?;
            info!("Created Redis secret {}", redis_secret_name);
        }

        Ok(())
    }

    /// Save component configuration to tenant.components field with encrypted password
    async fn save_component_config(
        &self,
        tenant: &Tenant,
        component_type: ComponentType,
        instance_name: &str,
        service_name: &str,
        port: u16,
        database: Option<String>,
        username: &str,
        secret_name: Option<String>,
    ) {
        let tenant_id = tenant.base.id.unwrap_or(0);

        // Get tenant's public key for encryption
        let public_key = match tenant.encryption_public_key.as_ref() {
            Some(key) => key,
            None => {
                warn!(
                    "Tenant {} does not have encryption public key. Skipping component config save.",
                    tenant_id
                );
                return;
            }
        };

        // Read password from Kubernetes secret if secret_name is provided
        let encrypted_password = if let Some(ref secret_name_ref) = secret_name {
            let guard = self.kube_client.lock().await;
            if let Some(kube_client) = guard.as_ref() {
                let secrets: Api<Secret> =
                    Api::namespaced(kube_client.clone(), &format!("sigbot-tenant-{}", tenant_id));
                match secrets.get(secret_name_ref.as_str()).await {
                    Ok(secret) => {
                        // Extract password from secret based on component type
                        let password_key = match component_type {
                            ComponentType::Postgresql | ComponentType::Timescaledb => "postgres-password",
                            ComponentType::Redis => "password",
                            ComponentType::Emqx => "password",
                            _ => "password",
                        };

                        if let Some(data) = secret.data {
                            if let Some(password_bytes) = data.get(password_key) {
                                // Decode base64 to get plain password
                                let password = match String::from_utf8(password_bytes.0.clone()) {
                                    Ok(pwd) => pwd,
                                    Err(e) => {
                                        error!("Failed to decode password from secret {}: {}", secret_name_ref, e);
                                        return;
                                    }
                                };

                                // Encrypt password using tenant's public key
                                match TenantEncryptionKeys::encrypt_password(public_key, &password) {
                                    Ok(encrypted) => Some(encrypted),
                                    Err(e) => {
                                        error!("Failed to encrypt password for tenant {}: {}", tenant_id, e);
                                        return;
                                    }
                                }
                            } else {
                                warn!(
                                    "Password key '{}' not found in secret {}",
                                    password_key, secret_name_ref
                                );
                                None
                            }
                        } else {
                            warn!("Secret {} has no data", secret_name_ref);
                            None
                        }
                    }
                    Err(e) => {
                        error!("Failed to read secret {}: {}", secret_name_ref, e);
                        None
                    }
                }
            } else {
                warn!("Kubernetes client not available for reading secret");
                None
            }
        } else {
            None // No password to encrypt
        };

        // Build component instance configuration
        use std::collections::HashMap;
        let mut deployment_metadata = HashMap::new();
        deployment_metadata.insert("namespace".to_string(), format!("sigbot-tenant-{}", tenant_id));
        deployment_metadata.insert("service_name".to_string(), service_name.to_string());
        if let Some(ref secret_name) = secret_name {
            deployment_metadata.insert("secret_name".to_string(), secret_name.clone());
        }

        let config = get_config();
        let mode = match component_type {
            ComponentType::Emqx => config.services.deployer.middleware.emqx.mode,
            ComponentType::Postgresql => config.services.deployer.middleware.postgresql.mode,
            ComponentType::Timescaledb => config.services.deployer.middleware.timescaledb.mode,
            ComponentType::Redis => config.services.deployer.middleware.redis.mode,
            _ => DeployMode::Standalone,
        };

        let component_instance = ComponentInstance {
            component_type: component_type.clone(),
            name: instance_name.to_string(),
            mode: Some(format!("{:?}", mode)),
            replicas: match component_type {
                ComponentType::Emqx => config.services.deployer.middleware.emqx.replicas,
                ComponentType::Postgresql => config.services.deployer.middleware.postgresql.replicas,
                ComponentType::Timescaledb => config.services.deployer.middleware.timescaledb.replicas,
                ComponentType::Redis => config.services.deployer.middleware.redis.replicas,
                _ => None,
            },
            connection: Some(ComponentConnectionConfig {
                host: Some(format!(
                    "{}.sigbot-tenant-{}.svc.cluster.local",
                    service_name, tenant_id
                )),
                port: Some(port),
                database,
                username: Some(username.to_string()),
                encrypted_password,
                params: None,
            }),
            performance: None, // Can be extended with performance parameters
            deployment_metadata: Some(deployment_metadata),
            status: Some("running".to_string()),
            deployed_at: Some(Utc::now().to_rfc3339()),
            updated_at: Some(Utc::now().to_rfc3339()),
        };

        // Update tenant.components field
        let mut components_config = if let Some(components_json) = tenant.components.as_ref() {
            serde_json::from_value::<ComponentsConfig>(components_json.clone())
                .unwrap_or_else(|_| ComponentsConfig::new())
        } else {
            ComponentsConfig::new()
        };

        components_config.upsert_component(component_instance);

        // Save updated tenant with components
        let mut updated_tenant = tenant.clone();
        updated_tenant.components = Some(serde_json::to_value(&components_config).unwrap_or(serde_json::Value::Null));

        // Update tenant in database
        let repo = self.state.tenant_repo.lock().await;
        if let Err(e) = repo.get(&self.state.config).update(updated_tenant).await {
            error!("Failed to update tenant {} components: {}", tenant_id, e);
        } else {
            info!(
                "Saved component configuration for tenant {} component {}",
                tenant_id, instance_name
            );
        }
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

    async fn deploy_redis(&self, client: &Client, namespace: &str, tenant_id: i64, tenant_name: &str) -> Result<()> {
        let config = get_config();
        let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);

        if config.services.deployer.middleware.redis.mode == DeployMode::Cluster {
            // Deploy Redis Cluster
            let deployment_name = format!("redis-cluster-{}", tenant_id);
            if deployments.get(&deployment_name).await.is_ok() {
                info!("Redis Cluster deployment {} already exists", deployment_name);
                return Ok(());
            }

            let deployment = self.create_redis_cluster_deployment(&deployment_name, tenant_id, tenant_name);
            deployments
                .create(&PostParams::default(), &deployment)
                .await
                .context("Failed to create Redis Cluster deployment")?;
            info!("Created Redis Cluster deployment {}", deployment_name);
        } else {
            // Deploy Redis Standalone
            let deployment_name = format!("redis-{}", tenant_id);
            if deployments.get(&deployment_name).await.is_ok() {
                info!("Redis deployment {} already exists", deployment_name);
                return Ok(());
            }

            let deployment = self.create_redis_deployment(&deployment_name, tenant_id, tenant_name);
            deployments
                .create(&PostParams::default(), &deployment)
                .await
                .context("Failed to create Redis deployment")?;
            info!("Created Redis deployment {}", deployment_name);
        }

        Ok(())
    }

    fn create_emqx_deployment(&self, name: &str, tenant_id: i64, tenant_name: &str) -> Deployment {
        let config = get_config();
        let replicas = if config.services.deployer.middleware.emqx.mode == DeployMode::Cluster {
            config.services.deployer.middleware.emqx.replicas.unwrap_or(3) as i32
        } else {
            config.services.deployer.middleware.emqx.replicas.unwrap_or(1) as i32
        };

        // Simplified EMQX deployment - in production, use Helm charts or more complete manifests
        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some({
                    let mut labels = BTreeMap::new();
                    labels.insert("app".to_string(), "emqx".to_string());
                    labels.insert("tenant-id".to_string(), tenant_id.to_string());
                    labels.insert("tenant-name".to_string(), tenant_name.to_string());
                    labels.insert(
                        "deploy-mode".to_string(),
                        format!("{:?}", config.services.deployer.middleware.emqx.mode),
                    );
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(replicas),
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
                            image: Some(config.services.deployer.images.emqx.clone()),
                            env: Some(vec![
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "EMQX_DASHBOARD__DEFAULT_USERNAME".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("emqx-secret-{}", tenant_id)),
                                            key: "username".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "EMQX_DASHBOARD__DEFAULT_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("emqx-secret-{}", tenant_id)),
                                            key: "password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ]),
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
        let config = get_config();
        let replicas = if config.services.deployer.middleware.postgresql.mode == DeployMode::Cluster {
            config.services.deployer.middleware.postgresql.replicas.unwrap_or(3) as i32
        } else {
            config.services.deployer.middleware.postgresql.replicas.unwrap_or(1) as i32
        };

        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some({
                    let mut labels = BTreeMap::new();
                    labels.insert("app".to_string(), "postgresql".to_string());
                    labels.insert("tenant-id".to_string(), tenant_id.to_string());
                    labels.insert("tenant-name".to_string(), tenant_name.to_string());
                    labels.insert(
                        "deploy-mode".to_string(),
                        format!("{:?}", config.services.deployer.middleware.postgresql.mode),
                    );
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(replicas),
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
                            image: Some(config.services.deployer.images.postgresql.clone()),
                            env: Some(vec![
                                // Bitnami PostgreSQL standard environment variables
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_DATABASE".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("postgresql-secret-{}", tenant_id)),
                                            key: "postgres-database".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_USERNAME".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("postgresql-secret-{}", tenant_id)),
                                            key: "postgres-username".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("postgresql-secret-{}", tenant_id)),
                                            key: "postgres-password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: POSTGRESQL_POSTGRES_PASSWORD for postgres superuser
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_POSTGRES_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("postgresql-secret-{}", tenant_id)),
                                            key: "postgres-postgres-password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: Replication configuration (for cluster mode)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_REPLICATION_MODE".to_string(),
                                    value: Some(
                                        if config.services.deployer.middleware.postgresql.mode == DeployMode::Cluster {
                                            "master".to_string()
                                        } else {
                                            "".to_string()
                                        },
                                    ),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_REPLICATION_USER".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("postgresql-secret-{}", tenant_id)),
                                            key: "postgres-replication-username".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_REPLICATION_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("postgresql-secret-{}", tenant_id)),
                                            key: "postgres-replication-password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: Synchronous replication (for high availability)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_SYNCHRONOUS_REPLICATION".to_string(),
                                    value: Some(
                                        if config.services.deployer.middleware.postgresql.mode == DeployMode::Cluster {
                                            "on".to_string()
                                        } else {
                                            "off".to_string()
                                        },
                                    ),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_NUM_SYNCHRONOUS_REPLICAS".to_string(),
                                    value: Some(
                                        if config.services.deployer.middleware.postgresql.mode == DeployMode::Cluster {
                                            "1".to_string()
                                        } else {
                                            "0".to_string()
                                        },
                                    ),
                                    ..Default::default()
                                },
                                // Enterprise: Port configuration
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_MASTER_PORT_NUMBER".to_string(),
                                    value: Some("5432".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_PORT_NUMBER".to_string(),
                                    value: Some("5432".to_string()),
                                    ..Default::default()
                                },
                                // Enterprise: WAL level for replication
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_WAL_LEVEL".to_string(),
                                    value: Some(
                                        if config.services.deployer.middleware.postgresql.mode == DeployMode::Cluster {
                                            "replica".to_string()
                                        } else {
                                            "replica".to_string() // Enable WAL for potential future replication
                                        },
                                    ),
                                    ..Default::default()
                                },
                                // Enterprise: Statement timeout (milliseconds)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_STATEMENT_TIMEOUT".to_string(),
                                    value: Some("60000".to_string()), // 60 seconds
                                    ..Default::default()
                                },
                                // Enterprise: Connection limits
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_USERNAME_CONNECTION_LIMIT".to_string(),
                                    value: Some("100".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_POSTGRES_CONNECTION_LIMIT".to_string(),
                                    value: Some("100".to_string()),
                                    ..Default::default()
                                },
                                // Enterprise: Timezone configuration
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_TIMEZONE".to_string(),
                                    value: Some("Asia/Shanghai".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "TZ".to_string(),
                                    value: Some("Asia/Shanghai".to_string()),
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
        let config = get_config();
        let replicas = if config.services.deployer.middleware.timescaledb.mode == DeployMode::Cluster {
            config.services.deployer.middleware.timescaledb.replicas.unwrap_or(3) as i32
        } else {
            config.services.deployer.middleware.timescaledb.replicas.unwrap_or(1) as i32
        };

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
                    labels.insert(
                        "deploy-mode".to_string(),
                        format!("{:?}", config.services.deployer.middleware.timescaledb.mode),
                    );
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(replicas),
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
                            image: Some(config.services.deployer.images.timescaledb.clone()),
                            env: Some(vec![
                                // Bitnami PostgreSQL/TimescaleDB standard environment variables
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_DATABASE".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("timescaledb-secret-{}", tenant_id)),
                                            key: "postgres-database".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_USERNAME".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("timescaledb-secret-{}", tenant_id)),
                                            key: "postgres-username".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("timescaledb-secret-{}", tenant_id)),
                                            key: "postgres-password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: POSTGRESQL_POSTGRES_PASSWORD for postgres superuser
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_POSTGRES_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("timescaledb-secret-{}", tenant_id)),
                                            key: "postgres-postgres-password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: Replication configuration (for cluster mode)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_REPLICATION_MODE".to_string(),
                                    value: Some(
                                        if config.services.deployer.middleware.timescaledb.mode == DeployMode::Cluster {
                                            "master".to_string()
                                        } else {
                                            "".to_string()
                                        },
                                    ),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_REPLICATION_USER".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("timescaledb-secret-{}", tenant_id)),
                                            key: "postgres-replication-username".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_REPLICATION_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("timescaledb-secret-{}", tenant_id)),
                                            key: "postgres-replication-password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: Synchronous replication (for high availability)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_SYNCHRONOUS_REPLICATION".to_string(),
                                    value: Some(
                                        if config.services.deployer.middleware.timescaledb.mode == DeployMode::Cluster {
                                            "on".to_string()
                                        } else {
                                            "off".to_string()
                                        },
                                    ),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_NUM_SYNCHRONOUS_REPLICAS".to_string(),
                                    value: Some(
                                        if config.services.deployer.middleware.timescaledb.mode == DeployMode::Cluster {
                                            "1".to_string()
                                        } else {
                                            "0".to_string()
                                        },
                                    ),
                                    ..Default::default()
                                },
                                // Enterprise: Port configuration
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_MASTER_PORT_NUMBER".to_string(),
                                    value: Some("5432".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_PORT_NUMBER".to_string(),
                                    value: Some("5432".to_string()),
                                    ..Default::default()
                                },
                                // Enterprise: WAL level for replication
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_WAL_LEVEL".to_string(),
                                    value: Some(
                                        if config.services.deployer.middleware.timescaledb.mode == DeployMode::Cluster {
                                            "replica".to_string()
                                        } else {
                                            "replica".to_string() // Enable WAL for potential future replication
                                        },
                                    ),
                                    ..Default::default()
                                },
                                // Enterprise: Statement timeout (milliseconds)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_STATEMENT_TIMEOUT".to_string(),
                                    value: Some("60000".to_string()), // 60 seconds
                                    ..Default::default()
                                },
                                // Enterprise: Connection limits
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_USERNAME_CONNECTION_LIMIT".to_string(),
                                    value: Some("100".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_POSTGRES_CONNECTION_LIMIT".to_string(),
                                    value: Some("100".to_string()),
                                    ..Default::default()
                                },
                                // Enterprise: Timezone configuration
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRESQL_TIMEZONE".to_string(),
                                    value: Some("Asia/Shanghai".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "TZ".to_string(),
                                    value: Some("Asia/Shanghai".to_string()),
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

    fn create_redis_deployment(&self, name: &str, tenant_id: i64, tenant_name: &str) -> Deployment {
        let config = get_config();

        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some({
                    let mut labels = BTreeMap::new();
                    labels.insert("app".to_string(), "redis".to_string());
                    labels.insert("tenant-id".to_string(), tenant_id.to_string());
                    labels.insert("tenant-name".to_string(), tenant_name.to_string());
                    labels.insert("deploy-mode".to_string(), "standalone".to_string());
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(1),
                selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: Some({
                        let mut labels = BTreeMap::new();
                        labels.insert("app".to_string(), "redis".to_string());
                        labels.insert("tenant-id".to_string(), tenant_id.to_string());
                        labels
                    }),
                    ..Default::default()
                },
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some({
                            let mut labels = BTreeMap::new();
                            labels.insert("app".to_string(), "redis".to_string());
                            labels.insert("tenant-id".to_string(), tenant_id.to_string());
                            labels
                        }),
                        ..Default::default()
                    }),
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: "redis".to_string(),
                            image: Some(config.services.deployer.images.redis.clone()),
                            env: Some(vec![
                                // Bitnami Redis standard environment variables
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("redis-secret-{}", tenant_id)),
                                            key: "password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: AOF persistence
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_AOF_ENABLED".to_string(),
                                    value: Some("yes".to_string()),
                                    ..Default::default()
                                },
                                // Enterprise: RDB snapshot policy
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_RDB_POLICY".to_string(),
                                    value: Some("3600#1 300#100 60#10000".to_string()),
                                    ..Default::default()
                                },
                                // Enterprise: TLS encryption (optional, can be enabled via config)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_TLS_ENABLED".to_string(),
                                    value: Some("no".to_string()),
                                    ..Default::default()
                                },
                            ]),
                            ports: Some(vec![k8s_openapi::api::core::v1::ContainerPort {
                                container_port: 6379,
                                name: Some("redis".to_string()),
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

    fn create_redis_cluster_deployment(&self, name: &str, tenant_id: i64, tenant_name: &str) -> Deployment {
        let config = get_config();
        let replicas = config.services.deployer.middleware.redis.replicas.unwrap_or(6) as i32; // Redis Cluster needs at least 6 nodes (3 masters + 3 replicas)

        // Build REDIS_NODES list: redis-node-0 redis-node-1 ... redis-node-5
        // In Kubernetes, we'll use StatefulSet naming pattern: {name}-0, {name}-1, etc.
        let redis_nodes: Vec<String> = (0..replicas).map(|i| format!("{}-{}", name, i)).collect();
        let redis_nodes_str = redis_nodes.join(" ");

        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some({
                    let mut labels = BTreeMap::new();
                    labels.insert("app".to_string(), "redis-cluster".to_string());
                    labels.insert("tenant-id".to_string(), tenant_id.to_string());
                    labels.insert("tenant-name".to_string(), tenant_name.to_string());
                    labels.insert("deploy-mode".to_string(), "cluster".to_string());
                    labels
                }),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(replicas),
                selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: Some({
                        let mut labels = BTreeMap::new();
                        labels.insert("app".to_string(), "redis-cluster".to_string());
                        labels.insert("tenant-id".to_string(), tenant_id.to_string());
                        labels
                    }),
                    ..Default::default()
                },
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some({
                            let mut labels = BTreeMap::new();
                            labels.insert("app".to_string(), "redis-cluster".to_string());
                            labels.insert("tenant-id".to_string(), tenant_id.to_string());
                            labels
                        }),
                        ..Default::default()
                    }),
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: "redis-cluster".to_string(),
                            image: Some(config.services.deployer.images.redis.clone()),
                            env: Some(vec![
                                // Bitnami Redis Cluster standard environment variables
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_PASSWORD".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("redis-secret-{}", tenant_id)),
                                            key: "password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: REDIS_NODES - list of all cluster nodes (required for all nodes)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_NODES".to_string(),
                                    value: Some(redis_nodes_str.clone()),
                                    ..Default::default()
                                },
                                // Enterprise: REDISCLI_AUTH - for redis-cli authentication (set for all nodes, but only creator uses it)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDISCLI_AUTH".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: Some(format!("redis-secret-{}", tenant_id)),
                                            key: "password".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                // Enterprise: Cluster configuration
                                // Note: According to tested Bitnami Redis Cluster deployment:
                                // - REDIS_CLUSTER_CREATOR=yes should only be set on the last node (redis-node-5)
                                // - REDIS_CLUSTER_REPLICAS=1 should only be set on the last node
                                // - REDISCLI_AUTH should be set on the last node
                                // In Kubernetes Deployment, all pods share the same env vars.
                                // For proper implementation, consider using StatefulSet to set these only on the last pod.
                                // For now, Bitnami image should handle cluster creation intelligently.
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_CLUSTER_CREATOR".to_string(),
                                    value: Some("yes".to_string()), // Set for all, but Bitnami handles cluster creation
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_CLUSTER_REPLICAS".to_string(),
                                    value: Some("1".to_string()), // 1 replica per master
                                    ..Default::default()
                                },
                                // Enterprise: AOF persistence
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_AOF_ENABLED".to_string(),
                                    value: Some("yes".to_string()),
                                    ..Default::default()
                                },
                                // Enterprise: RDB snapshot policy
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_RDB_POLICY".to_string(),
                                    value: Some("3600#1 300#100 60#10000".to_string()),
                                    ..Default::default()
                                },
                                // Enterprise: TLS encryption (optional, can be enabled via config)
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "REDIS_TLS_ENABLED".to_string(),
                                    value: Some("no".to_string()),
                                    ..Default::default()
                                },
                            ]),
                            ports: Some(vec![
                                k8s_openapi::api::core::v1::ContainerPort {
                                    container_port: 6379,
                                    name: Some("redis".to_string()),
                                    ..Default::default()
                                },
                                k8s_openapi::api::core::v1::ContainerPort {
                                    container_port: 16379,
                                    name: Some("redis-cluster".to_string()),
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
        let config = get_config();

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
                            image: Some(config.services.deployer.images.sigbot.clone()),
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
