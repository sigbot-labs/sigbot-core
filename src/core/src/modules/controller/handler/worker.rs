use crate::config::config::StorageProvider;
use crate::context::identity::{RequestIdentity, ScopeQuery};
use crate::context::state::SigbotState;
use crate::modules::controller::storage::postgres::WorkerPostgresRepository;
use crate::modules::controller::storage::sqlite::WorkerSQLiteRepository;
use anyhow::{ensure, Result};
use sigbot_types::controller::{SaveWorker, WorkerConfiguration, WorkerInfo, WorkerProvider, WorkerStatus};
use uuid::Uuid;

pub struct WorkerHandler;

impl WorkerHandler {
    pub async fn list(state: &SigbotState, identity: &RequestIdentity, scope: &ScopeQuery) -> Result<Vec<WorkerInfo>> {
        let tenant_id = scope.authorize(identity, "worker")?;
        match state.config.storage.provider {
            StorageProvider::Postgres => {
                WorkerPostgresRepository::new(state.postgres_pool().await?)
                    .list(tenant_id, &scope.env)
                    .await
            }
            StorageProvider::SQLite => {
                WorkerSQLiteRepository::new(state.sqlite_pool().await?)
                    .list(tenant_id, &scope.env)
                    .await
            }
        }
    }

    pub async fn save(state: &SigbotState, identity: &RequestIdentity, request: SaveWorker) -> Result<Uuid> {
        identity.require_resource(&request.env, "worker", "all")?;
        let tenant_id = identity.tenant_id().parse()?;
        Self::validate(&request, tenant_id)?;
        match state.config.storage.provider {
            StorageProvider::Postgres => {
                WorkerPostgresRepository::new(state.postgres_pool().await?)
                    .save(tenant_id, identity.principal_id(), &request)
                    .await
            }
            StorageProvider::SQLite => {
                WorkerSQLiteRepository::new(state.sqlite_pool().await?)
                    .save(tenant_id, identity.principal_id(), &request)
                    .await
            }
        }
    }

    pub async fn delete(state: &SigbotState, identity: &RequestIdentity, scope: &ScopeQuery, id: Uuid) -> Result<bool> {
        let tenant_id = scope.authorize(identity, "worker")?;
        ensure!(id.get_version_num() == 7, "invalid worker id");
        match state.config.storage.provider {
            StorageProvider::Postgres => {
                WorkerPostgresRepository::new(state.postgres_pool().await?)
                    .delete(tenant_id, &scope.env, id)
                    .await
            }
            StorageProvider::SQLite => {
                WorkerSQLiteRepository::new(state.sqlite_pool().await?)
                    .delete(tenant_id, &scope.env, id)
                    .await
            }
        }
    }

    pub async fn status(
        state: &SigbotState,
        identity: &RequestIdentity,
        scope: &ScopeQuery,
        id: Uuid,
        status: WorkerStatus,
    ) -> Result<bool> {
        let tenant_id = scope.authorize(identity, "worker")?;
        ensure!(id.get_version_num() == 7, "invalid worker id");
        match state.config.storage.provider {
            StorageProvider::Postgres => {
                WorkerPostgresRepository::new(state.postgres_pool().await?)
                    .status(tenant_id, &scope.env, id, status, identity.principal_id())
                    .await
            }
            StorageProvider::SQLite => {
                WorkerSQLiteRepository::new(state.sqlite_pool().await?)
                    .status(tenant_id, &scope.env, id, status, identity.principal_id())
                    .await
            }
        }
    }

    pub(crate) fn validate(request: &SaveWorker, tenant_id: Uuid) -> Result<()> {
        ensure!(
            !request.name.trim().is_empty() && request.name.len() <= 128,
            "invalid worker name"
        );
        ensure!(
            request.workflow_id.is_none_or(|id| id.get_version_num() == 7)
                && (1..=10_000).contains(&request.max_concurrent_workflows)
                && (0..=10_000).contains(&request.priority),
            "invalid worker scope or capacity"
        );
        ensure!(
            request.auth_config.supports(request.provider),
            "worker authentication/provider mismatch"
        );
        ensure!(
            request.configuration.supports(request.provider),
            "worker runtime configuration/provider mismatch"
        );
        ensure!(
            request.auth_config.secret_keys().iter().all(|key| {
                !key.is_empty()
                    && key.len() <= 128
                    && key
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            }),
            "credentials must reference central configuration secret keys"
        );
        let endpoint = url::Url::parse(&request.conn_url)?;
        ensure!(
            match request.provider {
                WorkerProvider::Docker => matches!(endpoint.scheme(), "unix" | "http" | "https"),
                WorkerProvider::Kubernetes => endpoint.scheme() == "https",
            },
            "invalid worker endpoint scheme"
        );
        ensure!(
            endpoint.username().is_empty() && endpoint.password().is_none() && endpoint.query().is_none(),
            "worker endpoint must not contain credentials"
        );
        match &request.configuration {
            WorkerConfiguration::Docker {
                network,
                environment_file,
                log_volume_prefix,
            } => ensure!(
                !matches!(network.as_str(), "" | "host" | "bridge" | "none")
                    && !environment_file.is_empty()
                    && environment_file.len() <= 128
                    && environment_file.starts_with(&format!("tenant-{tenant_id}-"))
                    && environment_file
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                    && log_volume_prefix.len() <= 128,
                "invalid Docker worker isolation configuration"
            ),
            WorkerConfiguration::Kubernetes {
                namespace,
                runtime_secret,
                runtime_config_map,
                service_account,
                secret_revision,
                egress_cidrs,
                egress_ports,
                ..
            } => ensure!(
                [
                    namespace,
                    runtime_secret,
                    runtime_config_map,
                    service_account,
                    secret_revision
                ]
                .iter()
                .all(|value| !value.is_empty() && value.len() <= 253)
                    && egress_cidrs.len() <= 64
                    && egress_cidrs.iter().all(|cidr| Self::valid_cidr(cidr))
                    && egress_ports.len() <= 64
                    && egress_ports.iter().all(|port| *port > 0),
                "invalid Kubernetes worker isolation configuration"
            ),
        }
        ensure!(
            request.labels.len() <= 64
                && request
                    .labels
                    .iter()
                    .all(|(key, value)| !key.is_empty() && key.len() <= 128 && value.len() <= 256),
            "invalid worker labels"
        );
        for (name, deployment) in request.middleware.deployments() {
            let Some(deployment) = deployment else {
                continue;
            };
            ensure!(
                !deployment.image.is_empty()
                    && deployment.image.len() <= 512
                    && !deployment.image.bytes().any(|byte| byte.is_ascii_whitespace())
                    && !deployment.image.ends_with(":latest"),
                "{name} image must use an explicit immutable version"
            );
            ensure!(
                (50..=64_000).contains(&deployment.resources.cpu_millis)
                    && (64..=262_144).contains(&deployment.resources.memory_mb)
                    && (1..=16_384).contains(&deployment.resources.storage_gb),
                "{name} resource limits are out of bounds"
            );
        }
        Ok(())
    }

    fn valid_cidr(value: &str) -> bool {
        let Some((address, prefix)) = value.split_once('/') else {
            return false;
        };
        let Ok(address) = address.parse::<std::net::IpAddr>() else {
            return false;
        };
        let Ok(prefix) = prefix.parse::<u8>() else {
            return false;
        };
        prefix <= if address.is_ipv4() { 32 } else { 128 }
    }
}
