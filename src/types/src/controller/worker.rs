use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkerProvider {
    Docker,
    Kubernetes,
}

impl WorkerProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::Kubernetes => "kubernetes",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Online,
    Draining,
    Offline,
    Error,
}

impl WorkerStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Draining => "draining",
            Self::Offline => "offline",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(tag = "auth_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerAuthConfig {
    DockerAnonymous,
    DockerJwt {
        token_secret: String,
    },
    KubernetesJwt {
        token_secret: String,
        #[serde(default)]
        certificate_authority_secret: Option<String>,
    },
    KubernetesPrivateKey {
        client_certificate_secret: String,
        private_key_secret: String,
        certificate_authority_secret: String,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(tag = "runtime_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerConfiguration {
    Docker {
        network: String,
        environment_file: String,
        #[serde(default)]
        log_volume_prefix: String,
    },
    Kubernetes {
        namespace: String,
        runtime_secret: String,
        runtime_config_map: String,
        service_account: String,
        #[serde(default = "WorkerConfiguration::default_secret_revision")]
        secret_revision: String,
        #[serde(default)]
        log_pvc_prefix: String,
        #[serde(default)]
        egress_cidrs: Vec<String>,
        #[serde(default)]
        egress_ports: Vec<u16>,
    },
}

impl WorkerConfiguration {
    fn default_secret_revision() -> String {
        "1".into()
    }

    pub const fn supports(&self, provider: WorkerProvider) -> bool {
        matches!(
            (self, provider),
            (Self::Docker { .. }, WorkerProvider::Docker) | (Self::Kubernetes { .. }, WorkerProvider::Kubernetes)
        )
    }
}

impl WorkerAuthConfig {
    pub fn secret_keys(&self) -> Vec<&str> {
        match self {
            Self::DockerAnonymous => Vec::new(),
            Self::DockerJwt { token_secret } => vec![token_secret],
            Self::KubernetesJwt {
                token_secret,
                certificate_authority_secret,
            } => std::iter::once(token_secret.as_str())
                .chain(certificate_authority_secret.iter().map(String::as_str))
                .collect(),
            Self::KubernetesPrivateKey {
                client_certificate_secret,
                private_key_secret,
                certificate_authority_secret,
            } => vec![
                client_certificate_secret,
                private_key_secret,
                certificate_authority_secret,
            ],
        }
    }

    pub const fn supports(&self, provider: WorkerProvider) -> bool {
        matches!(
            (self, provider),
            (Self::DockerAnonymous | Self::DockerJwt { .. }, WorkerProvider::Docker)
                | (
                    Self::KubernetesJwt { .. } | Self::KubernetesPrivateKey { .. },
                    WorkerProvider::Kubernetes
                )
        )
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MiddlewareResources {
    pub cpu_millis: u32,
    pub memory_mb: u32,
    pub storage_gb: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MiddlewareDeployment {
    pub image: String,
    pub resources: MiddlewareResources,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantMiddlewareConfiguration {
    pub redis: Option<MiddlewareDeployment>,
    pub pgvector: Option<MiddlewareDeployment>,
    pub timescaledb: Option<MiddlewareDeployment>,
    pub emqx: Option<MiddlewareDeployment>,
}

impl TenantMiddlewareConfiguration {
    pub fn deployments(&self) -> [(&'static str, Option<&MiddlewareDeployment>); 4] {
        [
            ("redis", self.redis.as_ref()),
            ("pgvector", self.pgvector.as_ref()),
            ("timescaledb", self.timescaledb.as_ref()),
            ("emqx", self.emqx.as_ref()),
        ]
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SaveWorker {
    pub id: Option<Uuid>,
    pub env: String,
    pub workflow_id: Option<Uuid>,
    pub name: String,
    pub provider: WorkerProvider,
    pub conn_url: String,
    pub auth_config: WorkerAuthConfig,
    pub configuration: WorkerConfiguration,
    #[serde(default)]
    pub middleware: TenantMiddlewareConfiguration,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default = "SaveWorker::default_priority")]
    pub priority: i32,
    #[serde(default = "SaveWorker::default_capacity")]
    pub max_concurrent_workflows: i32,
}

impl SaveWorker {
    const fn default_priority() -> i32 {
        100
    }

    const fn default_capacity() -> i32 {
        16
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct WorkerInfo {
    pub id: Uuid,
    pub env: String,
    pub tenant_id: Uuid,
    pub workflow_id: Option<Uuid>,
    pub name: String,
    pub provider: WorkerProvider,
    pub conn_url: String,
    pub auth_config: WorkerAuthConfig,
    pub configuration: WorkerConfiguration,
    pub middleware: TenantMiddlewareConfiguration,
    pub labels: BTreeMap<String, String>,
    pub status: WorkerStatus,
    pub priority: i32,
    pub max_concurrent_workflows: i32,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
}
