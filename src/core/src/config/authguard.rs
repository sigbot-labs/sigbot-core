//! Authguard integration settings. Credentials are supplied through mounted Secrets.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthguardProperties {
    pub provider: AuthguardProviderKind,
    #[serde(alias = "service-tenant-id")]
    pub service_tenant_id: Option<i64>,
    #[serde(alias = "service-space-id")]
    pub service_space_id: Option<i64>,
    #[serde(alias = "service-env")]
    pub service_env: Option<String>,
    #[serde(alias = "service-principal-id")]
    pub service_principal_id: Option<String>,
    pub service: String,
    pub partition: String,
    pub region: String,
    #[serde(alias = "read-action")]
    pub read_action: String,
    #[serde(alias = "write-action")]
    pub write_action: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthguardProviderKind {
    #[default]
    SignedHeader,
    Grpc,
}

impl Default for AuthguardProperties {
    fn default() -> Self {
        Self {
            provider: AuthguardProviderKind::SignedHeader,
            service_tenant_id: None,
            service_space_id: None,
            service_env: None,
            service_principal_id: None,
            service: "sigbot".into(),
            partition: "default".into(),
            region: "global".into(),
            read_action: "sigbot:read".into(),
            write_action: "sigbot:write".into(),
        }
    }
}
