//! Authguard integration settings. Credentials are supplied through mounted Secrets.
use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use sigbot_types::messager::topic::MessageScope;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthguardProperties {
    pub provider: AuthguardProviderKind,
    pub service_tenant_id: Option<Uuid>,
    pub service_env: Option<String>,
    pub service_principal_id: Option<String>,
    pub service: String,
    pub partition: String,
    pub region: String,
    pub read_action: String,
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

impl AuthguardProperties {
    pub fn message_scope(&self) -> Result<MessageScope> {
        let scope = MessageScope {
            env: self.service_env.clone().context("service environment required")?,
            tenant_id: self.service_tenant_id.context("service tenant required")?,
        };
        scope.validate()?;
        Ok(scope)
    }
}
