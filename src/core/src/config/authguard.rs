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
    /// Trusted external OAuth/OIDC authority advertised by protocol discovery.
    pub oauth: Option<OAuthDiscoveryProperties>,
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
            oauth: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthDiscoveryProperties {
    pub issuer: String,
    /// The authority's real OpenID Connect discovery document.
    pub discovery_url: String,
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl OAuthDiscoveryProperties {
    pub fn validate(&self) -> Result<()> {
        let issuer = Self::validate_url(&self.issuer)?;
        anyhow::ensure!(issuer.query().is_none(), "OAuth issuer cannot contain a query");
        Self::validate_url(&self.discovery_url)?;
        anyhow::ensure!(
            self.scopes.len() <= 64
                && self.scopes.iter().all(|scope| !scope.is_empty()
                    && scope.len() <= 128
                    && scope.bytes().all(|c| matches!(c,0x21|0x23..=0x5b|0x5d..=0x7e))),
            "invalid OAuth scopes"
        );
        Ok(())
    }

    pub fn validate_url(value: &str) -> Result<url::Url> {
        let url = url::Url::parse(value)?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        anyhow::ensure!((url.scheme()=="https"||(url.scheme()=="http"&&loopback))&&url.host_str().is_some()&&url.username().is_empty()&&url.password().is_none()&&url.fragment().is_none(),"protocol public/discovery URLs require HTTPS (HTTP is allowed only on loopback), without credentials or fragments");
        Ok(url)
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
