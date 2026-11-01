//! Delegates authentication and policy grants to the official Authguard SDK.
use crate::config::authguard::AuthguardProperties;
use crate::config::authguard::AuthguardProviderKind;
use crate::context::identity::RequestIdentity;
use anyhow::ensure;
use anyhow::Result;
use authguard_adapter_rust::AccessRequest;
use authguard_adapter_rust::GrpcAccessContextResolver;
use authguard_adapter_rust::HeaderAccessContextResolver;
use authguard_adapter_rust::IAccessContextResolver;
use axum::http::HeaderMap;
use axum::http::Method;
use std::sync::Arc;

pub struct AuthguardProvider {
    resolver: Option<Arc<dyn IAccessContextResolver>>,
    config: AuthguardProperties,
}

struct RequestHeaders<'a>(&'a HeaderMap);

impl AccessRequest for RequestHeaders<'_> {
    fn header(&self, name: &str) -> Option<&str> {
        self.0.get(name).and_then(|value| value.to_str().ok())
    }
}

impl AuthguardProvider {
    pub fn from_config(config: &AuthguardProperties) -> Result<Self> {
        let resolver: Arc<dyn IAccessContextResolver> = match config.provider {
            AuthguardProviderKind::SignedHeader => Arc::new(HeaderAccessContextResolver::from_env()?),
            AuthguardProviderKind::Grpc => Arc::new(GrpcAccessContextResolver::from_env()?),
        };
        Ok(Self::with_resolver(config.clone(), resolver))
    }

    pub fn with_resolver(config: AuthguardProperties, resolver: Arc<dyn IAccessContextResolver>) -> Self {
        Self {
            config,
            resolver: Some(resolver),
        }
    }

    /// Workers have no HTTP authentication surface and need not receive gateway signing keys.
    /// An API host explicitly checks this before binding its listener.
    pub fn deny_all(config: AuthguardProperties) -> Self {
        Self { config, resolver: None }
    }
    pub fn is_configured(&self) -> bool {
        self.resolver.is_some()
    }

    pub async fn authenticate(&self, headers: &HeaderMap, method: &Method, path: &str) -> Result<RequestIdentity> {
        let access = self
            .resolver
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Authguard provider unavailable"))?
            .resolve(&RequestHeaders(headers))
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing trusted Authguard context"))?;
        let identity = RequestIdentity::new(access)?;
        let action = if *method == Method::GET || *method == Method::HEAD {
            &self.config.read_action
        } else {
            &self.config.write_action
        };
        ensure!(&identity.access.action == action, "action mismatch");
        ensure!(identity.resource.service == self.config.service, "service mismatch");
        ensure!(
            identity.resource.partition == self.config.partition,
            "partition mismatch"
        );
        ensure!(identity.resource.region == self.config.region, "region mismatch");
        ensure!(
            identity.resource.path_string() == path.trim_start_matches('/'),
            "request resource mismatch"
        );
        if let Some(tenant) = headers.get("x-sigbot-tenant-id") {
            ensure!(tenant.to_str()? == identity.tenant_id(), "tenant mismatch");
        }
        Ok(identity)
    }
}
