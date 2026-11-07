// SPDX-License-Identifier: GPL-3.0-only

use crate::auth::authguard::AuthguardMcpAuth;
use crate::domain::resource::ResourceMcpRegistry;
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use axum_server::Handle;
use common_context::IProvider;
use rust_mcp_sdk::auth::{
    AuthProvider, AuthorizationServerMetadata, OauthProtectedResourceMetadata, RemoteAuthProvider,
};
use rust_mcp_sdk::error::SdkResult;
use rust_mcp_sdk::mcp_server::{hyper_server, HyperServerOptions, ToMcpServerHandler};
use rust_mcp_sdk::schema::{
    Implementation, InitializeResult, ProtocolVersion, ServerCapabilities, ServerCapabilitiesTools,
};
use sigbot_core::config::config::{ApiMcpProperties, OAuthDiscoveryProperties};
use sigbot_core::context::authguard::AuthguardProvider;
use sigbot_core::context::state::SigbotState;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// MCP protocol boundary shared by the HTTP server and its domain registry.
pub trait IMcpApi: IProvider {}

pub struct HttpMcpApi {
    config: ApiMcpProperties,
    authguard: Arc<AuthguardProvider>,
    app: Arc<SigbotState>,
    state: Mutex<HttpMcpState>,
}

struct HttpMcpState {
    handle: Option<Handle>,
    task: Option<JoinHandle<SdkResult<()>>>,
}

impl HttpMcpApi {
    pub fn new(config: ApiMcpProperties, app: SigbotState) -> Self {
        Self {
            config,
            authguard: app.authguard.clone(),
            app: Arc::new(app),
            state: Mutex::new(HttpMcpState {
                handle: None,
                task: None,
            }),
        }
    }

    async fn start(&self) -> Result<()> {
        ensure!(self.config.enabled, "MCP API is disabled");
        ensure!(
            self.config.transport == "streamable_http",
            "MCP transport must be streamable_http"
        );
        ensure!(self.config.auth_enabled, "MCP API requires Authguard");
        ensure!(self.authguard.is_configured(), "Authguard verifier is unavailable");

        let oauth = self
            .app
            .config
            .authguard
            .oauth
            .as_ref()
            .context("MCP requires authguard.oauth issuer and discovery_url")?;
        let auth = Self::oauth_provider(&self.config, oauth, self.authguard.clone()).await?;
        let server = Self::server(
            &self.config,
            ResourceMcpRegistry::new(self.app.clone()).to_mcp_server_handler(),
            Arc::new(auth),
        )?;
        let handle = server.server_handle();
        let mut state = self.state.lock().await;
        ensure!(state.task.is_none(), "MCP API is already running");
        state.handle = Some(handle.clone());
        state.task = Some(tokio::spawn(server.start()));
        drop(state);
        tokio::time::timeout(Duration::from_secs(10), handle.listening())
            .await
            .context("MCP listener startup timed out")?
            .context("MCP listener failed to bind")?;
        Ok(())
    }

    /// SDK-owned RFC 9728/RFC 8414 routes and WWW-Authenticate discovery.
    async fn oauth_provider(
        config: &ApiMcpProperties,
        oauth: &OAuthDiscoveryProperties,
        authguard: Arc<AuthguardProvider>,
    ) -> Result<RemoteAuthProvider> {
        oauth.validate()?;
        let resource_url = OAuthDiscoveryProperties::validate_url(&config.public_url)?;
        ensure!(
            resource_url.query().is_none() && resource_url.path().ends_with("/mcp"),
            "MCP public_url must identify the canonical /mcp resource without a query"
        );
        let metadata = tokio::time::timeout(
            Duration::from_secs(10),
            AuthorizationServerMetadata::from_discovery_url(&oauth.discovery_url),
        )
        .await
        .context("OAuth/OIDC authority discovery timed out")?
        .map_err(|error| anyhow::anyhow!("OAuth/OIDC authority discovery failed: {error}"))?;
        ensure!(
            metadata.issuer == OAuthDiscoveryProperties::validate_url(&oauth.issuer)?,
            "discovered OAuth issuer mismatch"
        );
        OAuthDiscoveryProperties::validate_url(metadata.authorization_endpoint.as_str())?;
        OAuthDiscoveryProperties::validate_url(metadata.token_endpoint.as_str())?;
        if let Some(scopes) = &metadata.scopes_supported {
            ensure!(
                oauth.scopes.iter().all(|scope| scopes.contains(scope)),
                "configured resource scopes are not supported by the authority"
            );
        }
        let scopes = if oauth.scopes.is_empty() {
            metadata.scopes_supported.clone()
        } else {
            Some(oauth.scopes.clone())
        };
        let mut resource =
            OauthProtectedResourceMetadata::new(config.public_url.as_str(), vec![oauth.issuer.as_str()], scopes)?;
        resource.resource_name = Some(config.name.clone());
        resource.bearer_methods_supported = Some(vec!["header".into()]);
        Ok(RemoteAuthProvider::new(
            metadata,
            resource,
            Box::new(AuthguardMcpAuth::new(authguard, resource_url.path().into())),
            None,
        ))
    }

    fn server(
        config: &ApiMcpProperties,
        handler: Arc<dyn rust_mcp_sdk::McpServerHandler>,
        auth: Arc<dyn AuthProvider>,
    ) -> Result<rust_mcp_sdk::mcp_server::HyperServer> {
        let address: SocketAddr = config.bind_address.parse().context("invalid MCP bind_address")?;
        let public_url = OAuthDiscoveryProperties::validate_url(&config.public_url)?;
        let details = InitializeResult {
            server_info: Implementation {
                name: config.name.clone(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                title: Some("Sigbot MCP API".to_owned()),
                description: Some("Authenticated Sigbot Model Context Protocol outlet".to_owned()),
                icons: Vec::new(),
                website_url: None,
            },
            capabilities: ServerCapabilities {
                tools: Some(ServerCapabilitiesTools {
                    list_changed: Some(false),
                }),
                ..Default::default()
            },
            meta: None,
            instructions: Some("Only tools backed by registered Sigbot application services are exposed.".to_owned()),
            protocol_version: ProtocolVersion::V2025_11_25.into(),
        };
        Ok(hyper_server::create_server(
            details,
            handler,
            HyperServerOptions {
                host: address.ip().to_string(),
                port: address.port(),
                custom_streamable_http_endpoint: Some(public_url.path().to_owned()),
                sse_support: false,
                health_endpoint: Some("/healthz".to_owned()),
                auth: Some(auth),
                ..Default::default()
            },
        ))
    }

    async fn stop(&self) -> Result<()> {
        let (handle, task) = {
            let mut state = self.state.lock().await;
            (state.handle.take(), state.task.take())
        };
        if let Some(handle) = handle {
            handle.graceful_shutdown(Some(Duration::from_secs(5)));
        }
        if let Some(task) = task {
            task.await
                .context("MCP server task failed")?
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        Ok(())
    }
}

impl IMcpApi for HttpMcpApi {}

#[async_trait]
impl IProvider for HttpMcpApi {
    async fn init(&self) -> Result<()> {
        self.start().await
    }

    async fn close(&self) -> Result<()> {
        self.stop().await
    }
}

#[cfg(test)]
#[path = "../tests/mcp/discovery_test.rs"]
mod discovery_tests;
