// SPDX-License-Identifier: GPL-3.0-only

use super::IMcpApi;
use crate::mcp::authguard::AuthguardMcpAuth;
use crate::mcp::workflow::WorkflowMcpRegistry;
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use axum_server::Handle;
use common_context::IProvider;
use rust_mcp_sdk::error::SdkResult;
use rust_mcp_sdk::mcp_server::{hyper_server, HyperServerOptions, ToMcpServerHandler};
use rust_mcp_sdk::schema::{
    Implementation, InitializeResult, ProtocolVersion, ServerCapabilities, ServerCapabilitiesResources,
    ServerCapabilitiesTools,
};
use sigbot_core::config::config::ApiMcpProperties;
use sigbot_core::context::authguard::AuthguardProvider;
use sigbot_core::context::state::SigbotState;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// MCP protocol outlet. Domain tools are registered only when backed by a real
/// application service; an empty registry is preferable to fabricated data.
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

        let address: SocketAddr = self.config.bind_address.parse().context("invalid MCP bind_address")?;
        let details = InitializeResult {
            server_info: Implementation {
                name: self.config.name.clone(),
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
                resources: Some(ServerCapabilitiesResources {
                    list_changed: Some(false),
                    subscribe: Some(false),
                }),
                ..Default::default()
            },
            meta: None,
            instructions: Some("Only tools backed by registered Sigbot application services are exposed.".to_owned()),
            protocol_version: ProtocolVersion::V2025_11_25.into(),
        };
        let server = hyper_server::create_server(
            details,
            WorkflowMcpRegistry::new(self.app.clone()).to_mcp_server_handler(),
            HyperServerOptions {
                host: address.ip().to_string(),
                port: address.port(),
                custom_streamable_http_endpoint: Some("/mcp".to_owned()),
                sse_support: false,
                health_endpoint: Some("/healthz".to_owned()),
                auth: Some(Arc::new(AuthguardMcpAuth::new(self.authguard.clone()))),
                ..Default::default()
            },
        );
        let handle = server.server_handle();
        let task = tokio::spawn(server.start());
        let mut state = self.state.lock().await;
        ensure!(state.task.is_none(), "MCP API is already running");
        state.handle = Some(handle);
        state.task = Some(task);
        Ok(())
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
