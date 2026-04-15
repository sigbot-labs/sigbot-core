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

//! Sigbot MCP Server Implementation using rust-mcp-sdk
//!
//! This module implements an MCP Server using the rust-mcp-sdk,
//! exposing Sigbot capabilities (strategies, backtests, market data)
//! via the Model Context Protocol.

use crate::config::McpServerConfig;
use crate::error::{McpError, Result};
use async_trait::async_trait;
use common_telemetry::{error, info, warn};
use rust_mcp_sdk::mcp_server::{server_runtime, McpServerOptions, ServerHandler};
use rust_mcp_sdk::schema::{
    CallToolRequestParams, CallToolResult, Implementation, InitializeResult, ListResourcesResult,
    ListToolsResult, PaginatedRequestParams, ProtocolVersion, ReadResourceRequestParams,
    ReadResourceResult, Resource, RpcError, ServerCapabilities, ServerCapabilitiesResources,
    ServerCapabilitiesTools, TextContent, Tool,
};
use rust_mcp_sdk::{mcp_icon, McpServer, SdkResult, SseTransport, StdioTransport, TransportOptions};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Sigbot MCP Server
///
/// Wraps rust-mcp-sdk's server runtime with Sigbot-specific functionality
pub struct SigbotMcpServer {
    config: McpServerConfig,
    server: Option<Arc<dyn McpServer>>,
    running: Arc<RwLock<bool>>,
}

impl SigbotMcpServer {
    /// Create a new Sigbot MCP Server
    pub fn new(config: McpServerConfig) -> Self {
        Self {
            config,
            server: None,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Get server configuration
    pub fn config(&self) -> &McpServerConfig {
        &self.config
    }

    /// Initialize the MCP server with tools and resources
    pub async fn init(&mut self) -> Result<()> {
        info!(
            "Initializing MCP server: {} v{}",
            self.config.name, self.config.version
        );

        // Build server details and capabilities
        let server_details = InitializeResult {
            server_info: Implementation {
                name: self.config.name.clone(),
                version: self.config.version.clone(),
                title: Some(self.config.name.clone()),
                description: Some("Sigbot MCP Server for trading strategies and market data".into()),
                icons: vec![mcp_icon!(
                    src = "https://sigbot.dev/icon.png",
                    mime_type = "image/png",
                    sizes = ["128x128"],
                    theme = "dark"
                )],
                website_url: Some("https://sigbot.dev".into()),
            },
            capabilities: ServerCapabilities {
                tools: Some(ServerCapabilitiesTools {
                    list_changed: Some(true),
                }),
                resources: Some(ServerCapabilitiesResources {
                    list_changed: Some(true),
                    subscribe: Some(true),
                }),
                ..Default::default()
            },
            meta: None,
            instructions: Some("Sigbot MCP Server - access trading strategies, backtests, and market data".into()),
            protocol_version: ProtocolVersion::V2025_11_25.into(),
        };

        // Create handler
        let handler = SigbotServerHandler;

        // Create transport based on config
        let transport = match self.config.transport.as_str() {
            "stdio" => {
                Arc::new(StdioTransport::new(TransportOptions::default())?)
            }
            "sse" | _ => {
                let addr: std::net::SocketAddr = self.config.sse_bind_address.parse().map_err(|e| {
                    McpError::InvalidRequest(format!("Invalid bind address: {}", e))
                })?;
                Arc::new(SseTransport::new(addr, TransportOptions::default())?)
            }
        };

        // Create server runtime
        let server: Arc<dyn McpServer> = server_runtime::create_server(McpServerOptions {
            server_details,
            transport,
            handler: handler.to_mcp_server_handler(),
            task_store: None,
            client_task_store: None,
            message_observer: None,
        });

        self.server = Some(server);
        Ok(())
    }

    /// Start the MCP server
    pub async fn start(&mut self) -> Result<()> {
        if self.server.is_none() {
            self.init().await?;
        }

        let mut running = self.running.write().await;
        if *running {
            return Err(McpError::Internal("Server is already running".to_string()));
        }

        if let Some(server) = &self.server {
            let server_clone = Arc::clone(server);
            tokio::spawn(async move {
                if let Err(e) = server_clone.start().await {
                    error!("MCP server error: {}", e);
                }
            });

            info!("MCP server started successfully on {}", self.config.sse_bind_address);
        }

        *running = true;
        Ok(())
    }

    /// Stop the MCP server
    pub async fn stop(&mut self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Err(McpError::Internal("Server is not running".to_string()));
        }

        info!("Stopping MCP server...");
        // Note: rust-mcp-sdk doesn't have explicit shutdown, server stops when transport closes
        *running = false;
        info!("MCP server stopped");
        Ok(())
    }

    /// Check if server is running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }
}

/// Sigbot Server Handler - implements MCP request handling
pub struct SigbotServerHandler;

#[async_trait]
impl ServerHandler for SigbotServerHandler {
    /// Handle list tools request
    async fn handle_list_tools_request(
        &self,
        _params: Option<PaginatedRequestParams>,
        _runtime: Arc<dyn McpServer>,
    ) -> std::result::Result<ListToolsResult, RpcError> {
        let tools = vec![
            Tool {
                name: "get_strategies".to_string(),
                description: Some("Get list of all available trading strategies".to_string()),
                input_schema: rust_mcp_sdk::schema::ToolInputSchema::default(),
                ..Default::default()
            },
            Tool {
                name: "run_backtest".to_string(),
                description: Some("Execute a backtest for a strategy".to_string()),
                input_schema: rust_mcp_sdk::schema::ToolInputSchema::default(),
                ..Default::default()
            },
            Tool {
                name: "get_market_data".to_string(),
                description: Some("Get market data for a symbol".to_string()),
                input_schema: rust_mcp_sdk::schema::ToolInputSchema::default(),
                ..Default::default()
            },
            Tool {
                name: "get_position".to_string(),
                description: Some("Get current trading position".to_string()),
                input_schema: rust_mcp_sdk::schema::ToolInputSchema::default(),
                ..Default::default()
            },
        ];

        Ok(ListToolsResult {
            meta: None,
            next_cursor: None,
            tools,
        })
    }

    /// Handle call tool request
    async fn handle_call_tool_request(
        &self,
        params: CallToolRequestParams,
        _runtime: Arc<dyn McpServer>,
    ) -> std::result::Result<CallToolResult, rust_mcp_sdk::schema::schema_utils::CallToolError> {
        use rust_mcp_sdk::schema::schema_utils::CallToolError;

        info!("MCP tool call: {}", params.tool_name);

        let result = match params.tool_name.as_str() {
            "get_strategies" => serde_json::json!({
                "strategies": [
                    {"id": "strategy_1", "name": "Moving Average Crossover", "status": "active"},
                    {"id": "strategy_2", "name": "RSI Mean Reversion", "status": "active"}
                ]
            }),
            "run_backtest" => serde_json::json!({
                "backtest_id": "bt_123456",
                "strategy_id": params
                    .arguments
                    .as_ref()
                    .and_then(|args| args.get("strategy_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown"),
                "status": "running"
            }),
            "get_market_data" => serde_json::json!({
                "symbol": params
                    .arguments
                    .as_ref()
                    .and_then(|args| args.get("symbol"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("BTCUSDT"),
                "price": 50000.0,
                "timestamp": chrono::Utc::now().timestamp_millis()
            }),
            "get_position" => serde_json::json!({
                "account_id": params
                    .arguments
                    .as_ref()
                    .and_then(|args| args.get("account_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default"),
                "positions": []
            }),
            _ => {
                return Err(CallToolError::new(RpcError::method_not_found().with_message(
                    format!("Unknown tool: {}", params.tool_name)
                )));
            }
        };

        Ok(CallToolResult {
            content: vec![rust_mcp_sdk::schema::Content::text(
                serde_json::to_string(&result).map_err(|e| {
                    CallToolError::new(RpcError::internal_error().with_message(e.to_string()))
                })?
            )],
            is_error: None,
            meta: None,
        })
    }

    /// Handle list resources request
    async fn handle_list_resources_request(
        &self,
        _params: Option<PaginatedRequestParams>,
        _runtime: Arc<dyn McpServer>,
    ) -> std::result::Result<ListResourcesResult, RpcError> {
        let resources = vec![
            Resource {
                uri: "sigbot://strategies".to_string(),
                name: "All Strategies".to_string(),
                description: Some("List all available trading strategies".to_string()),
                mime_type: Some("application/json".to_string()),
                ..Default::default()
            },
            Resource {
                uri: "sigbot://backtests".to_string(),
                name: "All Backtests".to_string(),
                description: Some("List all backtest results".to_string()),
                mime_type: Some("application/json".to_string()),
                ..Default::default()
            },
            Resource {
                uri: "sigbot://market-data".to_string(),
                name: "Market Data".to_string(),
                description: Some("Access real-time market data".to_string()),
                mime_type: Some("application/json".to_string()),
                ..Default::default()
            },
            Resource {
                uri: "sigbot://positions".to_string(),
                name: "Positions".to_string(),
                description: Some("View current trading positions".to_string()),
                mime_type: Some("application/json".to_string()),
                ..Default::default()
            },
        ];

        Ok(ListResourcesResult {
            meta: None,
            next_cursor: None,
            resources,
        })
    }

    /// Handle read resource request
    async fn handle_read_resource_request(
        &self,
        params: ReadResourceRequestParams,
        _runtime: Arc<dyn McpServer>,
    ) -> std::result::Result<ReadResourceResult, RpcError> {
        info!("MCP resource read: {}", params.uri);

        let content = match params.uri.as_str() {
            "sigbot://strategies" => serde_json::json!({ "strategies": [] }),
            "sigbot://backtests" => serde_json::json!({ "backtests": [] }),
            "sigbot://market-data" => serde_json::json!({ "data": {} }),
            "sigbot://positions" => serde_json::json!({ "positions": [] }),
            _ => {
                return Err(RpcError::invalid_params().with_message(format!(
                    "Unknown resource: {}",
                    params.uri
                )));
            }
        };

        Ok(ReadResourceResult {
            contents: vec![rust_mcp_sdk::schema::ResourceContents {
                uri: params.uri.clone(),
                mime_type: Some("application/json".to_string()),
                text: Some(serde_json::to_string(&content).map_err(|e| {
                    RpcError::internal_error().with_message(e.to_string())
                })?),
                blob: None,
            }],
            meta: None,
        })
    }
}

impl Drop for SigbotMcpServer {
    fn drop(&mut self) {
        if std::thread::panicking() {
            warn!("MCP server dropped during panic");
        } else {
            info!("MCP server dropped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_creation() {
        let config = McpServerConfig::default();
        let server = SigbotMcpServer::new(config);
        assert_eq!(server.config().name, "sigbot-mcp-server");
    }
}
