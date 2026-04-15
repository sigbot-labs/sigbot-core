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

//! Sigbot MCP Client Implementation using rust-mcp-sdk
//!
//! This module implements an MCP Client using the rust-mcp-sdk,
//! allowing Sigbot to connect to external MCP services like
//! Binance MCP, Bitget MCP, etc.

use sigbot_core::config::config::EvaluatorMcpServerConfig;
use crate::error::{McpError, Result};
use common_telemetry::{error, info, warn};
use rust_mcp_sdk::client::{McClient, McClientBuilder, ClientOptions};
use rust_mcp_sdk::schema::{CallToolRequest, Tool};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Sigbot MCP Client for connecting to external MCP services
pub struct SigbotMcpClient {
    config: EvaluatorMcpServerConfig,
    client: Option<McClient>,
    discovered_tools: RwLock<Vec<Tool>>,
    connected: RwLock<bool>,
}

impl SigbotMcpClient {
    /// Create a new MCP Client
    pub fn new(config: EvaluatorMcpServerConfig) -> Self {
        Self {
            config,
            client: None,
            discovered_tools: RwLock::new(Vec::new()),
            connected: RwLock::new(false),
        }
    }

    /// Get the service name
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Connect to the external MCP server
    pub async fn connect(&self) -> Result<()> {
        if *self.connected.read().await {
            return Ok(());
        }

        info!(
            "Connecting to external MCP service: {} at {}",
            self.config.name, self.config.url
        );

        // Build client options
        let options = ClientOptions::default()
            .with_name(&self.config.name)
            .with_timeout(std::time::Duration::from_secs(30));

        // Create client based on transport type
        let client = match self.config.transport.as_str() {
            "stdio" => {
                // For STDIO transport, spawn the external process
                McClientBuilder::new(options)
                    .stdio(&self.config.url)
                    .build()
            }
            "sse" | _ => {
                // For SSE transport, connect to the SSE endpoint
                McClientBuilder::new(options)
                    .sse(&self.config.url)
                    .build()
            }
        };

        // Initialize the connection
        let init_result = client
            .initialize()
            .await
            .map_err(|e| McpError::Connection(format!("Failed to initialize: {}", e)))?;

        info!("Connected to MCP server: {:?}", init_result.server_info);

        // Discover available tools
        let tools_result = client
            .list_tools()
            .await
            .map_err(|e| McpError::Protocol(format!("Failed to list tools: {}", e)))?;

        let mut discovered = self.discovered_tools.write().await;
        discovered.clear();

        // Filter tools based on configuration
        for tool in tools_result.tools {
            if self.config.enabled_tools.is_empty()
                || self.config.enabled_tools.contains(&tool.name)
            {
                info!("Discovered tool: {}", tool.name);
                discovered.push(tool);
            }
        }

        *self.connected.write().await = true;
        info!(
            "Connected to {} with {} tools",
            self.config.name,
            discovered.len()
        );

        self.client = Some(client);
        Ok(())
    }

    /// Disconnect from the external MCP server
    pub async fn disconnect(&self) {
        if let Some(client) = &self.client {
            client.shutdown().await;
        }
        *self.connected.write().await = false;
        *self.discovered_tools.write().await = Vec::new();
        info!("Disconnected from external MCP service: {}", self.config.name);
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }

    /// List available tools
    pub async fn list_tools(&self) -> Vec<Tool> {
        self.discovered_tools.read().await.clone()
    }

    /// Call a tool on the remote MCP server
    pub async fn call_tool(&self, tool_name: &str, arguments: Value) -> Result<Value> {
        if !*self.connected.read().await {
            return Err(McpError::Connection("Not connected to MCP server".to_string()));
        }

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| McpError::Connection("Client not initialized".to_string()))?;

        info!("Calling remote tool: {} on {}", tool_name, self.config.name);

        let request = CallToolRequest {
            name: tool_name.to_string(),
            arguments: Some(arguments),
            ..Default::default()
        };

        let result = client
            .call_tool(request)
            .await
            .map_err(|e| McpError::ToolExecution(format!("Tool call failed: {}", e)))?;

        // Check for errors in the result
        if result.is_error == Some(true) {
            let error_msg = result
                .content
                .first()
                .and_then(|c| c.as_text())
                .unwrap_or("Unknown error");
            return Err(McpError::ToolExecution(error_msg.to_string()));
        }

        // Extract result content
        let content = result
            .content
            .first()
            .and_then(|c| c.as_text())
            .unwrap_or("{}");

        let parsed: Value = serde_json::from_str(content)
            .unwrap_or_else(|_| Value::String(content.to_string()));

        Ok(parsed)
    }

    /// Read a resource from the remote MCP server
    pub async fn read_resource(&self, uri: &str) -> Result<String> {
        if !*self.connected.read().await {
            return Err(McpError::Connection("Not connected to MCP server".to_string()));
        }

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| McpError::Connection("Client not initialized".to_string()))?;

        let result = client
            .read_resource(uri)
            .await
            .map_err(|e| McpError::ResourceNotFound(format!("Failed to read resource: {}", e)))?;

        let content = result
            .contents
            .first()
            .and_then(|c| c.text.clone())
            .ok_or_else(|| McpError::ResourceNotFound(format!("No content for: {}", uri)))?;

        Ok(content)
    }

    /// List available resources on the remote server
    pub async fn list_resources(&self) -> Result<Vec<rust_mcp_sdk::schema::Resource>> {
        if !*self.connected.read().await {
            return Err(McpError::Connection("Not connected to MCP server".to_string()));
        }

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| McpError::Connection("Client not initialized".to_string()))?;

        let result = client
            .list_resources()
            .await
            .map_err(|e| McpError::Protocol(format!("Failed to list resources: {}", e)))?;

        Ok(result.resources)
    }
}

/// MCP Client Manager - manages multiple MCP clients
pub struct McpClientManager {
    clients: RwLock<HashMap<String, Arc<SigbotMcpClient>>>,
}

impl McpClientManager {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new MCP client
    pub async fn register(&self, config: EvaluatorMcpServerConfig) -> Result<Arc<SigbotMcpClient>> {
        let client = Arc::new(SigbotMcpClient::new(config.clone()));
        client.connect().await?;

        let mut clients = self.clients.write().await;
        clients.insert(config.name.clone(), client.clone());

        Ok(client)
    }

    /// Get a client by name
    pub async fn get(&self, name: &str) -> Option<Arc<SigbotMcpClient>> {
        self.clients.read().await.get(name).cloned()
    }

    /// List all registered clients
    pub async fn list_clients(&self) -> Vec<String> {
        self.clients.read().await.keys().cloned().collect()
    }

    /// Remove a client
    pub async fn remove(&self, name: &str) -> Option<Arc<SigbotMcpClient>> {
        let mut clients = self.clients.write().await;
        if let Some(client) = clients.remove(name) {
            client.disconnect().await;
            Some(client)
        } else {
            None
        }
    }

    /// Shutdown all clients
    pub async fn shutdown(&self) {
        let clients = self.clients.read().await;
        for client in clients.values() {
            client.disconnect().await;
        }
        info!("Shutdown all MCP clients");
    }
}

impl Default for McpClientManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for McpClientManager {
    fn drop(&mut self) {
        warn!("MCP ClientManager dropped - clients may not be properly disconnected");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = EvaluatorMcpServerConfig {
            name: "test-mcp".to_string(),
            url: "http://localhost:8080/sse".to_string(),
            transport: "sse".to_string(),
            auth_token: None,
            api_key: None,
            api_secret: None,
            enabled_tools: vec![],
            timeout_secs: 30,
        };
        let client = SigbotMcpClient::new(config);
        assert_eq!(client.name(), "test-mcp");
    }
}
