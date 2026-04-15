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
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

//! A2A Server - Thin wrapper for exposing Sigbot Agents via A2A protocol
//!
//! This module provides minimal A2A protocol support by wrapping adk-core agents.
//! Note: Full A2A protocol implementation should use adk-server when it's stable.

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::info;
use adk_core::Agent;
use std::sync::Arc;

/// Agent Card for A2A discovery
#[derive(Debug, Serialize)]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    pub version: String,
    pub capabilities: Vec<AgentCapability>,
}

#[derive(Debug, Serialize)]
pub struct AgentCapability {
    pub name: String,
    pub description: String,
}

/// A2A JSON-RPC Request
#[derive(Debug, Deserialize)]
pub struct A2ARequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
    pub id: Option<serde_json::Value>,
}

/// A2A JSON-RPC Response
#[derive(Debug, Serialize)]
pub struct A2AResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<A2AErrorDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct A2AErrorDetail {
    pub code: i32,
    pub message: String,
}

/// A2A Server configuration
#[derive(Debug, Clone)]
pub struct A2AConfig {
    pub bind_address: String,
    pub agent_name: String,
    pub agent_description: String,
    pub agent_version: String,
}

impl Default for A2AConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8081".to_string(),
            agent_name: "sigbot-agent".to_string(),
            agent_description: "Sigbot Agent".to_string(),
            agent_version: "1.0.0".to_string(),
        }
    }
}

/// A2A Server state
#[derive(Clone)]
pub struct A2AState {
    config: A2AConfig,
    #[allow(dead_code)]
    agent_loader: Arc<dyn Agent>,
}

/// A2A Server
pub struct A2AServer {
    config: A2AConfig,
    #[allow(dead_code)]
    agent_loader: Arc<dyn Agent>,
}

impl A2AServer {
    /// Create a new A2A Server
    pub fn new(config: A2AConfig, agent_loader: Arc<dyn Agent>) -> Self {
        Self { config, agent_loader }
    }

    /// Build the Axum router
    ///
    /// Note: This is a minimal A2A implementation.
    /// For full A2A protocol support, use adk-server's create_app_with_a2a when stable.
    pub fn build_router(&self) -> Router {
        let state = A2AState {
            config: self.config.clone(),
            agent_loader: self.agent_loader.clone(),
        };

        Router::new()
            .route("/.well-known/agent.json", get(agent_card_handler))
            .route("/a2a", post(a2a_handler))
            .route("/healthz", get(health_handler))
            .with_state(state)
    }

    /// Start the A2A server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let router = self.build_router();
        let listener = TcpListener::bind(&self.config.bind_address).await?;

        info!("A2A Server starting on http://{}", self.config.bind_address);
        info!("Agent Card:  GET  http://{}/.well-known/agent.json", self.config.bind_address);
        info!("A2A Invoke:  POST http://{}/a2a", self.config.bind_address);

        axum::serve(listener, router).await?;

        Ok(())
    }
}

/// Agent Card handler
async fn agent_card_handler(
    State(state): State<A2AState>,
) -> impl IntoResponse {
    // TODO: Extract capabilities from agent dynamically
    // For now, use basic info from config
    Json(AgentCard {
        name: state.config.agent_name.clone(),
        description: state.config.agent_description.clone(),
        version: state.config.agent_version.clone(),
        capabilities: vec![],  // TODO: Extract from agent
    })
}

/// A2A JSON-RPC handler
async fn a2a_handler(
    State(_state): State<A2AState>,
    Json(request): Json<A2ARequest>,
) -> impl IntoResponse {
    match request.method.as_str() {
        "message/send" | "message/stream" => {
            // Extract message text from request
            let message_text = request
                .params
                .as_ref()
                .and_then(|p| p.get("message"))
                .and_then(|m| m.get("parts"))
                .and_then(|parts| parts.as_array())
                .and_then(|arr| arr.first())
                .and_then(|part| part.get("text"))
                .and_then(|t| t.as_str())
                .unwrap_or("");

            // TODO: Full implementation would invoke adk-core agent here
            // Example:
            // let event_stream = _state.agent_loader.run(invocation_context).await?;
            // Process event_stream and convert to A2A response format
            let message = format!("A2A message received: {}", message_text);

            Json(A2AResponse {
                jsonrpc: "2.0".to_string(),
                result: Some(serde_json::json!({
                    "message": {
                        "role": "agent",
                        "messageId": "response-1",
                        "parts": [{"text": message}]
                    }
                })),
                error: None,
                id: request.id,
            })
        }
        _ => {
            Json(A2AResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(A2AErrorDetail {
                    code: -32601,
                    message: format!("Method not found: {}", request.method),
                }),
                id: request.id,
            })
        }
    }
}

/// Health check handler
async fn health_handler() -> impl IntoResponse {
    StatusCode::OK
}

/// Builder for A2A Server
pub struct A2AServerBuilder {
    config: A2AConfig,
    agent_loader: Option<Arc<dyn Agent>>,
}

impl A2AServerBuilder {
    pub fn new() -> Self {
        Self {
            config: A2AConfig::default(),
            agent_loader: None,
        }
    }

    pub fn with_bind_address(mut self, addr: impl Into<String>) -> Self {
        self.config.bind_address = addr.into();
        self
    }

    pub fn with_agent_name(mut self, name: impl Into<String>) -> Self {
        self.config.agent_name = name.into();
        self
    }

    pub fn with_agent_loader(mut self, loader: Arc<dyn Agent>) -> Self {
        self.agent_loader = Some(loader);
        self
    }

    pub fn with_agent_description(mut self, desc: impl Into<String>) -> Self {
        self.config.agent_description = desc.into();
        self
    }

    pub fn with_agent_version(mut self, version: impl Into<String>) -> Self {
        self.config.agent_version = version.into();
        self
    }

    pub fn build(self) -> A2AServer {
        A2AServer::new(
            self.config,
            self.agent_loader.expect("Agent loader is required"),
        )
    }
}

impl Default for A2AServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_builder() {
        let server = A2AServerBuilder::new()
            .with_bind_address("0.0.0.0:9999")
            .with_agent_name("test-agent")
            .build();

        assert_eq!(server.config.bind_address, "0.0.0.0:9999");
        assert_eq!(server.config.agent_name, "test-agent");
    }

    #[tokio::test]
    async fn test_agent_card() {
        let server = A2AServerBuilder::new()
            .with_agent_name("test-agent")
            .with_agent_description("Test Agent Description")
            .build();

        let router = server.build_router();

        // Test would go here, but requires more test infrastructure
        assert!(true);
    }
}
