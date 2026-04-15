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

//! Sigbot A2A (Agent-to-Agent) Module
//!
//! ## Architecture
//!
//! This module provides **minimal A2A protocol support** for exposing Sigbot Agents:
//! - `/.well-known/agent.json` - Agent Card discovery
//! - `/a2a` - JSON-RPC A2A endpoint
//! - `/healthz` - Health check
//!
//! ## Design Principles
//!
//! - **High Cohesion**: All A2A-related functionality is encapsulated here
//! - **Low Coupling**: Minimal dependencies, no adk-server (until it's stable)
//! - **Simple**: Basic JSON-RPC A2A implementation
//!
//! ## Note on Full A2A Support
//!
//! For full A2A protocol support (SSE streaming, session management, etc.),
//! use `adk-server::create_app_with_a2a` when it becomes stable.
//! Currently, `adk-server` has compatibility issues with tower-http.
//!
//! ## Configuration
//!
//! ```yaml
//! services:
//!   a2a:
//!     enabled: true
//!     bind-address: "0.0.0.0:8081"
//! ```
//!
//! ## Usage
//!
//! ```rust
//! use sigbot_a2a::{A2AServer, A2AServerBuilder, A2AConfig};
//!
//! // Create server with builder
//! let server = A2AServerBuilder::new()
//!     .with_bind_address("0.0.0.0:8081")
//!     .with_agent_name("evaluator-agent")
//!     .with_agent_description("Strategy evaluation agent")
//!     .build();
//!
//! // Start server
//! server.start().await?;
//! ```
//!
//! ## External Call Example (curl)
//!
//! ```bash
//! # Fetch Agent Card
//! curl http://localhost:8081/.well-known/agent.json
//!
//! # Send A2A message (JSON-RPC)
//! curl -X POST http://localhost:8081/a2a \
//!   -H "Content-Type: application/json" \
//!   -d '{
//!     "jsonrpc": "2.0",
//!     "method": "message/send",
//!     "params": {
//!       "message": {
//!         "role": "user",
//!         "messageId": "msg-1",
//!         "parts": [{"text": "Evaluate strategy-123"}]
//!       }
//!     },
//!     "id": 1
//!   }'
//! ```
//!
//! ## Related Modules
//!
//! - `sigbot-mcp`: For Sigbot **calling external** services
//! - `sigbot-evaluator`: Contains ADK Agent implementations
//! - `adk-core`: Provides `Agent` and `Tool` traits

// Re-export config types from sigbot-core
pub use sigbot_core::config::config::A2AProperties;

// Export server types
mod server;

pub use server::{
    A2AServer,
    A2AServerBuilder,
    A2AConfig,
    A2ARequest,
    A2AResponse,
    A2AState,
    AgentCard,
    AgentCapability,
};
