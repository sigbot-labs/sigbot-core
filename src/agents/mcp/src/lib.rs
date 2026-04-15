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

//! Sigbot MCP (Model Context Protocol) Module
//!
//! This module provides MCP Server and Client capabilities for Sigbot
//! using the rust-mcp-sdk (https://github.com/rust-mcp-stack/rust-mcp-sdk)
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         Sigbot MCP Module                                │
//! │                                                                          │
//! │  ┌──────────────────────────── MCP Server ────────────────────────────┐ │
//! │  │  (Wraps RESTful API - High Cohesion, Low Coupling)                │ │
//! │  │                                                                    │ │
//! │  │  ┌─────────────────┐    ┌─────────────────┐    ┌────────────────┐ │ │
//! │  │  │  MCP Protocol   │───▶│  API Adapter    │───▶│  Sigbot Core    │ │ │
//! │  │  │  Layer          │    │  Layer          │    │  RESTful API    │ │ │
//! │  │  │  (SSE/STDIO)    │    │  (Translation)  │    │  (Http/Axum)    │ │ │
//! │  │  └─────────────────┘    └─────────────────┘    └────────────────┘ │ │
//! │  │                                                                    │ │
//! │  │  Configuration: services.api.mcp (enabled, capabilities, etc.)    │ │
//! │  └────────────────────────────────────────────────────────────────────┘ │
//! │                                                                          │
//! │  ┌──────────────────────────── MCP Client ────────────────────────────┐ │
//! │  │  (Evaluator calls external MCP services)                          │ │
//! │  │                                                                    │ │
//! │  │  ┌─────────────────┐    ┌─────────────────┐    ┌────────────────┐ │ │
//! │  │  │  Evaluator      │───▶│  MCP Client     │───▶│  External MCP   │ │ │
//! │  │  │  Strategy       │    │  Pool           │    │  (Binance, etc) │ │ │
//! │  │  └─────────────────┘    └─────────────────┘    └────────────────┘ │ │
//! │  │                                                                    │ │
//! │  │  Configuration: services.evaluator.mcp_servers                    │ │
//! │  └────────────────────────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Configuration
//!
//! ### API MCP Server (expose Sigbot via MCP)
//! ```yaml
//! services:
//!   api:
//!     mcp:
//!       enabled: true
//!       name: "sigbot-api-mcp-server"
//!       transport: "sse"
//!       bind-address: "0.0.0.0:8080"
//!       capabilities:
//!         - "trading"
//!         - "market-data"
//!         - "backtest"
//!         - "strategy"
//!       auth-enabled: true
//! ```
//!
//! ### Evaluator MCP Clients (call external services)
//! ```yaml
//! services:
//!   evaluator:
//!     mcp-servers:
//!       - name: "binance-mcp"
//!         url: "https://binance-mcp.example.com/sse"
//!         transport: "sse"
//!         api-key: "${BINANCE_API_KEY}"
//!         api-secret: "${BINANCE_API_SECRET}"
//!         enabled-tools:
//!           - "get_market_data"
//!           - "get_kline"
//!         timeout-secs: 30
//!       - name: "bitget-mcp"
//!         url: "https://bitget-mcp.example.com/sse"
//!         transport: "sse"
//!         api-key: "${BITGET_API_KEY}"
//!         api-secret: "${BITGET_API_SECRET}"
//!         enabled-tools:
//!           - "get_ticker"
//!         timeout-secs: 30
//! ```

pub mod server;
pub mod client;
pub mod config;
pub mod error;

// Re-export commonly used types from rust-mcp-sdk
pub use rust_mcp_sdk::schema::{
    Tool as McpTool,
    Resource,
    ResourceTemplate,
    Prompt,
    Content,
    ToolResult,
};

// Re-export our own types
pub use error::{McpError, Result};
pub use server::SigbotMcpServer;
pub use client::SigbotMcpClient;

// Re-export config types
pub use config::{
    ApiMcpProperties,
    EvaluatorMcpServerConfig,
    A2AProperties,
    AgentConfig,
    AgentCapabilityConfig,
};
