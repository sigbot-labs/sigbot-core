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

//! MCP Client Module using rust-mcp-sdk
//!
//! ## Architecture
//!
//! The MCP Client module provides:
//! - `SigbotMcpClient`: Single MCP client for connecting to external services
//! - `McpClientManager`: Manages pool of MCP clients (for evaluator.mcp_servers config)
//!
//! ## Usage
//!
//! ```rust
//! use sigbot_mcp::client::{SigbotMcpClient, McpClientManager};
//! use sigbot_core::config::config::get_config;
//!
//! // Create client manager from evaluator config
//! let config = get_config();
//! let manager = McpClientManager::new();
//!
//! // Register all configured MCP servers
//! for mcp_server_config in &config.services.evaluator.mcp_servers {
//!     manager.register(mcp_server_config.clone()).await?;
//! }
//!
//! // Get specific client and call tool
//! let binance_client = manager.get("binance-mcp").await;
//! if let Some(client) = binance_client {
//!     let result = client.call_tool("get_kline", json!({ "symbol": "BTCUSDT" })).await?;
//! }
//! ```

pub mod mcp_client;

pub use mcp_client::{SigbotMcpClient, McpClientManager};
