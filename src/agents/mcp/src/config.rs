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

//! MCP Module Configuration
//!
//! Configuration is now centralized in sigbot-core:
//! - API MCP Server: `services.api.mcp`
//! - Evaluator MCP Clients: `services.evaluator.mcp_servers`
//! - A2A Server: `services.a2a`
//!
//! Access via:
//! ```rust
//! use sigbot_core::config::config::get_config;
//! let config = get_config();
//! let api_mcp_enabled = config.services.api.mcp.enabled;
//! let evaluator_mcp_servers = &config.services.evaluator.mcp_servers;
//! let a2a_config = &config.services.a2a;
//! ```

pub use sigbot_core::config::config::{
    ApiMcpProperties,
    EvaluatorMcpServerConfig,
    A2AProperties,
    AgentConfig,
    AgentCapabilityConfig,
};
