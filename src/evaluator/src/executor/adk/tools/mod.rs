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

//! Agent tools for the evaluator module
//!
//! This module provides concrete tool implementations for agents to interact
//! with external systems like Binance and Twitter.
//!
//! ## MCP Integration
//! Tools prefixed with `mcp_` can call external MCP services (e.g., Binance MCP, Bitget MCP)
//! when configured. If no MCP client is available, they fall back to mock data.

pub mod binance_tool;
pub mod twitter_tool;
// pub mod mcp_binance_tool;

pub use binance_tool::{BinanceKlineTool, BinanceMarketDataTool, BinanceVolumeTool};
pub use twitter_tool::{TwitterSearchTool, TwitterTrendsTool, TwitterUserTool};
// pub use mcp_binance_tool::{McpBinanceMarketTool, McpBinanceKlineTool};

use adk_core::Tool;
use adk_tool::BasicToolset;
use std::sync::Arc;

/// Register all default tools for the evaluator agents
pub fn register_default_tools() -> BasicToolset {
    let tools: Vec<Arc<dyn Tool>> = vec![
        // Register Binance tools (direct SDK integration)
        Arc::new(BinanceMarketDataTool::new()),
        Arc::new(BinanceKlineTool::new()),
        Arc::new(BinanceVolumeTool::new()),
        // Register Twitter tools
        Arc::new(TwitterSearchTool::new()),
        Arc::new(TwitterUserTool::new()),
        Arc::new(TwitterTrendsTool::new()),
    ];

    BasicToolset::new("evaluator_tools".to_string(), tools)
}

// /// Register tools with MCP client support
// ///
// /// When MCP clients are configured and connected, these tools will
// /// call external MCP services instead of using direct SDK integration.
// pub fn register_tools_with_mcp(
//     mcp_clients: Vec<Arc<sigbot_mcp::client::McpClient>>,
// ) -> BasicToolset {
//     let mut tools: Vec<Arc<dyn Tool>> = vec![
//         // Register MCP Binance tools with client
//         Arc::new(McpBinanceMarketTool::new(mcp_clients.first().cloned())),
//         Arc::new(McpBinanceKlineTool::new(mcp_clients.first().cloned())),
//     ];

//     // Add tools from MCP clients (external MCP services)
//     for client in &mcp_clients {
//         let builder = sigbot_mcp::client::ExternalMcpToolBuilder::new(client.clone());
//         for tool in futures::executor::block_on(builder.build_all()) {
//             tools.push(tool);
//         }
//     }

//     BasicToolset::new("evaluator_tools_with_mcp".to_string(), tools)
// }

#[cfg(test)]
mod tests {
    use super::*;
    use adk_core::Toolset; // Import Toolset trait

    #[test]
    fn test_register_default_tools() {
        let toolset = register_default_tools();
        assert_eq!(toolset.name(), "evaluator_tools");
        assert!(!toolset.tools().is_empty());
    }

    #[test]
    fn test_register_tools_with_mcp() {
        let toolset = register_tools_with_mcp(vec![]);
        assert_eq!(toolset.name(), "evaluator_tools_with_mcp");
    }
}
