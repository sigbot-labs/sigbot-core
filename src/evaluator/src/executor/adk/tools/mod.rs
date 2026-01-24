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

pub mod binance_tool;
pub mod twitter_tool;

pub use binance_tool::{BinanceKlineTool, BinanceMarketDataTool, BinanceVolumeTool};
pub use twitter_tool::{TwitterSearchTool, TwitterTrendsTool, TwitterUserTool};

use adk_core::Tool;
use adk_tool::BasicToolset;
use std::sync::Arc;

/// Register all default tools for the evaluator agents
pub fn register_default_tools() -> BasicToolset {
    let tools: Vec<Arc<dyn Tool>> = vec![
        // Register Binance tools
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

#[cfg(test)]
mod tests {
    use super::*;
    use adk_core::Toolset; // Import Toolset trait

    #[test]
    fn test_register_default_tools() {
        let toolset = register_default_tools();
        assert_eq!(toolset.name(), "evaluator_tools");
    }
}
