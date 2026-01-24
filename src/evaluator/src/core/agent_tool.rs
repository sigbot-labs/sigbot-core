// SPDX-LICENSE-IDENTIFIER: GNU GENERAL PUBLIC LICENSE Version 3
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

//! Agent tool system using ADK-Rust standard Tool trait
//!
//! This module provides tool infrastructure for the evaluator's multi-agent system,
//! using the standard adk-rust Tool and Toolset traits for compatibility and extensibility.

// Re-export ADK-Rust tool types
pub use adk_core::{Tool, ToolContext, Toolset};
pub use adk_tool::{BasicToolset, FunctionTool};

use std::sync::Arc;

/// Helper to create a basic toolset from a vector of tools
pub fn create_toolset(name: impl Into<String>, tools: Vec<Arc<dyn Tool>>) -> BasicToolset {
    let mut toolset = BasicToolset::new(name);
    for tool in tools {
        toolset.add_tool(tool);
    }
    toolset
}

#[cfg(test)]
mod tests {
    use super::*;
    use adk_core::Result;
    use serde_json::{json, Value};

    async fn mock_tool_fn(_ctx: Arc<dyn ToolContext>, args: Value) -> Result<Value> {
        Ok(json!({
            "result": "success",
            "input": args
        }))
    }

    #[tokio::test]
    async fn test_create_toolset() {
        let tool = Arc::new(FunctionTool::new("mock_tool", "A mock tool for testing", mock_tool_fn));

        let toolset = create_toolset("test_toolset", vec![tool]);
        assert_eq!(toolset.name(), "test_toolset");
    }
}
