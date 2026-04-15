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

//! MCP Error types

use thiserror::Error;

/// MCP error types
#[derive(Error, Debug)]
pub enum McpError {
    #[error("MCP protocol error: {0}")]
    Protocol(String),

    #[error("MCP connection error: {0}")]
    Connection(String),

    #[error("MCP authentication error: {0}")]
    Authentication(String),

    #[error("MCP resource not found: {0}")]
    ResourceNotFound(String),

    #[error("MCP tool not found: {0}")]
    ToolNotFound(String),

    #[error("MCP tool execution error: {0}")]
    ToolExecution(String),

    #[error("MCP serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("MCP internal error: {0}")]
    Internal(String),

    #[error("MCP timeout: {0}")]
    Timeout(String),

    #[error("MCP invalid request: {0}")]
    InvalidRequest(String),

    #[error("SDK error: {0}")]
    SdkError(#[from] rust_mcp_sdk::Error),
}

/// Result type alias for MCP operations
pub type Result<T> = std::result::Result<T, McpError>;
