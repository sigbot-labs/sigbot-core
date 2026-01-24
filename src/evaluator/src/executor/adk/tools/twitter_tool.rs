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

//! Twitter/X tools for agent use
//!
//! This module provides tools for interacting with Twitter/X platform,
//! including searching tweets, getting user profiles, and trending topics.

use adk_core::{Result as AdkResult, Tool, ToolContext};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use serde_json::{json, Value};
use std::sync::Arc;

/// Tool for searching tweets
pub struct TwitterSearchTool;

impl TwitterSearchTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TwitterSearchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TwitterSearchTool {
    fn name(&self) -> &str {
        "twitter_search"
    }

    fn description(&self) -> &str {
        "Search for tweets on Twitter/X matching a query. \
         Returns a list of tweets with content, author, and timestamp."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query keywords, hashtags, or phrases"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of tweets to return (default: 10, max: 100)",
                    "default": 10
                },
                "start_time": {
                    "type": "string",
                    "description": "Start time in ISO 8601 format (YYYY-MM-DDTHH:mm:ssZ)"
                },
                "end_time": {
                    "type": "string",
                    "description": "End time in ISO 8601 format (YYYY-MM-DDTHH:mm:ssZ)"
                }
            },
            "required": ["query"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let query = args["query"]
            .as_str()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing 'query' parameter"))?;
        let max_results = args["max_results"].as_u64().unwrap_or(10) as u32;

        info!(
            "TwitterSearchTool: Searching for query='{}', max_results={}",
            query, max_results
        );

        // In a real implementation, we would use a Twitter client
        // For now, return mock data
        let tweets = vec![
            json!({
                "id": "1234567890",
                "text": format!("Analysis of {} market trends suggests bullish momentum", query),
                "author_id": "9876543210",
                "created_at": "2023-10-25T10:00:00Z"
            }),
            json!({
                "id": "1234567891",
                "text": format!("Breaking news regarding {}", query),
                "author_id": "9876543211",
                "created_at": "2023-10-25T11:00:00Z"
            }),
        ];

        Ok(json!({
            "query": query,
            "count": tweets.len(),
            "tweets": tweets,
            "source": "twitter"
        }))
    }
}

/// Tool for fetching user tweets
pub struct TwitterUserTool;

impl TwitterUserTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TwitterUserTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TwitterUserTool {
    fn name(&self) -> &str {
        "twitter_user"
    }

    fn description(&self) -> &str {
        "Get recent tweets from a specific Twitter user."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "username": {
                    "type": "string",
                    "description": "Twitter username (handle)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of tweets to fetch (default: 10)",
                    "default": 10
                }
            },
            "required": ["username"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let username = args["username"]
            .as_str()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing 'username' parameter"))?;
        let limit = args["limit"].as_u64().unwrap_or(10) as u32;

        info!(
            "TwitterUserTool: Fetching tweets for user='{}', limit={}",
            username, limit
        );

        Ok(json!({
            "username": username,
            "tweets": [],
            "source": "twitter"
        }))
    }
}

/// Tool for fetching trending topics
pub struct TwitterTrendsTool;

impl TwitterTrendsTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TwitterTrendsTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TwitterTrendsTool {
    fn name(&self) -> &str {
        "twitter_trends"
    }

    fn description(&self) -> &str {
        "Get current trending topics on Twitter."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "Location for trends (default: 'Global')",
                    "default": "Global"
                }
            }
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, _args: Value) -> AdkResult<Value> {
        // In a real implementation, we would use a Twitter client
        // For now, return mock data
        Ok(json!({
            "trends": [
                {"name": "#Bitcoin", "volume": 100000},
                {"name": "#Crypto", "volume": 50000},
                {"name": "BullRun", "volume": 20000}
            ],
            "source": "twitter"
        }))
    }
}
