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

//! Twitter social media data tools for agent use
//!
//! This module provides tools for fetching social media data from Twitter,
//! including tweet search, user information, and trending topics.

use adk_core::{Result as AdkResult, Tool, ToolContext};
use anyhow::Error;
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
        "Search for tweets on Twitter/X based on a query. \
         Returns recent tweets matching the search criteria with metadata."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (e.g., '#Bitcoin', '@elonmusk', 'crypto market')"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of tweets to return (default: 10, max: 100)",
                    "default": 10
                },
                "start_time": {
                    "type": "string",
                    "description": "Start time for search (ISO 8601 format, optional)"
                },
                "end_time": {
                    "type": "string",
                    "description": "End time for search (ISO 8601 format, optional)"
                }
            },
            "required": ["query"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let query = args["query"]
            .as_str()
            .ok_or_else(|| Error::msg("Missing 'query' parameter"))?;
        let max_results = args["max_results"].as_u64().unwrap_or(10);
        let start_time = args["start_time"].as_str();
        let end_time = args["end_time"].as_str();

        info!(
            "TwitterSearchTool: Searching for query='{}', max_results={}",
            query, max_results
        );

        // TODO: Implement actual Twitter API integration
        // For now, return mock data structure
        debug!(
            "TwitterSearchTool: Mock search - query={}, start={:?}, end={:?}",
            query, start_time, end_time
        );

        Ok(json!({
            "query": query,
            "tweets": [],
            "count": 0,
            "max_results": max_results,
            "source": "twitter",
            "note": "Twitter API integration pending - returning mock data"
        }))
    }
}

/// Tool for fetching Twitter user information
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
        "Get information about a Twitter/X user. \
         Returns user profile data including follower count, bio, and verification status."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "username": {
                    "type": "string",
                    "description": "Twitter username (without @, e.g., 'elonmusk')"
                },
                "user_id": {
                    "type": "string",
                    "description": "Twitter user ID (alternative to username)"
                }
            },
            "oneOf": [
                {"required": ["username"]},
                {"required": ["user_id"]}
            ]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let username = args["username"].as_str();
        let user_id = args["user_id"].as_str();

        if username.is_none() && user_id.is_none() {
            return Err(Error::msg("Either 'username' or 'user_id' must be provided").into());
        }

        let identifier = username.or(user_id).unwrap();
        info!("TwitterUserTool: Fetching user info for '{}'", identifier);

        // TODO: Implement actual Twitter API integration
        debug!("TwitterUserTool: Mock user fetch - identifier={}", identifier);

        Ok(json!({
            "username": username.unwrap_or("unknown"),
            "user_id": user_id.unwrap_or("unknown"),
            "followers_count": 0,
            "following_count": 0,
            "verified": false,
            "bio": "",
            "source": "twitter",
            "note": "Twitter API integration pending - returning mock data"
        }))
    }
}

/// Tool for fetching Twitter trending topics
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
        "Get current trending topics on Twitter/X. \
         Returns a list of trending hashtags and topics with their tweet volumes."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "Location for trends (e.g., 'worldwide', 'US', 'UK')",
                    "default": "worldwide"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of trends to return (default: 10)",
                    "default": 10
                }
            }
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let location = args["location"].as_str().unwrap_or("worldwide");
        let limit = args["limit"].as_u64().unwrap_or(10);

        info!(
            "TwitterTrendsTool: Fetching trends for location='{}', limit={}",
            location, limit
        );

        // TODO: Implement actual Twitter API integration
        debug!(
            "TwitterTrendsTool: Mock trends fetch - location={}, limit={}",
            location, limit
        );

        Ok(json!({
            "location": location,
            "trends": [],
            "count": 0,
            "limit": limit,
            "source": "twitter",
            "note": "Twitter API integration pending - returning mock data"
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_metadata() {
        let search_tool = TwitterSearchTool::new();
        assert_eq!(search_tool.name(), "twitter_search");
        assert!(search_tool.parameters_schema().is_some());

        let user_tool = TwitterUserTool::new();
        assert_eq!(user_tool.name(), "twitter_user");
        assert!(user_tool.parameters_schema().is_some());

        let trends_tool = TwitterTrendsTool::new();
        assert_eq!(trends_tool.name(), "twitter_trends");
        assert!(trends_tool.parameters_schema().is_some());
    }
}
