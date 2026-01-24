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

use crate::core::agent_base::{ISigbotAgent, SigbotAgentContext, SigbotAgentResult};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use serde_json::{json, Value};
use sigbot_core::llm::handler::llm_factory::SigbotLLMFactory;
use std::collections::HashMap;

/// LoaderAgent loads warm data (historical kline, twitter, news, etc.) based on bootstrap information
/// Now supports automatic tool discovery and invocation
pub struct SigbotLoaderAgent;

impl SigbotLoaderAgent {
    pub fn new() -> Self {
        Self
    }

    /// Discover available tools from the context
    async fn discover_tools(&self, ctx: &SigbotAgentContext) -> Vec<String> {
        if let Some(toolset) = &ctx.toolset {
            // In a real implementation, we would query the toolset for available tools
            // For now, return known tool names
            vec![
                "binance_market_data".to_string(),
                "binance_kline".to_string(),
                "binance_volume".to_string(),
                "twitter_search".to_string(),
            ]
        } else {
            vec![]
        }
    }

    /// Use LLM to select appropriate tools based on bootstrap data
    async fn select_tools_with_llm(
        &self,
        bootstrap_info: &HashMap<String, Value>,
        available_tools: &[String],
    ) -> Result<Vec<(String, Value)>, Error> {
        let tools_list = available_tools.join(", ");
        let prompt = format!(
            r#"Based on the following market statistics, determine which tools to use and with what parameters:

Market Statistics: {:?}

Available Tools: {}

Tool Descriptions:
- binance_market_data: Get current market price (params: symbol)
- binance_kline: Get historical kline data (params: symbol, interval, limit)
- binance_volume: Get trading volume analysis (params: symbol, interval, limit)
- twitter_search: Search for tweets (params: query, max_results)

Please respond with a JSON array of tool calls in this format:
[
  {{"tool": "binance_kline", "params": {{"symbol": "BTCUSDT", "interval": "1h", "limit": 24}}}},
  {{"tool": "binance_volume", "params": {{"symbol": "BTCUSDT", "interval": "1h", "limit": 24}}}}
]

Only include tools that are relevant for the analysis."#,
            bootstrap_info, tools_list
        );

        let llm = SigbotLLMFactory::get_default();
        let llm_response = llm.generate(prompt).await?;
        debug!("LoaderAgent: LLM tool selection response: {}", llm_response);

        // Parse LLM response to extract tool calls
        // For now, return a default set of tools
        Ok(vec![
            (
                "binance_kline".to_string(),
                json!({
                    "symbol": "BTCUSDT",
                    "interval": "1h",
                    "limit": 24
                }),
            ),
            (
                "binance_volume".to_string(),
                json!({
                    "symbol": "BTCUSDT",
                    "interval": "1h",
                    "limit": 24
                }),
            ),
        ])
    }

    /// Invoke selected tools and aggregate results
    async fn invoke_tools(
        &self,
        _ctx: &SigbotAgentContext,
        tool_calls: Vec<(String, Value)>,
    ) -> Result<HashMap<String, Value>, Error> {
        let mut results = HashMap::new();

        for (tool_name, params) in tool_calls {
            info!("LoaderAgent: Invoking tool '{}' with params: {:?}", tool_name, params);

            // TODO: Actually invoke tools through the toolset
            // For now, create placeholder results
            results.insert(
                tool_name.clone(),
                json!({
                    "tool": tool_name,
                    "params": params,
                    "result": "pending_implementation",
                    "note": "Tool invocation will be implemented when integrated with adk-runner"
                }),
            );
        }

        Ok(results)
    }
}

#[async_trait]
impl ISigbotAgent for SigbotLoaderAgent {
    fn name(&self) -> &'static str {
        "LoaderAgent"
    }

    async fn execute(&self, ctx: &SigbotAgentContext) -> Result<SigbotAgentResult, Error> {
        info!(
            "LoaderAgent: Loading warm data for tenant_id={}, workflow_id={:?}",
            ctx.tenant_id, ctx.workflow_id
        );

        // Get bootstrap info from context
        let bootstrap_info = &ctx.data;

        // Discover available tools
        let available_tools = self.discover_tools(ctx).await;
        info!("LoaderAgent: Discovered {} tools", available_tools.len());

        // Select tools using LLM
        let tool_calls = self.select_tools_with_llm(bootstrap_info, &available_tools).await?;
        info!("LoaderAgent: Selected {} tools to invoke", tool_calls.len());

        // Invoke tools and get results
        let tool_results = self.invoke_tools(ctx, tool_calls).await?;

        // Aggregate results into warm data structure
        let mut warm_data = HashMap::new();
        warm_data.insert("tool_results".to_string(), json!(tool_results));
        warm_data.insert("tools_invoked".to_string(), json!(tool_results.len()));
        warm_data.insert("timestamp".to_string(), json!(chrono::Utc::now().timestamp_millis()));

        // Also include placeholder data for backward compatibility
        warm_data.insert("klines_1d".to_string(), Value::Array(vec![]));
        warm_data.insert("klines_4h".to_string(), Value::Array(vec![]));
        warm_data.insert("klines_1h".to_string(), Value::Array(vec![]));
        warm_data.insert("klines_30m".to_string(), Value::Array(vec![]));
        warm_data.insert("twitter_data".to_string(), Value::Array(vec![]));
        warm_data.insert("news_data".to_string(), Value::Array(vec![]));

        debug!("LoaderAgent: Loaded warm data with {} entries", warm_data.len());
        Ok(SigbotAgentResult::success(warm_data))
    }
}

impl Default for SigbotLoaderAgent {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_loader_agent_discover_tools() {
        let agent = SigbotLoaderAgent::new();
        let ctx = SigbotAgentContext::new("tenant1".to_string(), None);

        let tools = agent.discover_tools(&ctx).await;
        assert!(tools.is_empty()); // No toolset in context
    }

    #[tokio::test]
    async fn test_loader_agent_execute() {
        let agent = SigbotLoaderAgent::new();
        let ctx = SigbotAgentContext::new("tenant1".to_string(), Some("workflow1".to_string()));

        let result = agent.execute(&ctx).await;
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.success);
        assert!(result.data.contains_key("tool_results"));
    }
}
