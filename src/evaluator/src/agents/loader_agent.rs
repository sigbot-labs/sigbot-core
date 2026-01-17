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

use crate::core::agent::{ISigbotAgent, SigbotAgentContext, SigbotAgentResult};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use serde_json::Value;
use sigbot_core::llm::handler::llm_factory::SigbotLLMFactory;
use std::collections::HashMap;

/// LoaderAgent loads warm data (historical kline, twitter, news, etc.) based on bootstrap information
pub struct SigbotLoaderAgent;

impl SigbotLoaderAgent {
    pub fn new() -> Self {
        Self
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

        // Build prompt for LLM to decide what data to fetch
        let prompt = format!(
            r#"Based on the following market statistics, determine what historical data should be loaded:
Market Statistics: {:?}

Please analyze and suggest:
1. What time periods should be queried (e.g., 1d, 4h, 1h, 30m klines)
2. What external data sources should be checked (Twitter, TruthSocial news)
3. What specific data points are most relevant

Respond in JSON format with your recommendations."#,
            bootstrap_info
        );

        // Call LLM to get data loading recommendations
        let llm = SigbotLLMFactory::get_default();
        let llm_response = llm.generate(prompt).await?;
        debug!("LoaderAgent: LLM response for data loading: {}", llm_response);

        // TODO: Parse LLM response and actually fetch the data from datafeed/database
        // For now, return placeholder structure
        let mut warm_data = HashMap::new();
        warm_data.insert("klines_1d".to_string(), Value::Array(vec![]));
        warm_data.insert("klines_4h".to_string(), Value::Array(vec![]));
        warm_data.insert("klines_1h".to_string(), Value::Array(vec![]));
        warm_data.insert("klines_30m".to_string(), Value::Array(vec![]));
        warm_data.insert("twitter_data".to_string(), Value::Array(vec![]));
        warm_data.insert("news_data".to_string(), Value::Array(vec![]));

        debug!("LoaderAgent: Loaded warm data: {:?}", warm_data);
        Ok(SigbotAgentResult::success(warm_data))
    }
}

impl Default for SigbotLoaderAgent {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {}
