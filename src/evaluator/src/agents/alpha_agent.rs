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
use serde_json::Value;
use sigbot_core::llm::handler::llm_factory::SigbotLLMFactory;
use std::collections::HashMap;

/// AlphaAgent analyzes warm data and calculates hyperparameters (e.g., support/resistance levels)
pub struct SigbotAlphaAgent;

impl SigbotAlphaAgent {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ISigbotAgent for SigbotAlphaAgent {
    fn name(&self) -> &'static str {
        "AlphaAgent"
    }

    async fn execute(&self, ctx: &SigbotAgentContext) -> Result<SigbotAgentResult, Error> {
        info!(
            "AlphaAgent: Calculating hyperparameters for tenant_id={}, workflow_id={:?}",
            ctx.tenant_id, ctx.workflow_id
        );

        // Get warm data from context
        let warm_data = &ctx.data;

        // Check if there's audit feedback from previous attempt
        let mut prompt_prefix = String::new();
        if let Some(audit_feedback) = ctx.data.get("audit_feedback") {
            if let Some(feedback_str) = audit_feedback.as_str() {
                prompt_prefix = format!(
                    "Previous audit feedback: {}\n\nPlease adjust your calculations based on this feedback.\n\n",
                    feedback_str
                );
            }
        }

        // Build prompt for LLM to analyze data and calculate hyperparameters
        let prompt = format!(
            r#"{}
You are an expert quantitative analyst. Analyze the following market data and calculate optimal hyperparameters for trading strategy:

Warm Data: {:?}

Please calculate and provide:
1. Support levels (e.g., ETH support around 2500-2800)
2. Resistance levels (e.g., ETH resistance around 3200-3500)
3. Stop loss levels
4. Take profit levels
5. Position sizing parameters
6. Any other relevant trading parameters

Use mathematical analysis and technical indicators. Respond in JSON format with calculated values."#,
            prompt_prefix, warm_data
        );

        // Call LLM to analyze and calculate hyperparameters
        let llm = SigbotLLMFactory::get_default();
        let llm_response = llm.generate(prompt).await?;
        debug!("AlphaAgent: LLM response for hyperparameters: {}", llm_response);

        // Parse LLM response and extract hyperparameters
        let mut hyperparameters = HashMap::new();

        // Try to parse LLM response as JSON
        if let Ok(parsed) = serde_json::from_str::<HashMap<String, Value>>(&llm_response) {
            hyperparameters = parsed;
        } else {
            // Fallback: create placeholder structure
            hyperparameters.insert("support_level".to_string(), Value::Number(2500.into()));
            hyperparameters.insert("resistance_level".to_string(), Value::Number(3500.into()));
            hyperparameters.insert("stop_loss".to_string(), Value::Number(2400.into()));
            hyperparameters.insert("take_profit".to_string(), Value::Number(3600.into()));
        }

        debug!("AlphaAgent: Calculated hyperparameters: {:?}", hyperparameters);
        Ok(SigbotAgentResult::success(hyperparameters))
    }
}

impl Default for SigbotAlphaAgent {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {}
