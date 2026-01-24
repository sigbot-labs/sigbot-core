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
use common_telemetry::{debug, info, warn};
use sigbot_core::llm::handler::llm_factory::SigbotLLMFactory;
use std::collections::HashMap;

/// AuditorAgent performs compliance boundary checks on generated hyperparameters
pub struct SigbotAuditorAgent;

impl SigbotAuditorAgent {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ISigbotAgent for SigbotAuditorAgent {
    fn name(&self) -> &'static str {
        "AuditorAgent"
    }

    async fn execute(&self, ctx: &SigbotAgentContext) -> Result<SigbotAgentResult, Error> {
        info!(
            "AuditorAgent: Auditing hyperparameters for tenant_id={}, workflow_id={:?}",
            ctx.tenant_id, ctx.workflow_id
        );

        // Get hyperparameters from context
        let hyperparameters = &ctx.data;

        // Build prompt for LLM to check compliance
        let prompt = format!(
            r#"You are a risk management auditor. Check if the following hyperparameters comply with risk boundaries:

Hyperparameters: {:?}

Risk Boundaries (examples):
- ETH support cannot be < 2500
- ETH resistance cannot be > 3500
- Stop loss should be reasonable (not too tight or too wide)
- Position sizing should be within acceptable risk limits

Respond in JSON format:
{{
    "is_compliant": true/false,
    "failure_reasons": ["reason1", "reason2"] // empty if compliant
}}"#,
            hyperparameters
        );

        // Call LLM to audit hyperparameters
        let llm = SigbotLLMFactory::get_default();
        let llm_response = llm.generate(prompt).await?;
        debug!("AuditorAgent: LLM response for audit: {}", llm_response);

        // Parse LLM response
        let mut is_compliant = true;
        let mut failure_reasons = Vec::new();

        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&llm_response) {
            if let Some(compliant) = parsed.get("is_compliant").and_then(|v| v.as_bool()) {
                is_compliant = compliant;
            }
            if let Some(reasons) = parsed.get("failure_reasons").and_then(|v| v.as_array()) {
                for reason in reasons {
                    if let Some(reason_str) = reason.as_str() {
                        failure_reasons.push(reason_str.to_string());
                    }
                }
            }
        } else {
            // Fallback: perform basic validation
            // Check support/resistance levels (example for ETH)
            if let Some(support) = hyperparameters.get("support_level") {
                if let Some(num) = support.as_f64() {
                    if num < 2500.0 {
                        failure_reasons.push(format!("Support level {} is too low (minimum: 2500)", num));
                        is_compliant = false;
                    }
                }
            }

            if let Some(resistance) = hyperparameters.get("resistance_level") {
                if let Some(num) = resistance.as_f64() {
                    if num > 3500.0 {
                        failure_reasons.push(format!("Resistance level {} is too high (maximum: 3500)", num));
                        is_compliant = false;
                    }
                }
            }
        }

        if !is_compliant {
            warn!(
                "AuditorAgent: Hyperparameters failed audit. Reasons: {:?}",
                failure_reasons
            );
            Ok(SigbotAgentResult::failure(failure_reasons.join("; ")))
        } else {
            info!("AuditorAgent: Hyperparameters passed audit");
            Ok(SigbotAgentResult::success(HashMap::new())) // Return empty data, compliance is the result
        }
    }
}

impl Default for SigbotAuditorAgent {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {}
