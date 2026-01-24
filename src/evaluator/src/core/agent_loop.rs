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
use common_telemetry::{debug, error, info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Success condition type
#[derive(Clone)]
pub enum SuccessCondition {
    /// Default: result.success == true
    Default,
    /// Custom condition function
    Custom(Arc<dyn Fn(&SigbotAgentResult) -> bool + Send + Sync>),
}

/// Failure condition type
#[derive(Clone)]
pub enum FailureCondition {
    /// Default: result.success == false
    Default,
    /// Custom condition function
    Custom(Arc<dyn Fn(&SigbotAgentResult) -> bool + Send + Sync>),
}

/// Agent execution rule configuration
#[derive(Clone)]
pub struct AgentRule {
    /// Agent index in the loop
    pub agent_index: usize,
    /// Condition to check if agent result is considered success
    pub success_condition: SuccessCondition,
    /// Condition to check if agent result is considered failure (triggers retry)
    pub failure_condition: FailureCondition,
    /// On failure, goto which agent index (None means goto first agent, i.e., restart loop)
    pub goto_on_failure: Option<usize>,
}

impl AgentRule {
    pub fn new(agent_index: usize) -> Self {
        Self {
            agent_index,
            success_condition: SuccessCondition::Default,
            failure_condition: FailureCondition::Default,
            goto_on_failure: None, // Default: goto first agent (restart loop)
        }
    }

    pub fn with_success_condition<F>(mut self, condition: F) -> Self
    where
        F: Fn(&SigbotAgentResult) -> bool + Send + Sync + 'static,
    {
        self.success_condition = SuccessCondition::Custom(Arc::new(condition));
        self
    }

    pub fn with_failure_condition<F>(mut self, condition: F) -> Self
    where
        F: Fn(&SigbotAgentResult) -> bool + Send + Sync + 'static,
    {
        self.failure_condition = FailureCondition::Custom(Arc::new(condition));
        self
    }

    pub fn with_goto_on_failure(mut self, goto_index: Option<usize>) -> Self {
        self.goto_on_failure = goto_index;
        self
    }

    /// Check if result is success according to rule
    pub fn is_success(&self, result: &SigbotAgentResult) -> bool {
        match &self.success_condition {
            SuccessCondition::Default => result.success,
            SuccessCondition::Custom(condition) => condition(result),
        }
    }

    /// Check if result is failure according to rule
    pub fn is_failure(&self, result: &SigbotAgentResult) -> bool {
        match &self.failure_condition {
            FailureCondition::Default => !result.success,
            FailureCondition::Custom(condition) => condition(result),
        }
    }
}

/// LoopAgent executes multiple agents in a retry loop until success or max attempts reached
/// This implements the loop pattern similar to google adk-go's loop agent
pub struct SigbotLoopAgent {
    agents: Vec<Arc<dyn ISigbotAgent>>,
    rules: HashMap<usize, AgentRule>,
    max_retry_attempts: u32,
    retry_delay: Duration,
    /// If true, feed failure reasons from last agent back to first agent on retry
    feedback_enabled: bool,
}

impl SigbotLoopAgent {
    pub fn new(agents: Vec<Arc<dyn ISigbotAgent>>) -> Self {
        let mut rules = HashMap::new();
        // Create default rules for all agents
        for (idx, _) in agents.iter().enumerate() {
            rules.insert(idx, AgentRule::new(idx));
        }

        Self {
            agents,
            rules,
            max_retry_attempts: 3,
            retry_delay: Duration::from_millis(500),
            feedback_enabled: true,
        }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retry_attempts = max_retries;
        self
    }

    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    pub fn with_feedback(mut self, enabled: bool) -> Self {
        self.feedback_enabled = enabled;
        self
    }

    /// Add or update agent rule
    pub fn with_agent_rule(mut self, rule: AgentRule) -> Self {
        self.rules.insert(rule.agent_index, rule);
        self
    }

    /// Configure agent rule using builder pattern
    pub fn configure_agent<F>(mut self, agent_index: usize, config: F) -> Self
    where
        F: FnOnce(AgentRule) -> AgentRule,
    {
        let rule = self
            .rules
            .get(&agent_index)
            .cloned()
            .unwrap_or_else(|| AgentRule::new(agent_index));
        let updated_rule = config(rule);
        self.rules.insert(agent_index, updated_rule);
        self
    }
}

#[async_trait]
impl ISigbotAgent for SigbotLoopAgent {
    fn name(&self) -> &'static str {
        "LoopAgent"
    }

    async fn execute(&self, ctx: &SigbotAgentContext) -> Result<SigbotAgentResult, Error> {
        info!(
            "LoopAgent: Starting loop execution with {} agents, max_retries={}",
            self.agents.len(),
            self.max_retry_attempts
        );

        let mut attempt = 0;
        let mut last_result: Option<SigbotAgentResult> = None;
        let mut start_agent_idx = 0; // Track which agent to start from on retry

        while attempt < self.max_retry_attempts {
            attempt += 1;
            info!(
                "LoopAgent: Attempt {}/{} (starting from agent index {})",
                attempt, self.max_retry_attempts, start_agent_idx
            );

            // Prepare context for this attempt
            let mut current_ctx = ctx.clone();

            // If feedback is enabled and we have a previous failure, feed it back to the starting agent
            if self.feedback_enabled && attempt > 1 {
                if let Some(ref last_result) = last_result {
                    if let Some(error_msg) = &last_result.error {
                        debug!(
                            "LoopAgent: Feeding failure feedback to agent at index {}: {}",
                            start_agent_idx, error_msg
                        );
                        current_ctx
                            .data
                            .insert("loop_feedback".to_string(), Value::String(error_msg.clone()));
                    }
                }
            }

            // Execute agents sequentially according to rules, starting from start_agent_idx
            let mut current_agent_ctx = current_ctx;
            let mut final_result: Option<SigbotAgentResult> = None;
            let mut loop_failed = false;
            let mut goto_index: Option<usize> = None;

            let mut agent_idx = start_agent_idx;
            while agent_idx < self.agents.len() {
                let agent = &self.agents[agent_idx];
                let rule = self
                    .rules
                    .get(&agent_idx)
                    .cloned()
                    .unwrap_or_else(|| AgentRule::new(agent_idx));

                debug!(
                    "LoopAgent: Executing agent {}/{}: {}",
                    agent_idx + 1,
                    self.agents.len(),
                    agent.name()
                );

                match agent.execute(&current_agent_ctx).await {
                    Ok(result) => {
                        // Check success/failure according to rule
                        if rule.is_failure(&result) {
                            warn!(
                                "LoopAgent: Agent {} failed in attempt {} (rule check): {:?}",
                                agent.name(),
                                attempt,
                                result.error
                            );
                            final_result = Some(result.clone());
                            loop_failed = true;

                            // Determine goto index based on rule
                            goto_index = rule.goto_on_failure;
                            break; // Stop executing remaining agents on failure
                        } else if rule.is_success(&result) {
                            debug!("LoopAgent: Agent {} succeeded (rule check)", agent.name());
                            final_result = Some(result.clone());
                            // Update context with result data for next agent
                            current_agent_ctx = current_agent_ctx.with_data(result.data);
                            agent_idx += 1; // Continue to next agent
                        } else {
                            // Neither success nor failure according to rule, treat as success and continue
                            warn!(
                                "LoopAgent: Agent {} result unclear, treating as success: {:?}",
                                agent.name(),
                                result
                            );
                            final_result = Some(result.clone());
                            current_agent_ctx = current_agent_ctx.with_data(result.data);
                            agent_idx += 1;
                        }
                    }
                    Err(e) => {
                        error!(
                            "LoopAgent: Agent {} execution error in attempt {}: {}",
                            agent.name(),
                            attempt,
                            e
                        );
                        return Err(e);
                    }
                }
            }

            // Check if loop succeeded (all agents executed successfully)
            if !loop_failed {
                if let Some(result) = final_result {
                    info!("LoopAgent: All agents succeeded on attempt {}", attempt);
                    return Ok(result);
                }
            }

            // Loop failed, determine retry behavior
            last_result = final_result;
            if attempt < self.max_retry_attempts {
                // Determine where to goto on retry based on rule
                start_agent_idx = goto_index.unwrap_or(0); // Default: restart from first agent
                if start_agent_idx > 0 {
                    warn!(
                        "LoopAgent: Attempt {} failed, retrying from agent index {}...",
                        attempt, start_agent_idx
                    );
                } else {
                    warn!("LoopAgent: Attempt {} failed, retrying from beginning...", attempt);
                }
                sleep(self.retry_delay).await;
                // Continue loop with updated start_agent_idx
            } else {
                warn!("LoopAgent: Max retry attempts reached. Using last result despite failure.");
                // Return the last result even if it failed (for graceful degradation)
                if let Some(ref last_result) = last_result {
                    return Ok(last_result.clone());
                }
            }
        }

        // If we reach here, all attempts failed
        if let Some(last_result) = last_result {
            Ok(last_result)
        } else {
            Err(Error::msg("LoopAgent: All attempts failed and no result was produced"))
        }
    }
}

#[cfg(test)]
mod tests {}
