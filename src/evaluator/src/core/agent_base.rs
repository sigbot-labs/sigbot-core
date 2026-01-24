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

//! Agent base module using ADK-Rust standard Agent trait
//!
//! This module provides the foundational agent abstractions for the evaluator,
//! using adk-rust standard traits while maintaining sigbot-specific context.

use anyhow::Error;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// Re-export ADK-Rust agent types
pub use adk_core::{Agent, EventStream};
pub use adk_tool::Toolset;

/// Sigbot-specific agent execution context
/// This wraps additional metadata needed for sigbot workflows
#[derive(Clone, Debug)]
pub struct SigbotAgentContext {
    pub tenant_id: String,
    pub workflow_id: Option<String>,
    pub data: HashMap<String, Value>,
    pub toolset: Option<Arc<dyn Toolset>>,
}

impl SigbotAgentContext {
    pub fn new(tenant_id: String, workflow_id: Option<String>) -> Self {
        Self {
            tenant_id,
            workflow_id,
            data: HashMap::new(),
            toolset: None,
        }
    }

    pub fn with_data(mut self, data: HashMap<String, Value>) -> Self {
        self.data = data;
        self
    }

    pub fn with_toolset(mut self, toolset: Arc<dyn Toolset>) -> Self {
        self.toolset = Some(toolset);
        self
    }

    /// Get a value from the context data
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    /// Set a value in the context data
    pub fn set(&mut self, key: String, value: Value) {
        self.data.insert(key, value);
    }
}

/// Sigbot-specific agent execution result
/// This provides a simplified result format for sigbot workflows
#[derive(Clone, Debug)]
pub struct SigbotAgentResult {
    pub success: bool,
    pub data: HashMap<String, Value>,
    pub error: Option<String>,
}

impl SigbotAgentResult {
    pub fn success(data: HashMap<String, Value>) -> Self {
        Self {
            success: true,
            data,
            error: None,
        }
    }

    pub fn failure(error: String) -> Self {
        Self {
            success: false,
            data: HashMap::new(),
            error: Some(error),
        }
    }
}

/// Legacy agent trait for backward compatibility
/// New agents should implement adk_core::Agent directly
#[async_trait]
pub trait ISigbotAgent: Send + Sync {
    /// Agent name for identification
    fn name(&self) -> &'static str;

    /// Execute the agent with given context
    async fn execute(&self, ctx: &SigbotAgentContext) -> Result<SigbotAgentResult, Error>;
}

/// Adapter to convert ISigbotAgent to adk_core::Agent
/// This allows legacy agents to work with the new ADK-based system
pub struct SigbotAgentAdapter<T: ISigbotAgent> {
    agent: Arc<T>,
    context: Arc<tokio::sync::RwLock<SigbotAgentContext>>,
}

impl<T: ISigbotAgent> SigbotAgentAdapter<T> {
    pub fn new(agent: Arc<T>, context: SigbotAgentContext) -> Self {
        Self {
            agent,
            context: Arc::new(tokio::sync::RwLock::new(context)),
        }
    }
}

#[async_trait]
impl<T: ISigbotAgent + 'static> Agent for SigbotAgentAdapter<T> {
    fn name(&self) -> &str {
        self.agent.name()
    }

    fn description(&self) -> &str {
        "Sigbot agent adapter"
    }

    fn sub_agents(&self) -> &[Arc<dyn Agent>] {
        &[]
    }

    async fn run(&self, _ctx: Arc<dyn adk_core::InvocationContext>) -> adk_core::Result<EventStream> {
        use adk_core::Event;
        use async_stream::stream;

        let agent = self.agent.clone();
        let context = self.context.read().await.clone();

        let s = stream! {
            match agent.execute(&context).await {
                Ok(result) => {
                    let event_data = serde_json::to_value(&result.data).unwrap_or(Value::Null);
                    yield Ok(Event::new_with_data("agent_result", event_data));
                }
                Err(e) => {
                    yield Ok(Event::new_with_data("agent_error", serde_json::json!({
                        "error": e.to_string()
                    })));
                }
            }
        };

        Ok(Box::pin(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestAgent;

    #[async_trait]
    impl ISigbotAgent for TestAgent {
        fn name(&self) -> &'static str {
            "test_agent"
        }

        async fn execute(&self, _ctx: &SigbotAgentContext) -> Result<SigbotAgentResult, Error> {
            Ok(SigbotAgentResult::success(HashMap::new()))
        }
    }

    #[test]
    fn test_agent_context() {
        let ctx = SigbotAgentContext::new("tenant1".to_string(), Some("workflow1".to_string()));
        assert_eq!(ctx.tenant_id, "tenant1");
        assert_eq!(ctx.workflow_id, Some("workflow1".to_string()));
    }

    #[test]
    fn test_agent_result() {
        let result = SigbotAgentResult::success(HashMap::new());
        assert!(result.success);
        assert!(result.error.is_none());

        let error_result = SigbotAgentResult::failure("test error".to_string());
        assert!(!error_result.success);
        assert!(error_result.error.is_some());
    }

    #[tokio::test]
    async fn test_agent_adapter() {
        let agent = Arc::new(TestAgent);
        let ctx = SigbotAgentContext::new("tenant1".to_string(), None);
        let adapter = SigbotAgentAdapter::new(agent, ctx);

        assert_eq!(adapter.name(), "test_agent");
        assert_eq!(adapter.description(), "Sigbot agent adapter");
    }
}
