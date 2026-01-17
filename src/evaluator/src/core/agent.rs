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

use anyhow::Error;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// Agent trait - unified interface for all agents
#[async_trait]
pub trait ISigbotAgent: Send + Sync {
    /// Agent name for identification
    fn name(&self) -> &'static str;
    /// Execute the agent with given context
    async fn execute(&self, ctx: &SigbotAgentContext) -> Result<SigbotAgentResult, Error>;
}

/// Agent execution context
#[derive(Clone, Debug)]
pub struct SigbotAgentContext {
    pub tenant_id: String,
    pub workflow_id: Option<String>,
    pub data: HashMap<String, Value>,
}

impl SigbotAgentContext {
    pub fn new(tenant_id: String, workflow_id: Option<String>) -> Self {
        Self {
            tenant_id,
            workflow_id,
            data: HashMap::new(),
        }
    }

    pub fn with_data(mut self, data: HashMap<String, Value>) -> Self {
        self.data = data;
        self
    }
}

/// Agent execution result
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
