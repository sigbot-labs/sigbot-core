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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Hyperparameter update event published by evaluator
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SigbotHyperparameterUpdateEvent {
    pub tenant_id: String,
    pub workflow_id: String,
    pub strategy_id: String,
    pub hyperparameters: HashMap<String, serde_json::Value>,
    pub timestamp: i64,
    pub evaluation_id: String,
}

impl SigbotHyperparameterUpdateEvent {
    pub fn new(
        tenant_id: String,
        workflow_id: String,
        strategy_id: String,
        hyperparameters: HashMap<String, serde_json::Value>,
        evaluation_id: String,
    ) -> Self {
        Self {
            tenant_id,
            workflow_id,
            strategy_id,
            hyperparameters,
            timestamp: chrono::Utc::now().timestamp(),
            evaluation_id,
        }
    }
}

/// Market data trigger event for evaluator
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SigbotEvaluatorTriggerEvent {
    pub tenant_id: String,
    pub workflow_id: Option<String>,
    pub trigger_type: EvaluatorTriggerType,
    pub market_data: Option<HashMap<String, serde_json::Value>>,
    pub timestamp: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum EvaluatorTriggerType {
    CRON,
    MarketDataRule,
}

impl SigbotEvaluatorTriggerEvent {
    pub fn new(
        tenant_id: String,
        workflow_id: Option<String>,
        trigger_type: EvaluatorTriggerType,
        market_data: Option<HashMap<String, serde_json::Value>>,
    ) -> Self {
        Self {
            tenant_id,
            workflow_id,
            trigger_type,
            market_data,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

#[cfg(test)]
mod tests {}
