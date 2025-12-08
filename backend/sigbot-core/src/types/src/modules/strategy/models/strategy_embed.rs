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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyExecutionInput {
    /// Strategy code
    pub code: String,
    /// Strategy execution context
    pub context: StrategyContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyContext {
    /// Market Data（JSON format）
    pub market_data: Option<String>,
    /// Strategy parameters
    pub parameters: Option<HashMap<String, String>>,
    /// Other context data
    pub extra_data: Option<HashMap<String, String>>,
}

/// Strategy execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyExecutionResult {
    /// Whether the execution is successful
    pub success: bool,
    /// Return result (JSON string)
    pub result: Option<String>,
    /// Error message
    pub error: Option<String>,
    /// Execution duration (milliseconds)
    pub duration_ms: u64,
}

#[cfg(test)]
mod tests {}
