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

use crate::modules::exchange::models::trade_market::KlineModel;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyExecutionInput {
    /// Strategy code
    pub code: String,
    /// Execution mode: "STREAMING" (streaming) or "BATCH" (batch processing)
    /// This is a system-level runtime parameter set during strategy runner initialization
    pub run_mode: String,
    /// Strategy execution context (event-driven data for each market data update)
    pub context: StrategyContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyContext {
    /// Strategy input kline data map.
    /// e.g: {"btcusdc_5m": {"close": 100000, "high": 100000, "low": 99000, "open": 100000, "volume": 10000}}
    pub kline_data: Option<HashMap<String, Vec<KlineModel>>>,
    /// Strategy input market data map.  
    /// e.g: {"truthsocial::trump_post": {"2025-10-25T12:54:52.605Z": "..."}, "twitter::elon_post": {"2025-12-14T12:54:52.605Z": "..."}}
    pub market_data: Option<HashMap<String, HashMap<String, String>>>,
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
