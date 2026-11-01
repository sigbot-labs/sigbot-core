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

use crate::messager::messager::MessagerConfig;
use anyhow::Context;
use anyhow::Error;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

pub mod execution;
pub mod models;
pub mod strategy;

/// Namespace retained exclusively for the frozen embedded Strategy SDK.
#[doc(hidden)]
pub mod runtime {
    pub use super::execution::*;
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct StrategyArgument {
    /// Messager configuration for communication with the strategy runner
    pub messager_config: Arc<MessagerConfig>,
    /// System-level environment variables set during strategy runner pod startup
    pub sys_environment: Option<HashMap<String, String>>,
    /// Execution mode: "STREAMING" (streaming) or "BATCH" (batch processing)
    pub run_mode: String,
    #[serde(default)]
    pub properties: Option<HashMap<String, String>>,
}

impl StrategyArgument {
    pub fn from_json(json: &str) -> Result<Self, Error> {
        serde_json::from_str(json).context("Failed to parse strategy info from JSON")
    }
}

#[cfg(test)]
#[path = "../../tests/strategy/argument_test.rs"]
mod tests;
