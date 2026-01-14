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

use crate::modules::messager::messager::MessagerConfiguration;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

pub mod models;
pub use models::*;

// --- Log Manager types ---

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct LogManagerArgument {
    pub messager_config: Arc<MessagerConfiguration>,
    pub properties: Option<HashMap<String, String>>,
    pub secrets: Option<HashMap<String, String>>,
}

impl LogManagerArgument {
    pub fn from_json(json: &str) -> Result<Self, anyhow::Error> {
        serde_json::from_str(json).context(format!("Failed to parse log manager info from JSON. - {}", json))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum LogMgrProvider {
    DEFAULT,
}

impl LogMgrProvider {
    pub fn of(provider: &str) -> Result<LogMgrProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "DEFAULT" => Ok(LogMgrProvider::DEFAULT),
            _ => Err(anyhow::anyhow!("Unsupported the log manager provider: {}", provider)),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            LogMgrProvider::DEFAULT => "DEFAULT",
        }
    }
}
