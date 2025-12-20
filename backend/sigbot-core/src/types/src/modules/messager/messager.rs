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

// Manual impl for decode.
// #[derive(Serialize, Deserialize, Clone, Debug, sqlx::sqlite::FromRow, sqlx::sqlite::Decode)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct MessagerConfiguration {
    pub name: Option<String>,
    pub provider: Option<MessagerProvider>,
    pub configuration: Option<HashMap<String, String>>,
    pub secrets: Option<HashMap<String, String>>,
}

impl Default for MessagerConfiguration {
    fn default() -> Self {
        MessagerConfiguration {
            name: None,
            provider: None,
            configuration: None,
            secrets: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum MessagerProvider {
    LOCAL, // Local queue based (in-memory), for testing standalone mode.
    MQTT,  // MQTT based, for production.
}

impl MessagerProvider {
    pub fn of(provider: &str) -> Result<MessagerProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "LOCAL" => Ok(MessagerProvider::LOCAL),
            "MQTT" => Ok(MessagerProvider::MQTT),
            _ => Err(anyhow::anyhow!("Unsupported the Messager provider: {}", provider)),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            MessagerProvider::LOCAL => "LOCAL",
            MessagerProvider::MQTT => "MQTT",
        }
    }
}

#[cfg(test)]
mod tests {}
