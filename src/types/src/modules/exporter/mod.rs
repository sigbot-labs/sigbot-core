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
use anyhow::{Context, Error};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct SigbotExporterManagerArgument {
    pub messager_config: Arc<MessagerConfiguration>,
    pub properties: Option<HashMap<String, String>>,
    pub secrets: Option<HashMap<String, String>>,
}

impl SigbotExporterManagerArgument {
    pub fn from_json(json: &str) -> Result<Self, Error> {
        serde_json::from_str(json).context(format!("Failed to parse exporter manager info from JSON. - {}", json))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum ExporterMgrProvider {
    GOOGLESHEETS,
    KAFKA,
}

impl ExporterMgrProvider {
    pub fn of(provider: &str) -> Result<ExporterMgrProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "GOOGLESHEETS" | "GOOGLE_SHEETS" | "GOOGLE-STREET" => Ok(ExporterMgrProvider::GOOGLESHEETS),
            "KAFKA" => Ok(ExporterMgrProvider::KAFKA),
            _ => Err(anyhow::anyhow!("Unsupported the exporter provider: {}", provider)),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            ExporterMgrProvider::GOOGLESHEETS => "GOOGLESHEETS",
            ExporterMgrProvider::KAFKA => "KAFKA",
        }
    }
}

#[cfg(test)]
mod tests {}
