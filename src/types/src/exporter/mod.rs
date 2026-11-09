// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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

pub mod request;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum ExporterProvider {
    #[serde(rename = "GOOGLESHEETS")]
    GoogleSheets,
    #[serde(rename = "KAFKA")]
    Kafka,
}

impl ExporterProvider {
    pub fn of(provider: &str) -> Result<ExporterProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "GOOGLESHEETS" => Ok(ExporterProvider::GoogleSheets),
            "KAFKA" => Ok(ExporterProvider::Kafka),
            _ => Err(anyhow::anyhow!("Unsupported the exporter provider: {}", provider)),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            ExporterProvider::GoogleSheets => "GOOGLESHEETS",
            ExporterProvider::Kafka => "KAFKA",
        }
    }
}
