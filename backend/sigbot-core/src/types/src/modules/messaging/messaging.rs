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

use crate::EntityBase;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use std::collections::HashMap;

// ---- Entity ---

// Manual impl for decode.
// #[derive(Serialize, Deserialize, Clone, Debug, sqlx::sqlite::FromRow, sqlx::sqlite::Decode)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct MessagingInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub provider: Option<MessagingProvider>,
    pub plain_configuration: Option<HashMap<String, String>>,
    pub secret_configuration: Option<HashMap<String, String>>,
    pub description: Option<String>,
}

impl Default for MessagingInfo {
    fn default() -> Self {
        MessagingInfo {
            base: EntityBase::new_empty(),
            name: None,
            provider: Some(MessagingProvider::MQTT),
            plain_configuration: None,
            secret_configuration: None,
            description: None,
        }
    }
}

/// SqliteRow impl for Exchange.
impl<'r> FromRow<'r, SqliteRow> for MessagingInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(MessagingInfo {
            base: EntityBase::from_row(row).unwrap(),
            name: row.try_get("name")?,
            provider: row
                .try_get::<Option<String>, _>("provider")?
                .map(|s| MessagingProvider::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse messaging provider: {}", e),
                        )
                        .into(),
                    )
                })?,
            // TODO: auto convert and wrap to Map attribute.
            plain_configuration: None,
            secret_configuration: None,
            description: Some(row.try_get("description")?),
        })
    }
}

/// Postgres Row impl for Exchange.
impl<'r> FromRow<'r, PgRow> for MessagingInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(MessagingInfo {
            base: EntityBase::from_row(row)?,
            name: row.try_get("name")?,
            provider: row
                .try_get::<Option<String>, _>("provider")?
                .map(|s| MessagingProvider::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse messaging provider: {}", e),
                        )
                        .into(),
                    )
                })?,
            // TODO: auto convert and wrap to Map attribute.
            plain_configuration: None,
            secret_configuration: None,
            description: Some(row.try_get("description")?),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum MessagingProvider {
    MQTT,
}

impl MessagingProvider {
    pub fn of(provider: &str) -> Result<MessagingProvider, String> {
        match provider.to_uppercase().as_str() {
            "MQTT" => Ok(MessagingProvider::MQTT),
            _ => Err(format!("Unsupported the messaging provider: {}", provider)),
        }
    }
}

#[cfg(test)]
mod tests {}
