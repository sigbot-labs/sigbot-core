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

use crate::{EntityBase, PageResponse};
use anyhow::Context;
use common_makestruct::MakeStructWith;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use std::collections::HashMap;
use validator::Validate;

// ---- Entity ---

// Manual impl for decode.
// #[derive(Serialize, Deserialize, Clone, Debug, sqlx::sqlite::FromRow, sqlx::sqlite::Decode)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct MessagingInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub provider: Option<MessagingProvider>,
    pub configuration: Option<HashMap<String, String>>,
    pub secrets: Option<HashMap<String, String>>,
    pub description: Option<String>,
}

impl Default for MessagingInfo {
    fn default() -> Self {
        MessagingInfo {
            base: EntityBase::new_empty(),
            name: None,
            provider: None,
            configuration: None,
            secrets: None,
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
            configuration: None,
            secrets: None,
            description: Some(row.try_get("description")?),
        })
    }
}

/// Postgres Row impl for Exchange.
impl<'r> FromRow<'r, PgRow> for MessagingInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(MessagingInfo {
            base: <EntityBase as FromRow<PgRow>>::from_row(row)?,
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
            configuration: None,
            secrets: None,
            description: Some(row.try_get("description")?),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub enum MessagingProvider {
    MQTT,
}

impl MessagingProvider {
    pub fn of(provider: &str) -> Result<MessagingProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "MQTT" => Ok(MessagingProvider::MQTT),
            _ => Err(anyhow::anyhow!("Unsupported the messaging provider: {}", provider)),
        }
    }
}

// ---- Models ----

#[derive(
    Deserialize,
    Clone,
    Debug,
    PartialEq,
    Validate,
    utoipa::ToSchema,
    utoipa::IntoParams, // PageableQueryRequest // Try using macro auto generated pageable query request.
)]
#[into_params(parameter_in = Query)]
pub struct QueryMessagingRequest {
    #[validate(length(min = 1, max = 32))]
    pub name: Option<String>,
    pub active: Option<bool>,
    #[validate(length(min = 1, max = 16))]
    pub provider: Option<String>,
}

impl QueryMessagingRequest {
    pub fn to_entity(&self) -> Result<MessagingInfo, anyhow::Error> {
        Ok(MessagingInfo {
            base: EntityBase::new_empty(),
            name: Some(self.name.to_owned().unwrap_or_default()),
            provider: Some(
                MessagingProvider::of(self.provider.to_owned().unwrap_or_default().as_str())
                    .context("Failed to parse messaging provider")?,
            ),
            configuration: None,
            secrets: None,
            description: None,
        })
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct QueryMessagingResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<MessagingInfo>>,
}

impl QueryMessagingResponse {
    pub fn new(page: PageResponse, data: Vec<MessagingInfo>) -> Self {
        QueryMessagingResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, MakeStructWith)]
#[excludes(id)]
// #[smart_copy(target = "SaveMessagingRequestWith")]
pub struct SaveMessagingRequest {
    pub id: Option<i64>,
    #[validate(length(min = 1, max = 32))]
    pub name: String,
    #[validate(length(min = 1, max = 16))]
    pub provider: String,
    #[validate(length(min = 1, max = 8192))]
    pub plain_configuration: Option<HashMap<String, String>>,
    #[validate(length(min = 1, max = 8192))]
    pub secret_configuration: Option<HashMap<String, String>>,
    #[validate(length(min = 1, max = 256))]
    pub description: Option<String>,
}

impl SaveMessagingRequest {
    pub fn to_entity(&self) -> Result<MessagingInfo, anyhow::Error> {
        Ok(MessagingInfo {
            base: EntityBase::new_with_id(self.id),
            name: Some(self.name.clone()),
            provider: Some(
                MessagingProvider::of(self.provider.to_owned().as_str())
                    .context("Failed to parse messaging provider")?,
            ),
            configuration: self.plain_configuration.clone(),
            secrets: self.secret_configuration.clone(),
            description: self.description.clone(),
        })
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct SaveMessagingResponse {
    pub id: i64,
}

impl SaveMessagingResponse {
    pub fn new(id: i64) -> Self {
        SaveMessagingResponse { id }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema)]
pub struct DeleteMessagingRequest {
    pub id: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct DeleteMessagingResponse {
    pub count: u64,
}

impl DeleteMessagingResponse {
    pub fn new(count: u64) -> Self {
        DeleteMessagingResponse { count }
    }
}

#[cfg(test)]
mod tests {}
