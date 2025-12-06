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

use std::collections::HashMap;

use crate::{EntityBase, PageResponse};
use common_makestruct::MakeStructWith;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use validator::Validate;

// ---- Entity ---

// Manual impl for decode.
// #[derive(Serialize, Deserialize, Clone, Debug, sqlx::sqlite::FromRow, sqlx::sqlite::Decode)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct StrategyInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub provider: Option<String>,
    pub parameters: Option<HashMap<String, String>>,
    pub description: Option<String>,
}

impl Default for StrategyInfo {
    fn default() -> Self {
        StrategyInfo {
            base: EntityBase::new_empty(),
            name: None,
            provider: None,
            parameters: None,
            description: None,
        }
    }
}

/// SqliteRow impl for Strategy.
impl<'r> FromRow<'r, SqliteRow> for StrategyInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(StrategyInfo {
            base: EntityBase::from_row(row).unwrap(),
            name: row.try_get("name")?,
            provider: row.try_get::<Option<String>, _>("provider")?,
            // TODO: auto convert and wrap to Map attribute.
            parameters: None,
            description: Some(row.try_get("description")?),
        })
    }
}

/// Postgres Row impl for Strategy.
impl<'r> FromRow<'r, PgRow> for StrategyInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(StrategyInfo {
            base: EntityBase::from_row(row)?,
            name: row.try_get("name")?,
            provider: row.try_get::<Option<String>, _>("provider")?,
            // TODO: auto convert and wrap to Map attribute.
            parameters: None,
            description: Some(row.try_get("description")?),
        })
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
pub struct QueryStrategyRequest {
    // #[serde(flatten)]
    // #[serde(default)]
    // #[serde(skip)]
    // #[param(style = Form)]
    // #[param(value_type=Option<String>)]
    // pub page: Option<super::PageRequest>, // It is difficult to pass parameters using http get/query when nested structures.
    #[validate(length(min = 1, max = 32))]
    pub name: Option<String>,
    pub active: Option<bool>,
    #[validate(length(min = 1, max = 16))]
    pub provider: Option<String>,
}

impl QueryStrategyRequest {
    pub fn to_entity(&self) -> StrategyInfo {
        StrategyInfo {
            base: EntityBase::new_empty(),
            name: Some(self.name.clone().unwrap_or_default()),
            provider: self.provider.clone(),
            parameters: None,
            description: None,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct QueryStrategyResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<StrategyInfo>>,
}

impl QueryStrategyResponse {
    pub fn new(page: PageResponse, data: Vec<StrategyInfo>) -> Self {
        QueryStrategyResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, MakeStructWith)]
#[excludes(id)]
// #[smart_copy(target = "SaveStrategyRequestWith")]
pub struct SaveStrategyRequest {
    pub id: Option<i64>,
    #[validate(length(min = 1, max = 32))]
    pub name: String,
    #[validate(length(min = 1, max = 16))]
    pub provider: String,
    #[validate(length(min = 1, max = 8192))]
    pub parameters: Option<HashMap<String, String>>,
    #[validate(length(min = 1, max = 256))]
    pub description: Option<String>,
}

impl SaveStrategyRequest {
    pub fn to_entity(&self) -> StrategyInfo {
        StrategyInfo {
            base: EntityBase::new_with_id(self.id),
            name: Some(self.name.clone()),
            provider: Some(self.provider.clone()),
            parameters: self.parameters.clone(),
            description: self.description.clone(),
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct SaveStrategyResponse {
    pub id: i64,
}

impl SaveStrategyResponse {
    pub fn new(id: i64) -> Self {
        SaveStrategyResponse { id }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema)]
pub struct DeleteStrategyRequest {
    pub id: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct DeleteStrategyResponse {
    pub count: u64,
}

impl DeleteStrategyResponse {
    pub fn new(count: u64) -> Self {
        DeleteStrategyResponse { count }
    }
}

#[cfg(test)]
mod tests {}
