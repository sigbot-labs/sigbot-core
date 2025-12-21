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

use crate::modules::backtest::BacktestMgrProvider;
use crate::EntityBase;
use crate::PageResponse;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use utoipa::ToSchema;
use validator::Validate;

// ---- Entity ----

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
pub struct BacktestCaseInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    pub tenant_id: String,
    pub provider: Option<BacktestMgrProvider>,
}

impl Default for BacktestCaseInfo {
    fn default() -> Self {
        BacktestCaseInfo {
            base: EntityBase::new_empty(),
            tenant_id: String::new(),
            provider: None,
        }
    }
}

impl<'r> FromRow<'r, SqliteRow> for BacktestCaseInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(BacktestCaseInfo {
            base: EntityBase::from_row(row)?,
            tenant_id: row.try_get("tenant_id")?,
            provider: row
                .try_get::<Option<String>, _>("provider")?
                .map(|s| BacktestMgrProvider::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse backtest manager provider: {}", e),
                        )
                        .into(),
                    )
                })?,
        })
    }
}

impl<'r> FromRow<'r, PgRow> for BacktestCaseInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(BacktestCaseInfo {
            base: EntityBase::from_row(row)?,
            tenant_id: row.try_get("tenant_id")?,
            provider: row
                .try_get::<Option<String>, _>("provider")?
                .map(|s| BacktestMgrProvider::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse backtest manager provider: {}", e),
                        )
                        .into(),
                    )
                })?,
        })
    }
}

// ---- Models ----

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QueryBacktestCaseRequest {
    pub tenant_id: Option<String>,
    pub provider: Option<String>,
}

impl QueryBacktestCaseRequest {
    pub fn to_entity(&self) -> BacktestCaseInfo {
        BacktestCaseInfo {
            base: EntityBase::new_empty(),
            tenant_id: self.tenant_id.clone().unwrap_or_default(),
            provider: self
                .provider
                .clone()
                .map(|s| BacktestMgrProvider::of(&s))
                .transpose()
                .unwrap_or(None),
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct QueryBacktestCaseResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<BacktestCaseInfo>>,
}

impl QueryBacktestCaseResponse {
    pub fn new(page: PageResponse, data: Vec<BacktestCaseInfo>) -> Self {
        QueryBacktestCaseResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema)]
pub struct SaveBacktestCaseRequest {
    pub backtest_case_id: Option<i64>,
    #[validate(length(min = 1, max = 255))]
    pub tenant_id: String,
    #[validate(length(min = 1, max = 50))]
    pub provider: String,
}

impl SaveBacktestCaseRequest {
    pub fn to_entity(&self) -> BacktestCaseInfo {
        BacktestCaseInfo {
            base: EntityBase::new_with_id(self.backtest_case_id),
            tenant_id: self.tenant_id.clone(),
            provider: BacktestMgrProvider::of(&self.provider).ok(),
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct SaveBacktestCaseResponse {
    pub backtest_case_id: i64,
}

impl SaveBacktestCaseResponse {
    pub fn new(backtest_case_id: i64) -> Self {
        SaveBacktestCaseResponse { backtest_case_id }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema)]
pub struct DeleteBacktestCaseRequest {
    pub backtest_case_id: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct DeleteBacktestCaseResponse {
    pub count: u64,
}

impl DeleteBacktestCaseResponse {
    pub fn new(count: u64) -> Self {
        DeleteBacktestCaseResponse { count }
    }
}

#[cfg(test)]
mod tests {}
