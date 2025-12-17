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

use crate::PageResponse;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use utoipa::ToSchema;
use validator::Validate;

// ---- Entity ----

/// Derived balance information.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
pub struct BalanceInfo {
    pub wallet_id: i64,
    pub asset: String,
    pub available: f64,
    pub locked: f64,
    pub updated_at: Option<DateTime<Utc>>,
}

impl<'r> FromRow<'r, SqliteRow> for BalanceInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(BalanceInfo {
            wallet_id: row.try_get("wallet_id")?,
            asset: row.try_get("asset")?,
            available: row.try_get("available")?,
            locked: row.try_get("locked")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'r> FromRow<'r, PgRow> for BalanceInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(BalanceInfo {
            wallet_id: row.try_get("wallet_id")?,
            asset: row.try_get("asset")?,
            available: row.try_get("available")?,
            locked: row.try_get("locked")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

// ---- Models ----

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QueryBalanceRequest {
    pub wallet_id: Option<i64>,
    pub asset: Option<String>,
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct QueryBalanceResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<BalanceInfo>>,
}

impl QueryBalanceResponse {
    pub fn new(page: PageResponse, data: Vec<BalanceInfo>) -> Self {
        QueryBalanceResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[cfg(test)]
mod tests {}
