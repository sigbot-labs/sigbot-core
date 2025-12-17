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

/// Derived position information.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
pub struct PositionInfo {
    pub wallet_id: i64,
    pub symbol: String,
    pub side: PositionSide,
    pub size: f64,
    pub entry_price: f64,
    pub realized_pnl: f64,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum PositionSide {
    LONG,
    SHORT,
}

impl PositionSide {
    pub fn of(side: &str) -> Result<PositionSide, String> {
        match side.to_uppercase().as_str() {
            "LONG" => Ok(PositionSide::LONG),
            "SHORT" => Ok(PositionSide::SHORT),
            _ => Err(format!("Unsupported position side: {}", side)),
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            PositionSide::LONG => "LONG".to_string(),
            PositionSide::SHORT => "SHORT".to_string(),
        }
    }
}

impl<'r> FromRow<'r, SqliteRow> for PositionInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(PositionInfo {
            wallet_id: row.try_get("wallet_id")?,
            symbol: row.try_get("symbol")?,
            side: PositionSide::of(&row.try_get::<String, _>("side")?).map_err(|e| {
                sqlx::Error::Decode(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse position side: {}", e),
                    )
                    .into(),
                )
            })?,
            size: row.try_get("size")?,
            entry_price: row.try_get("entry_price")?,
            realized_pnl: row.try_get("realized_pnl")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'r> FromRow<'r, PgRow> for PositionInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(PositionInfo {
            wallet_id: row.try_get("wallet_id")?,
            symbol: row.try_get("symbol")?,
            side: PositionSide::of(&row.try_get::<String, _>("side")?).map_err(|e| {
                sqlx::Error::Decode(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse position side: {}", e),
                    )
                    .into(),
                )
            })?,
            size: row.try_get("size")?,
            entry_price: row.try_get("entry_price")?,
            realized_pnl: row.try_get("realized_pnl")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

// ---- Models ----

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QueryPositionRequest {
    pub wallet_id: Option<i64>,
    pub symbol: Option<String>,
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct QueryPositionResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<PositionInfo>>,
}

impl QueryPositionResponse {
    pub fn new(page: PageResponse, data: Vec<PositionInfo>) -> Self {
        QueryPositionResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[cfg(test)]
mod tests {}
