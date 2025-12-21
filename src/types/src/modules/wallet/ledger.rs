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

/// Immutable Ledger (append-only; no update/delete; unified for backtest and live trading)
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
pub struct LedgerInfo {
    pub wallet_id: i64,
    pub order_id: i64,
    pub trade_id: i64,
    pub symbol: String,
    pub side: TradeSide,
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
    pub fee_asset: Option<String>,
    pub exchange_order_id: Option<String>,
    pub exchange_trade_id: Option<String>,
    pub ts: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum TradeSide {
    BUY,
    SELL,
}

impl TradeSide {
    pub fn of(side: &str) -> Result<TradeSide, String> {
        match side.to_uppercase().as_str() {
            "BUY" => Ok(TradeSide::BUY),
            "SELL" => Ok(TradeSide::SELL),
            _ => Err(format!("Unsupported trade side: {}", side)),
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            TradeSide::BUY => "BUY".to_string(),
            TradeSide::SELL => "SELL".to_string(),
        }
    }
}

impl<'r> FromRow<'r, SqliteRow> for LedgerInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(LedgerInfo {
            wallet_id: row.try_get("wallet_id")?,
            order_id: row.try_get("order_id")?,
            trade_id: row.try_get("trade_id")?,
            symbol: row.try_get("symbol")?,
            side: TradeSide::of(&row.try_get::<String, _>("side")?).map_err(|e| {
                sqlx::Error::Decode(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse trade side: {}", e),
                    )
                    .into(),
                )
            })?,
            price: row.try_get("price")?,
            qty: row.try_get("qty")?,
            fee: row.try_get("fee")?,
            fee_asset: row.try_get("fee_asset")?,
            exchange_order_id: row.try_get("exchange_order_id")?,
            exchange_trade_id: row.try_get("exchange_trade_id")?,
            ts: row.try_get("ts")?,
        })
    }
}

impl<'r> FromRow<'r, PgRow> for LedgerInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(LedgerInfo {
            wallet_id: row.try_get("wallet_id")?,
            order_id: row.try_get("order_id")?,
            trade_id: row.try_get("trade_id")?,
            symbol: row.try_get("symbol")?,
            side: TradeSide::of(&row.try_get::<String, _>("side")?).map_err(|e| {
                sqlx::Error::Decode(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse trade side: {}", e),
                    )
                    .into(),
                )
            })?,
            price: row.try_get("price")?,
            qty: row.try_get("qty")?,
            fee: row.try_get("fee")?,
            fee_asset: row.try_get("fee_asset")?,
            exchange_order_id: row.try_get("exchange_order_id")?,
            exchange_trade_id: row.try_get("exchange_trade_id")?,
            ts: row.try_get("ts")?,
        })
    }
}

// ---- Models ----

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QueryLedgerRequest {
    pub wallet_id: Option<i64>,
    pub order_id: Option<i64>,
    pub symbol: Option<String>,
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct QueryLedgerResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<LedgerInfo>>,
}

impl QueryLedgerResponse {
    pub fn new(page: PageResponse, data: Vec<LedgerInfo>) -> Self {
        QueryLedgerResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[cfg(test)]
mod tests {}
