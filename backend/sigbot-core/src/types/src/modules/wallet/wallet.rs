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
use utoipa::ToSchema;
use validator::Validate;

use crate::PageResponse;

// ---- Entity ----

/// Trading Wallet Account Table (lifecycle: create → permanent; does not contain financial values)
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
pub struct WalletInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    pub tenant_id: String,
    pub exchange: String,
    pub mode: WalletMode,
    pub account_type: Option<String>,
}

impl Default for WalletInfo {
    fn default() -> Self {
        WalletInfo {
            base: EntityBase::new_empty(),
            tenant_id: String::new(),
            exchange: String::new(),
            mode: WalletMode::LIVE,
            account_type: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum WalletMode {
    LIVE,
    BACKTEST,
    PAPER,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
pub enum WalletProvider {
    DEFAULT,
}

impl WalletProvider {
    pub fn of(provider: &str) -> Result<WalletProvider, anyhow::Error> {
        match provider.to_uppercase().as_str() {
            "DEFAULT" => Ok(WalletProvider::DEFAULT),
            _ => Err(anyhow::anyhow!("Unsupported the wallet provider: {}", provider)),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            WalletProvider::DEFAULT => "DEFAULT",
        }
    }
}

impl WalletMode {
    pub fn of(mode: &str) -> Result<WalletMode, String> {
        match mode.to_uppercase().as_str() {
            "LIVE" => Ok(WalletMode::LIVE),
            "BACKTEST" => Ok(WalletMode::BACKTEST),
            "PAPER" => Ok(WalletMode::PAPER),
            _ => Err(format!("Unsupported wallet mode: {}", mode)),
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            WalletMode::LIVE => "live".to_string(),
            WalletMode::BACKTEST => "backtest".to_string(),
            WalletMode::PAPER => "paper".to_string(),
        }
    }
}

impl<'r> FromRow<'r, SqliteRow> for WalletInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(WalletInfo {
            base: EntityBase::from_row(row)?,
            tenant_id: row.try_get("tenant_id")?,
            exchange: row.try_get("exchange")?,
            mode: row
                .try_get::<Option<String>, _>("mode")?
                .map(|s| WalletMode::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse wallet mode: {}", e),
                        )
                        .into(),
                    )
                })?
                .unwrap_or(WalletMode::LIVE),
            account_type: row.try_get("account_type")?,
        })
    }
}

impl<'r> FromRow<'r, PgRow> for WalletInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(WalletInfo {
            base: EntityBase::from_row(row)?,
            tenant_id: row.try_get("tenant_id")?,
            exchange: row.try_get("exchange")?,
            mode: row
                .try_get::<Option<String>, _>("mode")?
                .map(|s| WalletMode::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse wallet mode: {}", e),
                        )
                        .into(),
                    )
                })?
                .unwrap_or(WalletMode::LIVE),
            account_type: row.try_get("account_type")?,
        })
    }
}

// ---- Models ----

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QueryWalletRequest {
    pub tenant_id: Option<String>,
    pub exchange: Option<String>,
    pub mode: Option<String>,
}

impl QueryWalletRequest {
    pub fn to_entity(&self) -> WalletInfo {
        WalletInfo {
            base: EntityBase::new_empty(),
            tenant_id: self.tenant_id.clone().unwrap_or_default(),
            exchange: self.exchange.clone().unwrap_or_default(),
            mode: self
                .mode
                .as_ref()
                .and_then(|m| WalletMode::of(m).ok())
                .unwrap_or(WalletMode::LIVE),
            account_type: None,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct QueryWalletResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<WalletInfo>>,
}

impl QueryWalletResponse {
    pub fn new(page: PageResponse, data: Vec<WalletInfo>) -> Self {
        QueryWalletResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema)]
pub struct SaveWalletRequest {
    pub wallet_id: Option<i64>,
    #[validate(length(min = 1, max = 255))]
    pub tenant_id: String,
    #[validate(length(min = 1, max = 100))]
    pub exchange: String,
    #[validate(length(min = 1, max = 50))]
    pub mode: String,
    #[validate(length(min = 1, max = 100))]
    pub account_type: Option<String>,
}

impl SaveWalletRequest {
    pub fn to_entity(&self) -> WalletInfo {
        WalletInfo {
            base: EntityBase::new_with_id(self.wallet_id),
            tenant_id: self.tenant_id.clone(),
            exchange: self.exchange.clone(),
            mode: WalletMode::of(&self.mode).unwrap_or(WalletMode::LIVE),
            account_type: self.account_type.clone(),
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct SaveWalletResponse {
    pub wallet_id: i64,
}

impl SaveWalletResponse {
    pub fn new(wallet_id: i64) -> Self {
        SaveWalletResponse { wallet_id }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, ToSchema)]
pub struct DeleteWalletRequest {
    pub wallet_id: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct DeleteWalletResponse {
    pub count: u64,
}

impl DeleteWalletResponse {
    pub fn new(count: u64) -> Self {
        DeleteWalletResponse { count }
    }
}

#[cfg(test)]
mod tests {}
