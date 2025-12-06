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
pub struct ExchangeInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub provider: Option<ExchangeProvider>,
    pub configuration: Option<HashMap<String, String>>,
    pub secrets: Option<HashMap<String, String>>,
    pub description: Option<String>,
}

impl Default for ExchangeInfo {
    fn default() -> Self {
        ExchangeInfo {
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
impl<'r> FromRow<'r, SqliteRow> for ExchangeInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(ExchangeInfo {
            base: EntityBase::from_row(row).unwrap(),
            name: row.try_get("name")?,
            provider: row
                .try_get::<Option<String>, _>("provider")?
                .map(|s| ExchangeProvider::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse exchange provider: {}", e),
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
impl<'r> FromRow<'r, PgRow> for ExchangeInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(ExchangeInfo {
            base: EntityBase::from_row(row)?,
            name: row.try_get("name")?,
            provider: row
                .try_get::<Option<String>, _>("provider")?
                .map(|s| ExchangeProvider::of(&s))
                .transpose()
                .map_err(|e| {
                    sqlx::Error::Decode(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse exchange provider: {}", e),
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
pub enum ExchangeProvider {
    // CEXs
    BINANCE,
    OKX,
    COINBASE,
    BITGET,
    BYBIT,
    KRAKEN,
    // DEXs
    HYPERLIQUID,
    LIGHTER,
}

impl ExchangeProvider {
    pub fn of(provider: &str) -> Result<ExchangeProvider, String> {
        match provider.to_uppercase().as_str() {
            "BINANCE" => Ok(ExchangeProvider::BINANCE),
            "OKX" => Ok(ExchangeProvider::OKX),
            "COINBASE" => Ok(ExchangeProvider::COINBASE),
            "BITGET" => Ok(ExchangeProvider::BITGET),
            "BYBIT" => Ok(ExchangeProvider::BYBIT),
            "KRAKEN" => Ok(ExchangeProvider::KRAKEN),
            "HYPERLIQUID" => Ok(ExchangeProvider::HYPERLIQUID),
            "LIGHTER" => Ok(ExchangeProvider::LIGHTER),
            _ => Err(format!("Unsupported the exchange provider: {}", provider)),
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
pub struct QueryExchangeRequest {
    // #[serde(flatten)]
    // #[serde(default)]
    // #[serde(skip)]
    // #[param(style = Form)]
    // #[param(value_type=Option<String>)]
    // pub page: Option<super::PageRequest>, // It is difficult to pass parameters using http get/query when nested structures.
    #[validate(length(min = 1, max = 32))]
    pub name: Option<String>,
    #[validate(length(min = 1, max = 16))]
    pub provider: Option<String>,
}

impl QueryExchangeRequest {
    pub fn to_entity(&self) -> ExchangeInfo {
        ExchangeInfo {
            base: EntityBase::new_empty(),
            name: Some(self.name.clone().unwrap_or_default()),
            provider: ExchangeProvider::of(self.provider.clone().unwrap_or_default().as_str()).ok(),
            configuration: None,
            secrets: None,
            description: None,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct QueryExchangeResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<ExchangeInfo>>,
}

impl QueryExchangeResponse {
    pub fn new(page: PageResponse, data: Vec<ExchangeInfo>) -> Self {
        QueryExchangeResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, MakeStructWith)]
#[excludes(id)]
// #[smart_copy(target = "SaveExchangeRequestWith")]
pub struct SaveExchangeRequest {
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

impl SaveExchangeRequest {
    pub fn to_entity(&self) -> ExchangeInfo {
        ExchangeInfo {
            base: EntityBase::new_with_id(self.id),
            name: Some(self.name.clone()),
            provider: ExchangeProvider::of(self.provider.clone().as_str()).ok(),
            configuration: self.plain_configuration.clone(),
            secrets: self.secret_configuration.clone(),
            description: self.description.clone(),
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct SaveExchangeResponse {
    pub id: i64,
}

impl SaveExchangeResponse {
    pub fn new(id: i64) -> Self {
        SaveExchangeResponse { id }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema)]
pub struct DeleteExchangeRequest {
    pub id: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct DeleteExchangeResponse {
    pub count: u64,
}

impl DeleteExchangeResponse {
    pub fn new(count: u64) -> Self {
        DeleteExchangeResponse { count }
    }
}
