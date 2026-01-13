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
use common_makestruct::MakeStructWith;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use validator::Validate;

/// Generic log entry structure for logging
/// This structure is designed to be reusable across different projects
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct LogInfo {
    #[serde(flatten)]
    pub base: EntityBase,
    /// Service name that this log entry belongs to (e.g., "backtest", "datafeed", "strategy", "api", etc.)
    pub service_name: Option<String>,
    /// Log type/category for classification (e.g., "INPUT", "EVALUATION", "TRANSACTION", "OUTPUT", "POST", "SYSTEM", "ERROR")
    pub log_type: Option<String>,
    /// Log level: "DEBUG", "INFO", "WARN", "ERROR", "FATAL"
    pub level: Option<String>,
    /// Log message content
    pub content: Option<String>,
    /// Source location (e.g., file path, module name, function name)
    pub source: Option<String>,
    /// Additional tags for filtering and categorization (comma-separated or JSON array)
    pub tags: Option<String>,
}

impl Default for LogInfo {
    fn default() -> Self {
        LogInfo {
            base: EntityBase::new_empty(),
            service_name: None,
            log_type: None,
            level: None,
            content: None,
            source: None,
            tags: None,
        }
    }
}

/// SqliteRow impl for Log.
impl<'r> FromRow<'r, SqliteRow> for LogInfo {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(LogInfo {
            base: EntityBase::from_row(row)?,
            service_name: row.try_get("service_name")?,
            log_type: row.try_get("log_type")?,
            level: row.try_get("level")?,
            content: row.try_get("content")?,
            source: row.try_get("source")?,
            tags: row.try_get("tags")?,
        })
    }
}

/// Postgres Row impl for Log.
impl<'r> FromRow<'r, PgRow> for LogInfo {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(LogInfo {
            base: EntityBase::from_row(row)?,
            service_name: row.try_get("service_name")?,
            log_type: row.try_get("log_type")?,
            level: row.try_get("level")?,
            content: row.try_get("content")?,
            source: row.try_get("source")?,
            tags: row.try_get("tags")?,
        })
    }
}

// --- Log operation models(append, tail, search, stats) ---

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, MakeStructWith)]
#[excludes(id)]
pub struct AppendLogRequest {
    #[validate(length(min = 1, max = 64))]
    pub service_name: Option<String>, // backtest, datafeed, strategy, api, etc.
    #[validate(length(min = 1, max = 64))]
    pub log_type: Option<String>,
    #[validate(length(min = 1, max = 16))]
    pub level: Option<String>, // DEBUG, INFO, WARN, ERROR, FATAL
    #[validate(length(min = 1, max = 4096))]
    pub content: String,
    #[validate(length(min = 1, max = 256))]
    pub source: Option<String>, // file path, module name, function name
    #[validate(length(min = 1, max = 256))]
    pub tags: Option<String>, // comma-separated tags or JSON array
}

impl AppendLogRequest {
    pub fn to_log(&self) -> LogInfo {
        LogInfo {
            base: EntityBase::new_empty(),
            service_name: self.service_name.clone(),
            log_type: self.log_type.clone(),
            level: self.level.clone(),
            content: Some(self.content.clone()),
            source: self.source.clone(),
            tags: self.tags.clone(),
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct AppendLogResponse {
    pub id: i64,
}

impl AppendLogResponse {
    pub fn new(id: i64) -> Self {
        AppendLogResponse { id }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TailLogRequest {
    #[validate(length(min = 1, max = 64))]
    pub service_name: Option<String>,
    #[validate(length(min = 1, max = 64))]
    pub log_type: Option<String>,
    #[validate(length(min = 1, max = 256))]
    pub keyword: Option<String>, // Search keyword in content
    pub limit: Option<u32>, // Number of recent logs to retrieve, default 100
    pub since: Option<chrono::DateTime<chrono::Utc>>, // Only return logs after this timestamp
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct TailLogResponse {
    pub logs: Vec<LogInfo>,
}

impl TailLogResponse {
    pub fn new(logs: Vec<LogInfo>) -> Self {
        TailLogResponse { logs }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SearchLogRequest {
    #[validate(length(min = 1, max = 64))]
    pub service_name: Option<String>,
    #[validate(length(min = 1, max = 64))]
    pub log_type: Option<String>,
    #[validate(length(min = 1, max = 16))]
    pub level: Option<String>,
    #[validate(length(min = 1, max = 256))]
    pub source: Option<String>,
    #[validate(length(min = 1, max = 256))]
    pub tags: Option<String>,
    #[validate(length(min = 1, max = 256))]
    pub keyword: Option<String>, // Search keyword in content
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct SearchLogResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<LogInfo>>,
}

impl SearchLogResponse {
    pub fn new(page: PageResponse, data: Vec<LogInfo>) -> Self {
        SearchLogResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct StatsLogRequest {
    #[validate(length(min = 1, max = 64))]
    pub service_name: Option<String>,
    #[validate(length(min = 1, max = 64))]
    pub log_type: Option<String>,
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct LogStats {
    pub total_count: i64,
    pub by_level: std::collections::HashMap<String, i64>,
    pub by_log_type: std::collections::HashMap<String, i64>,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct StatsLogResponse {
    pub stats: LogStats,
}

impl StatsLogResponse {
    pub fn new(stats: LogStats) -> Self {
        StatsLogResponse { stats }
    }
}
