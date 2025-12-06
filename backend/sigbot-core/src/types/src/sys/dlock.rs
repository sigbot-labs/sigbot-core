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

use std::time::Duration;

use crate::EntityBase;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct DLock {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub holder: Option<String>,
    pub timeout: Option<Duration>,
}

impl Default for DLock {
    fn default() -> Self {
        DLock {
            base: EntityBase::new_empty(),
            name: None,
            holder: None,
            timeout: None,
        }
    }
}

/// SqliteRow impl for DLock.

impl<'r> FromRow<'r, SqliteRow> for DLock {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(DLock {
            base: EntityBase {
                id: row.try_get("id")?,
                status: row.try_get::<Option<i8>, _>("status")?,
                created_by: row.try_get("created_by")?,
                created_time: row.try_get("created_time")?,
                updated_by: row.try_get("updated_by")?,
                updated_time: row.try_get("updated_time")?,
                del_flag: row.try_get::<Option<i8>, _>("del_flag")?,
            },
            name: row.try_get("name")?,
            holder: row.try_get("holder")?,
            timeout: row
                .try_get::<Option<i64>, _>("timeout")?
                .map(|timeout| Duration::from_secs(timeout as u64)),
        })
    }
}

/// Postgres Row impl for DLock.

impl<'r> FromRow<'r, PgRow> for DLock {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(DLock {
            base: EntityBase {
                id: row.try_get("id")?,
                status: row.try_get::<Option<i8>, _>("status")?,
                created_by: row.try_get("created_by")?,
                created_time: row.try_get("created_time")?,
                updated_by: row.try_get("updated_by")?,
                updated_time: row.try_get("updated_time")?,
                del_flag: row.try_get::<Option<i8>, _>("del_flag")?,
            },
            name: row.try_get("name")?,
            holder: row.try_get("holder")?,
            timeout: row
                .try_get::<Option<i64>, _>("timeout")?
                .map(|timeout| Duration::from_secs(timeout as u64)),
        })
    }
}
