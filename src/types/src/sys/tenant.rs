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
use serde_json;
use sqlx::postgres::PgRow;
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use validator::Validate;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct Tenant {
    #[serde(flatten)]
    pub base: EntityBase,
    pub name: Option<String>,
    pub shared: Option<bool>,  // True if the tenant is shared, false if the tenant is exclusive.
    pub admin_id: Option<i64>, // The ID of the administrator user.
    pub properties: Option<serde_json::Value>,
    pub description: Option<String>,
}

impl Default for Tenant {
    fn default() -> Self {
        Tenant {
            base: EntityBase::new_empty(),
            name: None,
            shared: None,
            admin_id: None,
            properties: None,
            description: None,
        }
    }
}

/// SqliteRow impl for Tenant.

impl<'r> FromRow<'r, SqliteRow> for Tenant {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Tenant {
            base: EntityBase::from_row(row).unwrap(),
            name: row.try_get("name")?,
            shared: row.try_get("shared")?,
            admin_id: row.try_get("admin_id")?,
            properties: row.try_get::<Option<String>, _>("properties")?.and_then(|json_str| {
                if json_str.is_empty() {
                    None
                } else {
                    serde_json::from_str::<serde_json::Value>(&json_str).ok()
                }
            }),
            description: row.try_get("description")?,
        })
    }
}

/// Postgres Row impl for Tenant.

impl<'r> FromRow<'r, PgRow> for Tenant {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(Tenant {
            base: EntityBase::from_row(row)?,
            name: row.try_get("name")?,
            shared: row.try_get("shared")?,
            admin_id: row.try_get("admin_id")?,
            properties: {
                // PostgreSQL JSONB can be retrieved as serde_json::Value
                let json_val: Option<serde_json::Value> = row.try_get("properties").ok().flatten();
                json_val.and_then(|v| if v.is_null() { None } else { Some(v) })
            },
            description: row.try_get("description")?,
        })
    }
}

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
pub struct QueryTenantRequest {
    #[validate(length(min = 1, max = 64))]
    pub name: Option<String>,
    pub shared: Option<bool>,
    pub admin_id: Option<i64>,
    pub properties: Option<HashMap<String, String>>,
    #[validate(length(min = 1, max = 1024))]
    pub description: Option<String>,
}

impl QueryTenantRequest {
    pub fn to_tenant(&self) -> Tenant {
        Tenant {
            base: EntityBase::new_empty(),
            name: Some(self.name.clone().unwrap_or_default()),
            shared: self.shared.clone(),
            admin_id: self.admin_id.clone(),
            properties: self
                .properties
                .as_ref()
                .map(|props| serde_json::to_value(props).unwrap_or(serde_json::Value::Null)),
            description: self.description.clone(),
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct QueryTenantResponse {
    pub page: Option<PageResponse>,
    pub data: Option<Vec<Tenant>>,
}

impl QueryTenantResponse {
    pub fn new(page: PageResponse, data: Vec<Tenant>) -> Self {
        QueryTenantResponse {
            page: Some(page),
            data: Some(data),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema, MakeStructWith)]
#[excludes(id)]
// #[smart_copy(target = "SaveTenantRequestWith")]
pub struct SaveTenantRequest {
    pub id: Option<i64>,
    #[validate(length(min = 1, max = 64))]
    pub name: Option<String>,
    pub shared: Option<bool>,
    pub admin_id: Option<i64>,
    pub properties: Option<HashMap<String, String>>,
    pub description: Option<String>,
}

impl SaveTenantRequest {
    pub fn to_tenant(&self) -> Tenant {
        Tenant {
            base: EntityBase::new_with_id(self.id),
            name: self.name.clone(), // self.name.as_ref().map(|n| n.to_string())
            shared: None,
            admin_id: None,
            properties: None,
            description: None,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct SaveTenantResponse {
    pub id: i64,
}

impl SaveTenantResponse {
    pub fn new(id: i64) -> Self {
        SaveTenantResponse { id }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Validate, utoipa::ToSchema)]
pub struct DeleteTenantRequest {
    pub id: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq, utoipa::ToSchema)]
pub struct DeleteTenantResponse {
    pub count: u64,
}

impl DeleteTenantResponse {
    pub fn new(count: u64) -> Self {
        DeleteTenantResponse { count }
    }
}
