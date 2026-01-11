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

pub mod mongo;
#[macro_use]
pub mod postgres;
#[macro_use]
pub mod sqlite;

use crate::config::config::{AppConfigProperties, AppDBType};
use anyhow::Error;
use async_trait::async_trait;
use regex::Regex;
use sigbot_types::{PageRequest, PageResponse};
use sigbot_utils::types::GenericValue;
use sqlx::{PgPool, SqlitePool};
use std::collections::HashMap;

#[async_trait] // solution2: async fn + dyn polymorphism problem.
pub trait AsyncRepository<T>: Send + Sync {
    // solution1: async fn + dyn polymorphism problem.
    // fn select(&self) -> Box<dyn Future<Output = Result<Page<T>, Error>> + Send>;
    async fn select(&self, mut param: T, page: PageRequest) -> Result<(PageResponse, Vec<T>), Error>
    where
        T: 'static + Send + Sync;
    async fn select_by_id(&self, id: i64) -> Result<T, Error>
    where
        T: 'static + Send + Sync;
    async fn insert(&self, mut param: T) -> Result<i64, Error>
    where
        T: 'static + Send + Sync;
    async fn update(&self, mut param: T) -> Result<i64, Error>
    where
        T: 'static + Send + Sync;
    async fn delete_all(&self) -> Result<u64, Error>;
    async fn delete_by_id(&self, id: i64) -> Result<u64, Error>;

    /// Execute a custom SQL query with prepared statement parameters and return results
    /// SQL template should use named parameters (e.g., :param_name for SQLite, $param_name for PostgreSQL)
    /// Parameters are provided as a HashMap mapping parameter names to values
    /// Default implementation returns an error indicating this feature is not supported
    /// Subclasses can override this method to provide custom SQL query functionality
    async fn select_by_sql(&self, _sql_template: &str, _params: &HashMap<String, GenericValue>) -> Result<Vec<T>, Error>
    where
        T: 'static + Send + Sync,
    {
        Err(Error::msg("select_by_sql is not implemented for this repository"))
    }
}

/// Execute a custom SQL query with prepared statement parameters for PostgreSQL
/// SQL template should use named parameters (:name or $name)
pub async fn select_by_sql_postgres<T>(
    pool: &PgPool,
    sql_template: &str,
    params: &HashMap<String, GenericValue>,
) -> Result<Vec<T>, Error>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Sync + Unpin + 'static,
{
    // Convert SQL template with named parameters (:name or $name) to positional parameters ($1, $2, etc.)
    let mut sql = sql_template.to_string();
    let mut param_values = Vec::new();
    let mut param_index = 1;

    // Extract parameter names from SQL template in order of appearance
    // Support both :name and $name formats for PostgreSQL
    let re_named = Regex::new(r":(\w+)|(?<!\w)\$(\w+)").unwrap();
    let mut replacements = Vec::new();

    for cap in re_named.captures_iter(sql_template) {
        let param_name = cap
            .get(1)
            .or_else(|| cap.get(2))
            .map(|m| m.as_str())
            .ok_or_else(|| Error::msg("Invalid parameter format"))?;

        if params.contains_key(param_name) {
            let placeholder = cap.get(0).unwrap().as_str();
            let positional = format!("${}", param_index);
            replacements.push((placeholder.to_string(), positional, param_name.to_string()));
            param_index += 1;
        }
    }

    // Replace placeholders with positional parameters and collect values
    for (placeholder, positional, param_name) in &replacements {
        sql = sql.replacen(placeholder, positional, 1);
        if let Some(value) = params.get(param_name) {
            param_values.push(value);
        }
    }

    // Build query with positional parameters
    let mut query = sqlx::query_as::<_, T>(&sql);
    for value in param_values {
        query = match value {
            GenericValue::Int32(v) => query.bind(*v),
            GenericValue::Int64(v) => query.bind(*v),
            GenericValue::Uint32(v) => query.bind(*v as i64),
            GenericValue::Uint64(v) => query.bind(*v as i64),
            GenericValue::Float32(v) => query.bind(*v as f64),
            GenericValue::Float64(v) => query.bind(*v),
            GenericValue::Bool(v) => query.bind(*v),
            GenericValue::String(v) => query.bind(v.clone()),
            GenericValue::DateTime(v) => query.bind(*v),
        };
    }

    query.fetch_all(pool).await.map_err(Error::from)
}

/// Execute a custom SQL query with prepared statement parameters for SQLite
/// SQL template should use named parameters (:name)
pub async fn select_by_sql_sqlite<T>(
    pool: &SqlitePool,
    sql_template: &str,
    params: &HashMap<String, GenericValue>,
) -> Result<Vec<T>, Error>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Sync + Unpin + 'static,
{
    // Convert SQL template with named parameters (:name) to positional parameters (?, ?, etc.)
    let mut sql = sql_template.to_string();
    let mut param_values = Vec::new();

    // Extract parameter names from SQL template in order of appearance
    // SQLite uses :name format for named parameters
    let re_named = Regex::new(r":(\w+)").unwrap();
    let mut replacements = Vec::new();

    for cap in re_named.captures_iter(sql_template) {
        let param_name = cap
            .get(1)
            .map(|m| m.as_str())
            .ok_or_else(|| Error::msg("Invalid parameter format"))?;

        if params.contains_key(param_name) {
            let placeholder = cap.get(0).unwrap().as_str();
            replacements.push((placeholder.to_string(), "?".to_string(), param_name.to_string()));
        }
    }

    // Replace placeholders with positional parameters and collect values
    for (placeholder, positional, param_name) in &replacements {
        sql = sql.replacen(placeholder, positional, 1);
        if let Some(value) = params.get(param_name) {
            param_values.push(value);
        }
    }

    // Build query with positional parameters
    let mut query = sqlx::query_as::<_, T>(&sql);
    for value in param_values {
        query = match value {
            GenericValue::Int32(v) => query.bind(*v),
            GenericValue::Int64(v) => query.bind(*v),
            GenericValue::Uint32(v) => query.bind(*v as i64),
            GenericValue::Uint64(v) => query.bind(*v as i64),
            GenericValue::Float32(v) => query.bind(*v as f64),
            GenericValue::Float64(v) => query.bind(*v),
            GenericValue::Bool(v) => query.bind(*v),
            GenericValue::String(v) => query.bind(v.clone()),
            GenericValue::DateTime(v) => query.bind(*v),
        };
    }

    query.fetch_all(pool).await.map_err(Error::from)
}

pub struct RepositoryContainer<T>
where
    T: 'static + Send + Sync,
{
    sqlite_repo: Option<Box<dyn AsyncRepository<T>>>,
    postgres_repo: Option<Box<dyn AsyncRepository<T>>>,
    mongo_repo: Option<Box<dyn AsyncRepository<T>>>,
}

impl<T> RepositoryContainer<T>
where
    T: 'static + Send + Sync,
{
    pub fn new(
        sqlite_repo: Option<Box<dyn AsyncRepository<T>>>,
        postgres_repo: Option<Box<dyn AsyncRepository<T>>>,
        mongo_repo: Option<Box<dyn AsyncRepository<T>>>,
    ) -> Self {
        RepositoryContainer {
            sqlite_repo,
            postgres_repo,
            mongo_repo,
        }
    }

    fn sqlite_repo(&self) -> &dyn AsyncRepository<T> {
        self.sqlite_repo
            .as_ref()
            .map(|repo| &**repo)
            .expect("The sqlite repository not configured.")
    }

    fn postgres_repo(&self) -> &dyn AsyncRepository<T> {
        self.postgres_repo
            .as_ref()
            .map(|repo| &**repo)
            .expect("The postgresql repository not configured.")
    }

    fn mongo_repo(&self) -> &dyn AsyncRepository<T> {
        self.mongo_repo
            .as_ref()
            .map(|repo| &**repo)
            .expect("The mongodb repository not configured.")
    }

    pub fn get(/*&mut self*/ &self, config: &AppConfigProperties) -> &dyn AsyncRepository<T> {
        match config.appdb.db_type {
            AppDBType::SQLITE => self.sqlite_repo(),
            AppDBType::POSTGRESQL => self.postgres_repo(),
            AppDBType::MONGODB => self.mongo_repo(),
        }
    }
}
