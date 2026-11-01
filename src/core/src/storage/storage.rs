// SPDX-License-Identifier: GPL-3.0-only

use super::parameters::{ParameterStyle, SqlParameters};
use crate::config::config::{AppConfigProperties, StorageProvider};
use anyhow::Error;
use async_trait::async_trait;
use sigbot_types::{PageRequest, PageResponse};
use sigbot_utils::types::GenericValue;
use sqlx::{PgPool, SqlitePool};
use std::collections::HashMap;
use uuid::Uuid;

#[async_trait]
pub trait IGenericRepository<T>: Send + Sync {
    async fn select(&self, param: T, page: PageRequest) -> Result<(PageResponse, Vec<T>), Error>
    where
        T: 'static + Send + Sync;

    async fn select_by(&self, id: Uuid) -> Result<T, Error>
    where
        T: 'static + Send + Sync;

    async fn insert(&self, entity: T) -> Result<Uuid, Error>
    where
        T: 'static + Send + Sync,
    {
        self.upsert(entity).await
    }

    async fn upsert(&self, param: T) -> Result<Uuid, Error>
    where
        T: 'static + Send + Sync;

    async fn update(&self, param: T) -> Result<Uuid, Error>
    where
        T: 'static + Send + Sync;

    async fn delete_all(&self) -> Result<u64, Error>;

    async fn delete_by_id(&self, id: Uuid) -> Result<u64, Error>;

    async fn select_by_sql(&self, _sql_template: &str, _params: &HashMap<String, GenericValue>) -> Result<Vec<T>, Error>
    where
        T: 'static + Send + Sync,
    {
        Err(Error::msg("select_by_sql is not implemented for this repository"))
    }
}

pub struct SqlQueryExecutor;

impl SqlQueryExecutor {
    pub async fn postgres<T>(
        pool: &PgPool,
        sql_template: &str,
        params: &HashMap<String, GenericValue>,
    ) -> Result<Vec<T>, Error>
    where
        T: for<'row> sqlx::FromRow<'row, sqlx::postgres::PgRow> + Send + Sync + Unpin + 'static,
    {
        let parameters = SqlParameters::compile(sql_template, params, ParameterStyle::Postgres)?;
        let mut query = sqlx::query_as::<_, T>(&parameters.statement);
        for value in parameters.values {
            query = match value {
                GenericValue::Int32(value) => query.bind(*value),
                GenericValue::Int64(value) => query.bind(*value),
                GenericValue::Uint32(value) => query.bind(*value as i64),
                GenericValue::Uint64(value) => {
                    query.bind(i64::try_from(*value).map_err(|_| Error::msg("SQL integer exceeds i64 range"))?)
                }
                GenericValue::Float32(value) => query.bind(*value as f64),
                GenericValue::Float64(value) => query.bind(*value),
                GenericValue::Bool(value) => query.bind(*value),
                GenericValue::String(value) => query.bind(value.clone()),
                GenericValue::DateTime(value) => query.bind(*value),
            };
        }
        query.fetch_all(pool).await.map_err(Error::from)
    }

    pub async fn sqlite<T>(
        pool: &SqlitePool,
        sql_template: &str,
        params: &HashMap<String, GenericValue>,
    ) -> Result<Vec<T>, Error>
    where
        T: for<'row> sqlx::FromRow<'row, sqlx::sqlite::SqliteRow> + Send + Sync + Unpin + 'static,
    {
        let parameters = SqlParameters::compile(sql_template, params, ParameterStyle::Sqlite)?;
        let mut query = sqlx::query_as::<_, T>(&parameters.statement);
        for value in parameters.values {
            query = match value {
                GenericValue::Int32(value) => query.bind(*value),
                GenericValue::Int64(value) => query.bind(*value),
                GenericValue::Uint32(value) => query.bind(*value as i64),
                GenericValue::Uint64(value) => {
                    query.bind(i64::try_from(*value).map_err(|_| Error::msg("SQL integer exceeds i64 range"))?)
                }
                GenericValue::Float32(value) => query.bind(*value as f64),
                GenericValue::Float64(value) => query.bind(*value),
                GenericValue::Bool(value) => query.bind(*value),
                GenericValue::String(value) => query.bind(value.clone()),
                GenericValue::DateTime(value) => query.bind(*value),
            };
        }
        query.fetch_all(pool).await.map_err(Error::from)
    }
}

pub struct RepositoryContainer<T>
where
    T: 'static + Send + Sync,
{
    sqlite: Option<Box<dyn IGenericRepository<T>>>,
    postgres: Option<Box<dyn IGenericRepository<T>>>,
}

impl<T> RepositoryContainer<T>
where
    T: 'static + Send + Sync,
{
    pub fn new(
        sqlite: Option<Box<dyn IGenericRepository<T>>>,
        postgres: Option<Box<dyn IGenericRepository<T>>>,
    ) -> Self {
        Self { sqlite, postgres }
    }

    pub fn get(&self, config: &AppConfigProperties) -> &dyn IGenericRepository<T> {
        match config.storage.provider {
            StorageProvider::SQLite => self.sqlite.as_deref().expect("The SQLite repository is not configured"),
            StorageProvider::Postgres => self
                .postgres
                .as_deref()
                .expect("The PostgreSQL repository is not configured"),
        }
    }
}
