// SPDX-License-Identifier: GPL-3.0-only

use super::parameters::{ParameterStyle, SqlParameters};
use crate::config::config::{AppConfigProperties, StorageProvider};
use anyhow::{bail, ensure, Context, Error, Result};
use async_trait::async_trait;
use serde_json::Value;
use sigbot_types::domain::Domain;
use sigbot_types::{PageRequest, PageResponse};
use sigbot_utils::types::GenericValue;
use sqlx::{PgPool, Row, SqlitePool};
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

#[derive(Clone)]
pub enum DomainPool {
    Postgres(PgPool),
    SQLite(SqlitePool),
}

pub struct DomainRepository {
    pool: DomainPool,
    domain: Domain,
    tenant: Uuid,
    env: String,
}

impl DomainRepository {
    pub fn for_domain(&self, domain: Domain) -> Self {
        Self::new(self.pool.clone(), domain, self.tenant, self.env.clone())
    }
    pub fn new(pool: DomainPool, domain: Domain, tenant: Uuid, env: String) -> Self {
        Self {
            pool,
            domain,
            tenant,
            env,
        }
    }
    fn parameter(&self, index: usize) -> String {
        format!(
            "{}{index}",
            if matches!(self.pool, DomainPool::Postgres(_)) {
                "$"
            } else {
                "?"
            }
        )
    }
    fn scope(&self, start: usize) -> String {
        if self.domain == Domain::Tenant {
            return format!("id={}", self.parameter(start));
        }
        format!(
            "tenant_id={} AND env={}",
            self.parameter(start),
            self.parameter(start + 1)
        )
    }
    async fn columns(&self) -> Result<Vec<String>> {
        let table = self.domain.table();
        Ok(match &self.pool {
            DomainPool::Postgres(pool) => sqlx::query_scalar("SELECT column_name::text FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 ORDER BY ordinal_position").bind(&table).fetch_all(pool).await?,
            DomainPool::SQLite(pool) => sqlx::query(&format!("PRAGMA table_info({table})")).fetch_all(pool).await?.iter().map(|row| row.get("name")).collect(),
        })
    }
    fn json_column(name: &str) -> bool {
        matches!(
            name,
            "configuration"
                | "default_parameters"
                | "dag_config"
                | "properties"
                | "secrets"
                | "auth_config"
                | "middleware"
                | "labels"
                | "intent"
                | "prepared_submission"
        )
    }
    async fn projection(&self) -> Result<String> {
        if matches!(self.pool, DomainPool::Postgres(_)) {
            return Ok(if self.domain == Domain::Ledger {
                "to_jsonb(t) || jsonb_build_object('balances',COALESCE((SELECT jsonb_agg(jsonb_build_object('asset',b.asset,'available',b.available::text,'locked',b.locked::text)) FROM wlt_balance b WHERE b.wallet_id=t.id),'[]'::jsonb),'entries',COALESCE((SELECT jsonb_agg(to_jsonb(e)) FROM (SELECT * FROM wlt_ledger l WHERE l.wallet_id=t.id ORDER BY ts DESC LIMIT 100) e),'[]'::jsonb))".into()
            } else {
                "to_jsonb(t)".into()
            });
        }
        let mut fields = self
            .columns()
            .await?
            .into_iter()
            .map(|column| {
                if Self::json_column(&column) {
                    format!("'{column}',json(t.\"{column}\")")
                } else {
                    format!("'{column}',t.\"{column}\"")
                }
            })
            .collect::<Vec<_>>();
        if self.domain == Domain::Ledger {
            fields.push("'balances',json(COALESCE((SELECT json_group_array(json_object('asset',asset,'available',CAST(available AS TEXT),'locked',CAST(locked AS TEXT))) FROM wlt_balance b WHERE b.wallet_id=t.id),'[]'))".into());
            fields.push("'entries',json(COALESCE((SELECT json_group_array(json_object('order_id',order_id,'trade_id',trade_id,'symbol',symbol,'side',side,'price',CAST(price AS TEXT),'qty',CAST(qty AS TEXT),'ts',ts)) FROM (SELECT * FROM wlt_ledger l WHERE l.wallet_id=t.id ORDER BY ts DESC LIMIT 100)),'[]'))".into());
        }
        Ok(format!("json_object({})", fields.join(",")))
    }
    fn guard(&self, deleting: bool) -> String {
        match self.domain {
            Domain::Tenant if deleting => " AND NOT EXISTS(SELECT 1 FROM workflow_info w WHERE w.tenant_id=tenant_info.id) AND NOT EXISTS(SELECT 1 FROM ledger_info l WHERE l.tenant_id=tenant_info.id)".into(),
            Domain::Workflow => " AND status IN ('STOPPED','SUCCESS','FAILED')".into(),
            Domain::Cluster => " AND NOT EXISTS(SELECT 1 FROM wf_run r WHERE r.cluster_id=cluster_info.id AND r.state IN ('queued','running'))".into(),
            Domain::Order => " AND state='pending' AND prepared_submission IS NULL AND NOT EXISTS(SELECT 1 FROM ord_execution e WHERE e.wallet_id=order_info.wallet_id AND e.reservation_node_id=order_info.producer_node_id AND e.run_id=order_info.run_id AND e.reservation_seq=order_info.seq)".into(),
            Domain::Ledger => " AND portfolio_version=0 AND NOT EXISTS(SELECT 1 FROM wlt_balance b WHERE b.wallet_id=ledger_info.id AND (b.available<>0 OR b.locked<>0))".into(),
            _ => String::new(),
        }
    }
    async fn write(&self, mut value: Value, update: bool) -> Result<Uuid> {
        let object = value.as_object_mut().context("entity must be an object")?;
        let id: Uuid = serde_json::from_value(object.get("id").cloned().context("id required")?)?;
        let available = self.columns().await?;
        ensure!(
            object.keys().all(|column| available.contains(column)),
            "unknown entity field"
        );
        let fields = object
            .keys()
            .filter(|key| !update || !matches!(key.as_str(), "id" | "tenant_id" | "env" | "created_at" | "created_by"))
            .cloned()
            .collect::<Vec<_>>();
        ensure!(!fields.is_empty(), "empty entity update");
        let table = self.domain.table();
        let expressions = fields
            .iter()
            .map(|column| match &self.pool {
                DomainPool::Postgres(_) => format!("(jsonb_populate_record(NULL::{table},$1)).\"{column}\""),
                DomainPool::SQLite(_) => format!("json_extract(?1,'$.{column}')"),
            })
            .collect::<Vec<_>>();
        let sql = if update {
            let assignments = fields
                .iter()
                .zip(expressions)
                .map(|(key, expr)| format!("\"{key}\"={expr}"))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "UPDATE {table} SET {assignments} WHERE id={} AND {}{} RETURNING id",
                self.parameter(2),
                self.scope(3),
                self.guard(false)
            )
        } else {
            format!(
                "INSERT INTO {table} ({}) SELECT {} RETURNING id",
                fields
                    .iter()
                    .map(|key| format!("\"{key}\""))
                    .collect::<Vec<_>>()
                    .join(","),
                expressions.join(",")
            )
        };
        match &self.pool {
            DomainPool::Postgres(pool) => {
                let mut query = sqlx::query_scalar::<_, Uuid>(&sql).bind(&value);
                if update {
                    query = query.bind(id).bind(self.tenant);
                    if self.domain != Domain::Tenant {
                        query = query.bind(&self.env);
                    }
                }
                query
                    .fetch_optional(pool)
                    .await?
                    .context("entity is missing or immutable")
            }
            DomainPool::SQLite(pool) => {
                let mut query = sqlx::query_scalar::<_, String>(&sql).bind(value.to_string());
                if update {
                    query = query.bind(id.to_string()).bind(self.tenant.to_string());
                    if self.domain != Domain::Tenant {
                        query = query.bind(&self.env);
                    }
                }
                Ok(query
                    .fetch_optional(pool)
                    .await?
                    .context("entity is missing or immutable")?
                    .parse()?)
            }
        }
    }
    pub async fn list(&self, limit: u32, offset: u32) -> Result<Vec<Value>> {
        let sql = format!(
            "SELECT {} FROM {} t WHERE {} ORDER BY id LIMIT {} OFFSET {}",
            self.projection().await?,
            self.domain.table(),
            self.scope(1),
            limit.clamp(1, 200),
            offset
        );
        match &self.pool {
            DomainPool::Postgres(pool) => {
                let mut q = sqlx::query_scalar(&sql).bind(self.tenant);
                if self.domain != Domain::Tenant {
                    q = q.bind(&self.env);
                }
                Ok(q.fetch_all(pool).await?)
            }
            DomainPool::SQLite(pool) => {
                let mut q = sqlx::query_scalar::<_, String>(&sql).bind(self.tenant.to_string());
                if self.domain != Domain::Tenant {
                    q = q.bind(&self.env);
                }
                q.fetch_all(pool)
                    .await?
                    .iter()
                    .map(|s| Ok(serde_json::from_str(s)?))
                    .collect()
            }
        }
    }
    pub async fn get(&self, id: Uuid) -> Result<Option<Value>> {
        let sql = format!(
            "SELECT {} FROM {} t WHERE id={} AND {}",
            self.projection().await?,
            self.domain.table(),
            self.parameter(1),
            self.scope(2)
        );
        match &self.pool {
            DomainPool::Postgres(pool) => {
                let mut q = sqlx::query_scalar(&sql).bind(id).bind(self.tenant);
                if self.domain != Domain::Tenant {
                    q = q.bind(&self.env);
                }
                Ok(q.fetch_optional(pool).await?)
            }
            DomainPool::SQLite(pool) => {
                let mut q = sqlx::query_scalar::<_, String>(&sql)
                    .bind(id.to_string())
                    .bind(self.tenant.to_string());
                if self.domain != Domain::Tenant {
                    q = q.bind(&self.env);
                }
                q.fetch_optional(pool)
                    .await?
                    .map(|s| Ok(serde_json::from_str(&s)?))
                    .transpose()
            }
        }
    }
}

#[async_trait]
impl IGenericRepository<Value> for DomainRepository {
    async fn select(&self, _param: Value, page: PageRequest) -> Result<(PageResponse, Vec<Value>)> {
        let data = self.list(page.get_limit(), page.get_offset()).await?;
        Ok((
            PageResponse::new(None, Some(page.get_offset()), Some(page.get_limit())),
            data,
        ))
    }
    async fn select_by(&self, id: Uuid) -> Result<Value> {
        self.get(id).await?.context("entity not found")
    }
    async fn insert(&self, value: Value) -> Result<Uuid> {
        self.write(value, false).await
    }
    async fn upsert(&self, value: Value) -> Result<Uuid> {
        let id = serde_json::from_value(value["id"].clone())?;
        if self.get(id).await?.is_some() {
            self.write(value, true).await
        } else {
            self.write(value, false).await
        }
    }
    async fn update(&self, value: Value) -> Result<Uuid> {
        self.write(value, true).await
    }
    async fn delete_all(&self) -> Result<u64> {
        bail!("bulk domain deletion is forbidden")
    }
    async fn delete_by_id(&self, id: Uuid) -> Result<u64> {
        let sql = format!(
            "DELETE FROM {} WHERE id={} AND {}{}",
            self.domain.table(),
            self.parameter(1),
            self.scope(2),
            self.guard(true)
        );
        Ok(match &self.pool {
            DomainPool::Postgres(pool) => {
                let mut q = sqlx::query(&sql).bind(id).bind(self.tenant);
                if self.domain != Domain::Tenant {
                    q = q.bind(&self.env);
                }
                q.execute(pool).await?.rows_affected()
            }
            DomainPool::SQLite(pool) => {
                let mut q = sqlx::query(&sql).bind(id.to_string()).bind(self.tenant.to_string());
                if self.domain != Domain::Tenant {
                    q = q.bind(&self.env);
                }
                q.execute(pool).await?.rows_affected()
            }
        })
    }
}
