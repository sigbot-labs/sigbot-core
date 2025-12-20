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

use super::AsyncRepository;
use crate::config::config::PostgresAppDBProperties;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, error, info};
use sigbot_types::{PageRequest, PageResponse};
use sqlx::migrate::MigrateDatabase;
use sqlx::{PgPool, Postgres};
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::OnceCell;

// 单例的 Postgres pool 管理器
struct PostgresPoolManager {
    pool: Arc<PgPool>,
}

static POOL_MANAGER: OnceCell<Arc<PostgresPoolManager>> = OnceCell::const_new();

async fn init_pool_manager(config: PostgresAppDBProperties) -> Result<Arc<PostgresPoolManager>, Error> {
    PostgresPoolManager::init(&config).await
}

impl PostgresPoolManager {
    async fn init(config: &PostgresAppDBProperties) -> Result<Arc<Self>, Error> {
        let db_url = format!(
            "postgres://{}:{}@{}:{}/{}",
            config.username,
            config.password.as_deref().unwrap_or(""),
            config.host,
            config.port,
            config.database
        );

        if !Postgres::database_exists(&db_url).await.unwrap_or(false) {
            info!("Creating database {}", db_url);
            match Postgres::create_database(&db_url).await {
                Ok(_) => info!("Create db success"),
                Err(error) => panic!("Error to create db: {}", error),
            }
        } else {
            debug!("The postgres database already exists, skip migration.");
        }

        match PgPool::connect(&db_url).await {
            Ok(pool) => {
                info!("Connected to the postgres database to migration ...");
                let pool = Self::run_migration(pool).await;
                Ok(Arc::new(PostgresPoolManager { pool: Arc::new(pool) }))
            }
            Err(e) => {
                error!("Failed to connect to the postgres database to migration: {}", e);
                Err(e.into())
            }
        }
    }

    async fn run_migration(pool: PgPool) -> PgPool {
        let results = sqlx::migrate!("../../tooling/deploy/migrations").run(&pool).await;
        info!("The postgres database migration result: {:?}", results);
        match results {
            Ok(_) => info!("The postgres database migration successfully."),
            Err(error) => {
                panic!("Error to migrate the postgres database: {}", error);
            }
        }
        pool
    }
}

pub struct PostgresRepository<T: Any + Send + Sync> {
    phantom: PhantomData<T>,
    pool: Arc<PgPool>,
}

impl<T: Any + Send + Sync> PostgresRepository<T> {
    pub async fn get_or_init(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        let config = config.clone();
        let manager = POOL_MANAGER
            .get_or_try_init(|| async move { init_pool_manager(config).await })
            .await?;
        Ok(PostgresRepository {
            phantom: PhantomData,
            pool: manager.pool.clone(),
        })
    }

    pub fn get_pool(&self) -> &PgPool {
        &*self.pool
    }
}

#[allow(unused)]
#[async_trait]
impl<T: Any + Send + Sync> AsyncRepository<T> for PostgresRepository<T> {
    async fn select(&self, mut param: T, page: PageRequest) -> Result<(PageResponse, Vec<T>), Error> {
        unimplemented!("select not implemented for PostgresRepository")
    }

    async fn select_by_id(&self, id: i64) -> Result<T, Error> {
        unimplemented!("select_by_id not implemented for PostgresRepository")
    }

    async fn insert(&self, param: T) -> Result<i64, Error> {
        unimplemented!("insert not implemented for PostgresRepository");
        let pool = self.get_pool();
    }

    async fn update(&self, param: T) -> Result<i64, Error> {
        unimplemented!("update not implemented for PostgresRepository")
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        unimplemented!("delete_all not implemented for PostgresRepository")
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        unimplemented!("delete_by_id not implemented for PostgresRepository")
    }
}

#[macro_export]
macro_rules! dynamic_postgres_query {
    ($bean:expr, $table:expr, $pool:expr, $order_by:expr, $page:expr, $($t:ty),+) => {
        {
            use chrono::{DateTime, Utc};
            use sigbot_utils::types::GenericValue;
            // Notice:
            // 1. (SQLite) Because the ORM library is not used for the time being, the fields are dynamically
            // parsed based on serde_json, so the #[serde(rename="xx")] annotation is effective.
            // 2. (MongoDB) The underlying BSON serialization is also based on serde, so using #[serde(rename="xx")] is also valid
            // TODO: It is recommended to use an ORM framework, see: https://github.com/diesel-rs/diesel
            let serialized = serde_json::to_value(&$bean).unwrap();
            let obj = serialized.as_object().unwrap();
            let mut fields = Vec::new();
            let mut params = Vec::new();
            let mut index = 0;
            for (key, value) in obj {
                if !value.is_null() {
                    let v = value.as_str().unwrap_or("");
                    if !v.is_empty() {
                        index += 1;
                        // Notice: Must use $x expression? otherwise, the sqlx will not work.
                        // e.g: "SELECT COUNT(1) as count FROM my_table WHERE created_at = $1 AND updated_at = $2"
                        fields.push(format!("{} = ${}", key, index));
                        if key == "created_at" || key == "updated_at" {
                            let dt = DateTime::parse_from_rfc3339(v)?;
                            params.push(GenericValue::DateTime(dt.with_timezone(&Utc)));
                        } else {
                            params.push(GenericValue::String(v.to_string()));
                        }
                    }
                }
            }
            if let Some(id) = $bean.base.id {
                fields.push("id = ?".to_string());
                params.push(GenericValue::Int64(id));
            }
            let where_clause = if fields.is_empty() {
                "1=1".to_string()
            } else {
                fields.join(" AND ")
            };
            // Queries to get total count.
            let total_query = format!("SELECT COUNT(1) FROM {} WHERE {}", $table, where_clause);
            use sqlx::Row;
            let mut total_operator = sqlx::query(&total_query);
            for param in params.iter() {
                if let GenericValue::Bool(v) = param {
                    total_operator = total_operator.bind(v);
                } else if let GenericValue::Int64(v) = param {
                    total_operator = total_operator.bind(v);
                } else if let GenericValue::Float64(v) = param {
                    total_operator = total_operator.bind(v);
                } else if let GenericValue::String(v) = param {
                    total_operator = total_operator.bind(v);
                } else if let GenericValue::DateTime(v) = param {
                    total_operator = total_operator.bind(v);
                }
            }
            let total_count = total_operator.fetch_one($pool).await?.get::<i64, _>(0);

            // Queries to get data.
            let query = format!("SELECT * FROM {} WHERE {} AND del_flag = 0 ORDER BY {} LIMIT {} OFFSET {}",
                  $table, where_clause, $order_by, $page.get_limit(), $page.get_offset());
            let mut operator = sqlx::query_as::<_, $($t),+>(&query);
            for param in params.iter() {
                if let GenericValue::Bool(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::Int64(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::Float64(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::String(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::DateTime(v) = param {
                    operator = operator.bind(v);
                }
            }
            match operator.fetch_all($pool).await {
                std::result::Result::Ok(result) => {
                    let p = PageResponse::new(
                      Some(total_count),
                      Some($page.get_offset()),
                      Some($page.get_limit()));
                    Ok((p, result))
                },
                Err(error) => {
                    Err(error.into())
                }
            }
        }
    };
}

#[macro_export]
macro_rules! dynamic_postgres_insert {
    ($bean:expr, $table:expr, $pool:expr) => {
        {
            use chrono::{DateTime, Utc};
            use sigbot_utils::types::GenericValue;
            use crate::util::auths::SecurityContext;

            let insert_by = SecurityContext::get_instance().get_current_uname_for_store().await;

            // Notice:
            // 1. (SQLite/Postgres) Because the ORM library is not used for the time being, the fields are dynamically
            // parsed based on serde_json, so the #[serde(rename="xx")] annotation is effective.
            // 2. (MongoDB) The underlying BSON serialization is also based on serde, so using #[serde(rename="xx")] is also valid
            // TODO: It is recommended to use an ORM framework, see: https://github.com/diesel-rs/diesel
            $bean.base.pre_insert(insert_by).await;

            let serialized = serde_json::to_value($bean).unwrap();
            let obj = serialized.as_object().unwrap();

            let mut fields = Vec::new();
            let mut values = Vec::new();
            let mut params = Vec::new();
            for (key, value) in obj {
                if !value.is_null() {
                    if value.is_boolean() {
                        let v = value.as_bool().unwrap();
                        fields.push(key.as_str());
                        values.push("?");
                        params.push(GenericValue::Bool(v));
                    } else if value.is_number() {
                        if value.is_i64() {
                            let v = value.as_i64().unwrap();
                            fields.push(key.as_str());
                            values.push("?");
                            params.push(GenericValue::Int64(v));
                        } else if value.is_f64() {
                            let v = value.as_f64().unwrap();
                            fields.push(key.as_str());
                            values.push("?");
                            params.push(GenericValue::Float64(v));
                        }
                    } else if value.is_string() {
                        let v = value.as_str().unwrap_or("");
                        if !v.is_empty() {
                            fields.push(key.as_str());
                            values.push("?");
                            if key == "created_at" || key == "updated_at" {
                                let dt = DateTime::parse_from_rfc3339(v)?;
                                params.push(GenericValue::DateTime(dt.with_timezone(&Utc)));
                            } else {
                                params.push(GenericValue::String(v.to_string()));
                            }
                        }
                    }
                }
            }
            if fields.is_empty() {
                return Ok(-1);
            }

            // e.g: 'INSERT INTO sys_user ( id, name ) VALUES ( 2, "John Doe" ) ON CONFLICT ( id ) DO UPDATE SET updated_at = CURRENT_TIMESTAMP(13) RETURNING id;'
            let query = format!("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {} RETURNING id",
                $table, fields.join(","), values.join(","), "updated_at = CURRENT_TIMESTAMP(13)");

            let mut operator = sqlx::query(&query);
            for param in params.iter() {
                if let GenericValue::Bool(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::Int64(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::Float64(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::String(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::DateTime(v) = param {
                    operator = operator.bind(v);
                }
            }

            match operator.fetch_one($pool).await {
                std::result::Result::Ok(row) => {
                    use sqlx::Row;
                    let inserted_id: i64 = row.get("id");
                    Ok(inserted_id)
                },
                Err(e) => Err(Error::from(e)),
            }
        }
    };
}

#[macro_export]
macro_rules! dynamic_postgres_update {
    ($bean:expr, $table:expr, $pool:expr) => {
        {
            use sigbot_utils::types::GenericValue;
            use crate::util::auths::SecurityContext;

            let updated_by = SecurityContext::get_instance().get_current_uname_for_store().await;
            $bean.base.pre_update(updated_by).await;

            // Notice:
            // 1. (SQLite) Because the ORM library is not used for the time being, the fields are dynamically
            // parsed based on serde_json, so the #[serde(rename="xx")] annotation is effective.
            // 2. (MongoDB) The underlying BSON serialization is also based on serde, so using #[serde(rename="xx")] is also valid
            // TODO: It is recommended to use an ORM framework, see: https://github.com/diesel-rs/diesel
            let id = $bean.base.id.unwrap();
            let serialized = serde_json::to_value($bean).unwrap();
            let obj = serialized.as_object().unwrap();

            let mut fields = Vec::new();
            let mut params = Vec::new();
            for (key, value) in obj {
                if !value.is_null() {
                    if value.is_boolean() {
                        let v = value.as_bool().unwrap();
                        fields.push(format!("{} = ?", key));
                        params.push(GenericValue::Bool(v));
                    } else if value.is_number() {
                        let v = value.as_i64().unwrap();
                        fields.push(format!("{} = ?", key));
                        params.push(GenericValue::Int64(v));
                    } else if value.is_string() {
                        let v = value.as_str().unwrap_or("");
                        if !v.is_empty() {
                            fields.push(format!("{} = ?", key));
                            params.push(GenericValue::String(v.to_string()));
                        }
                    }
                }
            }
            if fields.is_empty() {
                return Ok(0);
            }

            let query = format!("UPDATE {} SET {} WHERE id = ?", $table, fields.join(", "));
            let mut operator = sqlx::query(&query);
            for param in params.iter() {
                if let GenericValue::Bool(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::Int64(v) = param {
                    operator = operator.bind(v);
                } else if let GenericValue::String(v) = param {
                    operator = operator.bind(v);
                }
            }
            operator = operator.bind(id);

            match operator.execute($pool).await {
                std::result::Result::Ok(result) => {
                    if result.rows_affected() > 0 {
                        return Ok(id);
                    } else {
                        return Ok(-1);
                    }
                },
                Err(e) => Err(Error::from(e)),
            }
        }
    };
}
