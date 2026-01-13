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

use crate::config::config::PostgresAppDBProperties;
use crate::store::postgres::PostgresRepository;
use crate::store::IAsyncRepository;
use crate::sys::store::ILogRepository;
use anyhow::Error;
use async_trait::async_trait;
use chrono::Utc;
use common_telemetry::info;
use sigbot_types::sys::log::LogInfo;
use sigbot_types::{PageRequest, PageResponse};
use sqlx::Row;
use std::collections::HashMap;

pub struct LogPostgresRepository {
    inner: PostgresRepository<LogInfo>,
}

impl LogPostgresRepository {
    pub async fn new(config: &PostgresAppDBProperties) -> Result<Self, Error> {
        Ok(LogPostgresRepository {
            inner: PostgresRepository::get_or_init(config).await?,
        })
    }

    pub fn get_pool(&self) -> &sqlx::PgPool {
        self.inner.get_pool()
    }
}

#[async_trait]
impl IAsyncRepository<LogInfo> for LogPostgresRepository {
    async fn select(&self, _: LogInfo, _: PageRequest) -> Result<(PageResponse, Vec<LogInfo>), Error> {
        Err(Error::msg("Log select are not supported"))
    }

    async fn select_by_id(&self, _: i64) -> Result<LogInfo, Error> {
        Err(Error::msg("Log select_by_id are not supported"))
    }

    async fn upsert(&self, _: LogInfo) -> Result<i64, Error> {
        Err(Error::msg("Log upsert are not supported"))
    }

    async fn update(&self, _log: LogInfo) -> Result<i64, Error> {
        Err(Error::msg("Log updates are not supported"))
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        Err(Error::msg("Log delete_all are not supported"))
    }

    async fn delete_by_id(&self, id: i64) -> Result<u64, Error> {
        Err(Error::msg("Log delete_by_id are not supported"))
    }
}

#[async_trait]
impl ILogRepository for LogPostgresRepository {
    async fn append(&self, mut log: LogInfo) -> Result<i64, Error> {
        // Logs are append-only, use insert instead of upsert
        use crate::util::auths::SecurityContext;
        let insert_by = SecurityContext::get_instance().get_current_uname_for_store().await;
        log.base.pre_insert(insert_by).await;

        let result = sqlx::query(
            r#"
            INSERT INTO sys_log (
                service_name, log_type, level, content, source, tags,
                created_by, created_at, updated_by, updated_at, del_flag
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
            "#,
        )
        .bind(&log.service_name)
        .bind(&log.log_type)
        .bind(&log.level)
        .bind(&log.content)
        .bind(&log.source)
        .bind(&log.tags)
        .bind(&log.base.created_by)
        .bind(log.base.created_at)
        .bind(&log.base.updated_by)
        .bind(log.base.updated_at)
        .bind(log.base.del_flag)
        .fetch_one(self.get_pool())
        .await?;

        let id: i64 = result.get("id");
        info!("Inserted log.id: {}", id);
        Ok(id)
    }

    async fn tail(
        &self,
        service_name: Option<String>,
        log_type: Option<String>,
        keyword: Option<String>,
        limit: Option<u32>,
        since: Option<chrono::DateTime<Utc>>,
    ) -> Result<Vec<LogInfo>, Error> {
        let limit = limit.unwrap_or(100);
        let mut conditions = Vec::new();
        conditions.push("del_flag = 0".to_string());
        let mut param_index = 1;

        if service_name.is_some() {
            conditions.push(format!("service_name = ${}", param_index));
            param_index += 1;
        }
        if log_type.is_some() {
            conditions.push(format!("log_type = ${}", param_index));
            param_index += 1;
        }
        if keyword.is_some() {
            conditions.push(format!("content LIKE ${}", param_index));
            param_index += 1;
        }
        if since.is_some() {
            conditions.push(format!("created_at > ${}", param_index));
            param_index += 1;
        }

        let where_clause = conditions.join(" AND ");
        let query = format!(
            "SELECT * FROM sys_log WHERE {} ORDER BY created_at DESC LIMIT ${}",
            where_clause, param_index
        );

        let mut stmt = sqlx::query_as::<_, LogInfo>(&query);
        if let Some(ref sn) = service_name {
            stmt = stmt.bind(sn);
        }
        if let Some(ref lt) = log_type {
            stmt = stmt.bind(lt);
        }
        if let Some(ref kw) = keyword {
            stmt = stmt.bind(format!("%{}%", kw));
        }
        if let Some(ts) = since {
            stmt = stmt.bind(ts);
        }
        stmt = stmt.bind(limit as i64);

        let logs = stmt.fetch_all(self.get_pool()).await?;
        Ok(logs)
    }

    async fn search(
        &self,
        service_name: Option<String>,
        log_type: Option<String>,
        level: Option<String>,
        source: Option<String>,
        tags: Option<String>,
        keyword: Option<String>,
        start_time: Option<chrono::DateTime<Utc>>,
        end_time: Option<chrono::DateTime<Utc>>,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<LogInfo>), Error> {
        let mut conditions = Vec::new();
        conditions.push("del_flag = 0".to_string());

        let mut param_index = 1;

        if service_name.is_some() {
            conditions.push(format!("service_name = ${}", param_index));
            param_index += 1;
        }
        if log_type.is_some() {
            conditions.push(format!("log_type = ${}", param_index));
            param_index += 1;
        }
        if level.is_some() {
            conditions.push(format!("level = ${}", param_index));
            param_index += 1;
        }
        if source.is_some() {
            conditions.push(format!("source = ${}", param_index));
            param_index += 1;
        }
        if tags.is_some() {
            conditions.push(format!("tags LIKE ${}", param_index));
            param_index += 1;
        }
        if keyword.is_some() {
            conditions.push(format!("content LIKE ${}", param_index));
            param_index += 1;
        }
        if start_time.is_some() {
            conditions.push(format!("created_at >= ${}", param_index));
            param_index += 1;
        }
        if end_time.is_some() {
            conditions.push(format!("created_at <= ${}", param_index));
            param_index += 1;
        }

        let where_clause = conditions.join(" AND ");

        let count_query = format!("SELECT COUNT(1) as count FROM sys_log WHERE {}", where_clause);
        let mut count_stmt = sqlx::query(&count_query);
        if let Some(ref sn) = service_name {
            count_stmt = count_stmt.bind(sn);
        }
        if let Some(ref lt) = log_type {
            count_stmt = count_stmt.bind(lt);
        }
        if let Some(ref lvl) = level {
            count_stmt = count_stmt.bind(lvl);
        }
        if let Some(ref src) = source {
            count_stmt = count_stmt.bind(src);
        }
        if let Some(ref tgs) = tags {
            count_stmt = count_stmt.bind(format!("%{}%", tgs));
        }
        if let Some(ref kw) = keyword {
            count_stmt = count_stmt.bind(format!("%{}%", kw));
        }
        if let Some(st) = start_time {
            count_stmt = count_stmt.bind(st);
        }
        if let Some(et) = end_time {
            count_stmt = count_stmt.bind(et);
        }
        let count_row = count_stmt.fetch_one(self.get_pool()).await?;
        let total_count: i64 = count_row.get("count");

        let data_query = format!(
            "SELECT * FROM sys_log WHERE {} ORDER BY created_at DESC LIMIT ${} OFFSET ${}",
            where_clause,
            param_index,
            param_index + 1
        );
        let mut data_stmt = sqlx::query_as::<_, LogInfo>(&data_query);
        if let Some(ref sn) = service_name {
            data_stmt = data_stmt.bind(sn);
        }
        if let Some(ref lt) = log_type {
            data_stmt = data_stmt.bind(lt);
        }
        if let Some(ref lvl) = level {
            data_stmt = data_stmt.bind(lvl);
        }
        if let Some(ref src) = source {
            data_stmt = data_stmt.bind(src);
        }
        if let Some(ref tgs) = tags {
            data_stmt = data_stmt.bind(format!("%{}%", tgs));
        }
        if let Some(ref kw) = keyword {
            data_stmt = data_stmt.bind(format!("%{}%", kw));
        }
        if let Some(st) = start_time {
            data_stmt = data_stmt.bind(st);
        }
        if let Some(et) = end_time {
            data_stmt = data_stmt.bind(et);
        }
        data_stmt = data_stmt.bind(page.get_limit() as i64);
        data_stmt = data_stmt.bind(page.get_offset() as i64);

        let logs = data_stmt.fetch_all(self.get_pool()).await?;
        let page_response = PageResponse::new(Some(total_count), Some(page.get_offset()), Some(page.get_limit()));
        Ok((page_response, logs))
    }

    async fn stats(
        &self,
        service_name: Option<String>,
        log_type: Option<String>,
        start_time: Option<chrono::DateTime<Utc>>,
        end_time: Option<chrono::DateTime<Utc>>,
    ) -> Result<sigbot_types::sys::log::LogStats, Error> {
        let mut conditions = Vec::new();
        conditions.push("del_flag = 0".to_string());

        let mut param_index = 1;

        if service_name.is_some() {
            conditions.push(format!("service_name = ${}", param_index));
            param_index += 1;
        }
        if log_type.is_some() {
            conditions.push(format!("log_type = ${}", param_index));
            param_index += 1;
        }
        if start_time.is_some() {
            conditions.push(format!("created_at >= ${}", param_index));
            param_index += 1;
        }
        if end_time.is_some() {
            conditions.push(format!("created_at <= ${}", param_index));
            param_index += 1;
        }

        let where_clause = conditions.join(" AND ");

        // Helper macro to bind parameters
        macro_rules! bind_query_params {
            ($stmt:expr) => {{
                let mut stmt = $stmt;
                if let Some(ref sn) = service_name {
                    stmt = stmt.bind(sn);
                }
                if let Some(ref lt) = log_type {
                    stmt = stmt.bind(lt);
                }
                if let Some(st) = start_time {
                    stmt = stmt.bind(st);
                }
                if let Some(et) = end_time {
                    stmt = stmt.bind(et);
                }
                stmt
            }};
        }

        let total_query = format!("SELECT COUNT(1) as count FROM sys_log WHERE {}", where_clause);
        let total_stmt = sqlx::query(&total_query);
        let total_row = bind_query_params!(total_stmt).fetch_one(self.get_pool()).await?;
        let total_count: i64 = total_row.get("count");

        let level_query = format!(
            "SELECT level, COUNT(1) as count FROM sys_log WHERE {} AND level IS NOT NULL GROUP BY level",
            where_clause
        );
        let level_stmt = sqlx::query(&level_query);
        let level_rows = bind_query_params!(level_stmt).fetch_all(self.get_pool()).await?;
        let mut by_level = HashMap::new();
        for row in level_rows {
            let level: String = row.get("level");
            let count: i64 = row.get("count");
            by_level.insert(level, count);
        }

        let log_type_query = format!(
            "SELECT log_type, COUNT(1) as count FROM sys_log WHERE {} AND log_type IS NOT NULL GROUP BY log_type",
            where_clause
        );
        let log_type_stmt = sqlx::query(&log_type_query);
        let log_type_rows = bind_query_params!(log_type_stmt).fetch_all(self.get_pool()).await?;
        let mut by_log_type = HashMap::new();
        for row in log_type_rows {
            let log_type: String = row.get("log_type");
            let count: i64 = row.get("count");
            by_log_type.insert(log_type, count);
        }

        Ok(sigbot_types::sys::log::LogStats {
            total_count,
            by_level,
            by_log_type,
        })
    }
}
