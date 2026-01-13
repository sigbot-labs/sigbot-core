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

pub mod dlock_mongo;
pub mod dlock_postgres;
pub mod dlock_sqlite;
pub mod log_mongo;
pub mod log_postgres;
pub mod log_sqlite;
pub mod log_timescaledb;
pub mod tenant_mongo;
pub mod tenant_postgres;
pub mod tenant_sqlite;
pub mod user_mongo;
pub mod user_postgres;
pub mod user_sqlite;

// Log-specific operations trait and container
use crate::config::config::{AppDBProperties, AppDBType};
use anyhow::Error;
use async_trait::async_trait;
use chrono::Utc;
use sigbot_types::sys::log::LogInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;
use std::sync::Arc;

use crate::sys::store::log_mongo::LogMongoRepository;
use crate::sys::store::log_postgres::LogPostgresRepository;
use crate::sys::store::log_sqlite::LogSQLiteRepository;
use crate::sys::store::log_timescaledb::LogTimescaleDBRepository;

// Log-specific operations trait
#[async_trait]
pub trait ILogRepository: Send + Sync {
    async fn append(&self, log: LogInfo) -> Result<i64, Error>;
    async fn tail(
        &self,
        service_name: Option<String>,
        log_type: Option<String>,
        keyword: Option<String>,
        limit: Option<u32>,
        since: Option<chrono::DateTime<Utc>>,
    ) -> Result<Vec<LogInfo>, Error>;
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
    ) -> Result<(PageResponse, Vec<LogInfo>), Error>;
    async fn stats(
        &self,
        service_name: Option<String>,
        log_type: Option<String>,
        start_time: Option<chrono::DateTime<Utc>>,
        end_time: Option<chrono::DateTime<Utc>>,
    ) -> Result<sigbot_types::sys::log::LogStats, Error>;
}

pub struct LogRepositoryContainer {
    sqlite_repo: Option<Arc<dyn ILogRepository>>,
    postgres_repo: Option<Arc<dyn ILogRepository>>,
    mongo_repo: Option<Arc<dyn ILogRepository>>,
    timescaledb_repo: Option<Arc<dyn ILogRepository>>,
}

impl LogRepositoryContainer {
    pub async fn new(config: &AppDBProperties) -> Result<Self, Error> {
        Ok(LogRepositoryContainer {
            sqlite_repo: match config.db_type {
                AppDBType::SQLITE => Some(Arc::new(LogSQLiteRepository::new(&config.sqlite).await?)),
                _ => None,
            },
            postgres_repo: match config.db_type {
                AppDBType::POSTGRESQL => Some(Arc::new(LogPostgresRepository::new(&config.postgres).await?)),
                _ => None,
            },
            mongo_repo: match config.db_type {
                AppDBType::MONGODB => Some(Arc::new(LogMongoRepository::new(&config.mongodb).await?)),
                _ => None,
            },
            timescaledb_repo: match config.db_type {
                AppDBType::TIMESCALEDB => Some(Arc::new(LogTimescaleDBRepository::new(&config.timescaledb).await?)),
                _ => None,
            },
        })
    }

    fn sqlite_repo(&self) -> &dyn ILogRepository {
        self.sqlite_repo
            .as_ref()
            .map(|repo| &**repo)
            .expect("The sqlite log repository not configured.")
    }

    fn postgres_repo(&self) -> &dyn ILogRepository {
        self.postgres_repo
            .as_ref()
            .map(|repo| &**repo)
            .expect("The postgresql log repository not configured.")
    }

    fn mongo_repo(&self) -> &dyn ILogRepository {
        self.mongo_repo
            .as_ref()
            .map(|repo| &**repo)
            .expect("The mongodb log repository not configured.")
    }

    fn timescaledb_repo(&self) -> &dyn ILogRepository {
        self.timescaledb_repo
            .as_ref()
            .map(|repo| &**repo)
            .expect("The timescaledb log repository not configured.")
    }

    pub fn get(&self, config: &AppDBProperties) -> &dyn ILogRepository {
        match config.db_type {
            AppDBType::SQLITE => self.sqlite_repo(),
            AppDBType::POSTGRESQL => self.postgres_repo(),
            AppDBType::MONGODB => self.mongo_repo(),
            AppDBType::TIMESCALEDB => self.timescaledb_repo(),
        }
    }
}
