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

use crate::cache::CacheContainer;

use crate::cache::ICache;

use crate::config::config::AppConfig;

use crate::config::config::StorageProvider;

use crate::modules::workflow::storage::postgres::WorkflowPostgresRepository;

use crate::modules::workflow::storage::sqlite::WorkflowSQLiteRepository;

use crate::storage::RepositoryContainer;

use sigbot_types::workflow::workflow::WorkflowInfo;
use sigbot_utils::httpclients;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SigbotState {
    pub config: Arc<AppConfig>,
    // The Basic operators.
    pub string_cache: Arc<Box<dyn ICache<String>>>,
    pub authguard: Arc<crate::context::authguard::AuthguardProvider>,
    pub default_http_client: Arc<reqwest::Client>,
    // The Service module repositories.
    pub workflow_repo: Arc<Mutex<RepositoryContainer<WorkflowInfo>>>,
}

impl SigbotState {
    pub async fn sqlite_pool(&self) -> anyhow::Result<sqlx::SqlitePool> {
        anyhow::ensure!(
            matches!(self.config.storage.provider, StorageProvider::SQLite),
            "SQLite storage is not configured"
        );
        let repository =
            crate::storage::sqlite::SQLiteRepository::<WorkflowInfo>::get_or_init(&self.config.storage.sqlite).await?;
        Ok(repository.get_pool().clone())
    }

    pub async fn postgres_pool(&self) -> anyhow::Result<sqlx::PgPool> {
        anyhow::ensure!(
            matches!(self.config.storage.provider, StorageProvider::Postgres),
            "runtime projections require PostgreSQL"
        );
        let repository =
            crate::storage::postgres::PostgresRepository::<WorkflowInfo>::get_or_init(&self.config.storage.postgres)
                .await?;
        Ok(repository.get_pool().clone())
    }

    pub async fn new(config: &Arc<AppConfig>) -> Self {
        // Build tooling http client.
        let http_client = httpclients::build_default();

        let db_config = &config.storage;
        // --- The Service module repositories. ---

        let workflow_repo = RepositoryContainer::new(
            match db_config.provider {
                StorageProvider::SQLite => Some(Box::new(
                    WorkflowSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.provider {
                StorageProvider::Postgres => Some(Box::new(
                    WorkflowPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
        );

        // Build DI container.
        // let mut di_container = syrette::DIContainer::new();
        // di_container.bind::<dyn IUserHandler>().to::<UserHandler>()?;

        SigbotState {
            // Notice: Arc object clone only increments the reference counter, and does not copy the actual data block.
            config: config.clone(),
            // The basic operators.
            string_cache: Arc::new(CacheContainer::<String>::new()),
            authguard: Arc::new(
                crate::context::authguard::AuthguardProvider::from_config(&config.authguard).unwrap_or_else(|_| {
                    crate::context::authguard::AuthguardProvider::deny_all(config.authguard.clone())
                }),
            ),
            default_http_client: Arc::new(http_client),
            // The Application repositories.
            workflow_repo: Arc::new(Mutex::new(workflow_repo)),
        }
    }
}
