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

use crate::{
    cache::{memory::StringMemoryCache, redis::StringRedisCache, CacheContainer},
    config::config::{AppConfig, AppDBType},
    llm::handler::llm_engine::{ILLMManager, LLMEngine},
    mgmt::health::{MongoChecker, RedisClusterChecker, SQLiteChecker},
    modules::{
        exchange::store::{
            exchange_mongo::ExchangeInfoMongoRepository, exchange_postgres::ExchangeInfoPostgresRepository,
            exchange_sqlite::ExchangeInfoSQLiteRepository,
        },
        strategy::store::{
            strategy_mongo::StrategyInfoMongoRepository, strategy_postgres::StrategyInfoPostgresRepository,
            strategy_sqlite::StrategyInfoSQLiteRepository,
        },
    },
    store::RepositoryContainer,
    sys::store::{
        dlock_postgres::DLockPostgresRepository, user_mongo::UserMongoRepository,
        user_postgres::UserPostgresRepository, user_sqlite::UserSQLiteRepository,
    },
};

use oauth2::basic::BasicClient;
use sigbot_types::{
    modules::{exchange::exchange::ExchangeInfo, strategy::strategy::StrategyInfo},
    sys::{dlock::DLock, user::User},
};
use sigbot_utils::httpclients;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SigbotState {
    pub config: Arc<AppConfig>,
    // The Basic operators.
    pub string_cache: Arc<CacheContainer<String>>,
    pub oidc_client: Option<Arc<openidconnect::core::CoreClient>>,
    pub github_client: Option<Arc<BasicClient>>,
    pub default_http_client: Arc<reqwest::Client>,
    // The Health checker.
    pub sqlite_checker: SQLiteChecker,
    pub mongo_checker: MongoChecker,
    pub redis_cluster_checker: RedisClusterChecker,
    // The System module repositories.
    pub user_repo: Arc<Mutex<RepositoryContainer<User>>>,
    pub lock_repo: Arc<Mutex<RepositoryContainer<DLock>>>,
    // The Service module repositories.
    pub exchange_repo: Arc<Mutex<RepositoryContainer<ExchangeInfo>>>,
    pub strategy_repo: Arc<Mutex<RepositoryContainer<StrategyInfo>>>,
    pub llm_handler: Arc<dyn ILLMManager + Send + Sync>,
}

impl SigbotState {
    pub async fn new(config: &Arc<AppConfig>) -> Self {
        let cache_config = &config.cache;

        // Build cacher.
        let cache_container = CacheContainer::new(
            Box::new(StringMemoryCache::new(&cache_config.memory)),
            Box::new(StringRedisCache::new(&cache_config.redis)),
        );

        // Build auth clients.
        let auth_clients = (
            crate::util::oidcs::create_oidc_client(&config.auth.oidc)
                .await
                .map(|client| Arc::new(client)),
            crate::util::oauth2::create_oauth2_client(&config.auth.github)
                .await
                .map(|client| Arc::new(client)),
        );

        // Build tooling http client.
        let http_client = httpclients::build_default();

        // --- The System module repositories. ---

        let db_config = &config.appdb;
        let user_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(UserSQLiteRepository::new(&db_config.sqlite).await.unwrap())),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    UserPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(UserMongoRepository::new(&db_config.mongodb).await.unwrap())),
                _ => None,
            },
        );
        let lock_repo = RepositoryContainer::new(
            None,
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    DLockPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            None,
        );

        // --- The Service module repositories. ---

        let exchange_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    ExchangeInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    ExchangeInfoPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    ExchangeInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
        let strategy_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    StrategyInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    StrategyInfoPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    StrategyInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );

        let app_state = SigbotState {
            // Notice: Arc object clone only increments the reference counter, and does not copy the actual data block.
            config: config.clone(),
            // The basic operators.
            string_cache: Arc::new(cache_container),
            oidc_client: auth_clients.0,
            github_client: auth_clients.1,
            default_http_client: Arc::new(http_client),
            // The Health checker.
            sqlite_checker: SQLiteChecker::new(),
            mongo_checker: MongoChecker::new(),
            redis_cluster_checker: RedisClusterChecker::new(),
            // The System repositories.
            user_repo: Arc::new(Mutex::new(user_repo)),
            lock_repo: Arc::new(Mutex::new(lock_repo)),
            // The Application repositories.
            exchange_repo: Arc::new(Mutex::new(exchange_repo)),
            strategy_repo: Arc::new(Mutex::new(strategy_repo)),
            llm_handler: LLMEngine::get_default_implementation(),
        };

        // Build DI container.
        // let mut di_container = syrette::DIContainer::new();
        // di_container.bind::<dyn IUserHandler>().to::<UserHandler>()?;

        app_state
    }
}
