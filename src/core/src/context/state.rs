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
    cache::{CacheContainer, ICache},
    config::config::{AppConfig, AppDBType},
    mgmt::health::{MongoChecker, RedisClusterChecker, SQLiteChecker},
    modules::{
        backtest::store::{
            backtest_case_mongo::BacktestCaseInfoMongoRepository,
            backtest_case_postgres::BacktestCaseInfoPostgresRepository,
            backtest_case_sqlite::BacktestCaseInfoSQLiteRepository,
        },
        datafeed::store::{
            datafeed_mongo::DatafeedInfoMongoRepository, datafeed_postgres::DatafeedInfoPostgresRepository,
            datafeed_sqlite::DatafeedInfoSQLiteRepository,
        },
        exchange::store::{
            exchange_mongo::ExchangeInfoMongoRepository, exchange_postgres::ExchangeInfoPostgresRepository,
            exchange_sqlite::ExchangeInfoSQLiteRepository,
        },
        notification::store::{
            notification_mongo::NotificationInfoMongoRepository,
            notification_postgres::NotificationInfoPostgresRepository,
            notification_sqlite::NotificationInfoSQLiteRepository,
        },
        workflow::store::{
            workflow_mongo::WorkflowInfoMongoRepository,
            workflow_postgres::WorkflowInfoPostgresRepository,
            workflow_sqlite::WorkflowInfoSQLiteRepository,
        },
        strategy::store::{
            strategy_mongo::StrategyInfoMongoRepository, strategy_postgres::StrategyInfoPostgresRepository,
            strategy_sqlite::StrategyInfoSQLiteRepository,
        },
        wallet::store::{
            balance_mongo::BalanceInfoMongoRepository, balance_postgres::BalanceInfoPostgresRepository,
            balance_sqlite::BalanceInfoSQLiteRepository, ledger_mongo::LedgerInfoMongoRepository,
            ledger_postgres::LedgerInfoPostgresRepository, ledger_sqlite::LedgerInfoSQLiteRepository,
            position_mongo::PositionInfoMongoRepository, position_postgres::PositionInfoPostgresRepository,
            position_sqlite::PositionInfoSQLiteRepository, wallet_mongo::WalletInfoMongoRepository,
            wallet_postgres::WalletInfoPostgresRepository, wallet_sqlite::WalletInfoSQLiteRepository,
        },
    },
    store::RepositoryContainer,
    sys::store::{
        dlock_postgres::DLockPostgresRepository, tenant_mongo::TenantMongoRepository,
        tenant_postgres::TenantPostgresRepository, tenant_sqlite::TenantSQLiteRepository,
        user_mongo::UserMongoRepository, user_postgres::UserPostgresRepository, user_sqlite::UserSQLiteRepository,
    },
};
use oauth2::basic::BasicClient;
use sigbot_types::{
    modules::{
        backtest::backtest_case::BacktestCaseInfo,
        datafeed::datafeed::DatafeedInfo,
        exchange::exchange::ExchangeInfo,
        notification::notification::NotificationInfo,
        strategy::strategy::StrategyInfo,
        workflow::workflow::WorkflowInfo,
        wallet::{balance::BalanceInfo, ledger::LedgerInfo, position::PositionInfo, wallet::WalletInfo},
    },
    sys::{dlock::DLock, tenant::Tenant, user::User},
};
use sigbot_utils::httpclients;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SigbotState {
    pub config: Arc<AppConfig>,
    // The Basic operators.
    pub string_cache: Arc<Box<dyn ICache<String>>>,
    pub oidc_client: Option<Arc<openidconnect::core::CoreClient>>,
    pub github_client: Option<Arc<BasicClient>>,
    pub default_http_client: Arc<reqwest::Client>,
    // The Health checker.
    pub sqlite_checker: SQLiteChecker,
    pub mongo_checker: MongoChecker,
    pub redis_cluster_checker: RedisClusterChecker,
    // The System module repositories.
    pub user_repo: Arc<Mutex<RepositoryContainer<User>>>,
    pub tenant_repo: Arc<Mutex<RepositoryContainer<Tenant>>>,
    pub lock_repo: Arc<Mutex<RepositoryContainer<DLock>>>,
    // The Service module repositories.
    pub datafeed_repo: Arc<Mutex<RepositoryContainer<DatafeedInfo>>>,
    pub exchange_repo: Arc<Mutex<RepositoryContainer<ExchangeInfo>>>,
    pub strategy_repo: Arc<Mutex<RepositoryContainer<StrategyInfo>>>,
    pub backtest_case_repo: Arc<Mutex<RepositoryContainer<BacktestCaseInfo>>>,
    pub notification_repo: Arc<Mutex<RepositoryContainer<NotificationInfo>>>,
    pub workflow_repo: Arc<Mutex<RepositoryContainer<WorkflowInfo>>>,
    // The Wallet module repositories.
    pub wallet_repo: Arc<Mutex<RepositoryContainer<WalletInfo>>>,
    pub trade_repo: Arc<Mutex<RepositoryContainer<LedgerInfo>>>,
    pub balance_repo: Arc<Mutex<RepositoryContainer<BalanceInfo>>>,
    pub position_repo: Arc<Mutex<RepositoryContainer<PositionInfo>>>,
}

impl SigbotState {
    pub async fn new(config: &Arc<AppConfig>) -> Self {
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
        let tenant_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(TenantSQLiteRepository::new(&db_config.sqlite).await.unwrap())),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    TenantPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(TenantMongoRepository::new(&db_config.mongodb).await.unwrap())),
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

        let datafeed_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    DatafeedInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    DatafeedInfoPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    DatafeedInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
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
        let backtest_case_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    BacktestCaseInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    BacktestCaseInfoPostgresRepository::new(&db_config.postgres)
                        .await
                        .unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    BacktestCaseInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
        let notification_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    NotificationInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    NotificationInfoPostgresRepository::new(&db_config.postgres)
                        .await
                        .unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    NotificationInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
        let workflow_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    WorkflowInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    WorkflowInfoPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    WorkflowInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
        let wallet_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    WalletInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    WalletInfoPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    WalletInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
        let trade_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    LedgerInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    LedgerInfoPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    LedgerInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
        let balance_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    BalanceInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    BalanceInfoPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    BalanceInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
        let position_repo = RepositoryContainer::new(
            match db_config.db_type {
                AppDBType::SQLITE => Some(Box::new(
                    PositionInfoSQLiteRepository::new(&db_config.sqlite).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::POSTGRESQL => Some(Box::new(
                    PositionInfoPostgresRepository::new(&db_config.postgres).await.unwrap(),
                )),
                _ => None,
            },
            match db_config.db_type {
                AppDBType::MONGODB => Some(Box::new(
                    PositionInfoMongoRepository::new(&db_config.mongodb).await.unwrap(),
                )),
                _ => None,
            },
        );
        let app_state = SigbotState {
            // Notice: Arc object clone only increments the reference counter, and does not copy the actual data block.
            config: config.clone(),
            // The basic operators.
            string_cache: Arc::new(CacheContainer::<String>::new()),
            oidc_client: auth_clients.0,
            github_client: auth_clients.1,
            default_http_client: Arc::new(http_client),
            // The Health checker.
            sqlite_checker: SQLiteChecker::new(),
            mongo_checker: MongoChecker::new(),
            redis_cluster_checker: RedisClusterChecker::new(),
            // The System repositories.
            user_repo: Arc::new(Mutex::new(user_repo)),
            tenant_repo: Arc::new(Mutex::new(tenant_repo)),
            lock_repo: Arc::new(Mutex::new(lock_repo)),
            // The Application repositories.
            datafeed_repo: Arc::new(Mutex::new(datafeed_repo)),
            exchange_repo: Arc::new(Mutex::new(exchange_repo)),
            strategy_repo: Arc::new(Mutex::new(strategy_repo)),
            backtest_case_repo: Arc::new(Mutex::new(backtest_case_repo)),
            notification_repo: Arc::new(Mutex::new(notification_repo)),
            workflow_repo: Arc::new(Mutex::new(workflow_repo)),
            // The Wallet repositories.
            wallet_repo: Arc::new(Mutex::new(wallet_repo)),
            trade_repo: Arc::new(Mutex::new(trade_repo)),
            balance_repo: Arc::new(Mutex::new(balance_repo)),
            position_repo: Arc::new(Mutex::new(position_repo)),
        };

        // Build DI container.
        // let mut di_container = syrette::DIContainer::new();
        // di_container.bind::<dyn IUserHandler>().to::<UserHandler>()?;

        app_state
    }
}
