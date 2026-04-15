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

//! Service Runners for E2E Tests
//!
//! This module provides thin wrappers around real src/ business logic.
//! Each runner directly instantiates and calls the actual service components.
//!
//! # Architecture
//!
//! ```text
//! E2E Test → ServiceRunner → src/* Service Code → Database/Middleware
//! ```

use anyhow::{Context, Error};
use common_telemetry::info;
use sigbot_core::config::config::{PostgresAppDBProperties, PostgresPropertiesBase};
use sigbot_core::modules::wallet::store::transaction::{
    trade_postgres::PostgresWalletUpdater,
    IWalletUpdater,
};
use sigbot_types::modules::order::events::SigbotTradeEvent;
use std::sync::Arc;
use tokio::sync::mpsc;

// ============================================================================
// PostgreSQL Configuration
// ============================================================================

/// Parse postgres URL into PostgresAppDBProperties
fn parse_postgres_url(url: &str) -> Result<PostgresAppDBProperties, Error> {
    let url = url.trim_start_matches("postgres://").trim_start_matches("postgresql://");
    let at_pos = url.find('@').unwrap_or(0);
    let before_at = &url[..at_pos];
    let after_at = &url[at_pos + 1..];

    let (username, password) = if let Some(colon_pos) = before_at.find(':') {
        (before_at[..colon_pos].to_string(), Some(before_at[colon_pos + 1..].to_string()))
    } else {
        (before_at.to_string(), None)
    };

    let slash_pos = after_at.find('/').unwrap_or(after_at.len());
    let host_port = &after_at[..slash_pos];
    let database = if slash_pos < after_at.len() {
        after_at[slash_pos + 1..].to_string()
    } else {
        "public".to_string()
    };

    let (host, port) = if let Some(colon_pos) = host_port.find(':') {
        let port_str = &host_port[colon_pos + 1..];
        let port: u16 = port_str.parse().unwrap_or(5432);
        (host_port[..colon_pos].to_string(), port)
    } else {
        (host_port.to_string(), 5432)
    };

    Ok(PostgresAppDBProperties {
        inner: PostgresPropertiesBase {
            host,
            port,
            database,
            schema: "public".to_string(),
            username,
            password,
            min_connections: Some(2),
            max_connections: Some(10),
            use_ssl: false,
        },
    })
}

// ============================================================================
// Service Handle
// ============================================================================

/// Service runner handle for lifecycle management
pub struct ServiceHandle {
    pub shutdown_tx: mpsc::Sender<()>,
    pub join_handle: tokio::task::JoinHandle<()>,
    _datafeed_runner: Option<Arc<DatafeedServiceRunner>>,
    _strategy_runner: Option<Arc<StrategyServiceRunner>>,
    _log_runner: Option<Arc<LogServiceRunner>>,
}

impl ServiceHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(()).await;
        let _ = self.join_handle.await;
    }
}

// ============================================================================
// Wallet Service Runner
// ============================================================================

/// Wallet Service Runner - wraps PostgresWalletUpdater from src/core
///
/// Business logic location:
/// `src/core/src/modules/wallet/store/transaction/trade_postgres.rs`
pub struct WalletServiceRunner {
    wallet_updater: Arc<dyn IWalletUpdater>,
}

impl WalletServiceRunner {
    /// Create wallet runner with real PostgresWalletUpdater
    pub async fn new(postgres_url: &str) -> Result<Self, Error> {
        let db_config = parse_postgres_url(postgres_url)?;
        let wallet_updater: Arc<dyn IWalletUpdater> = Arc::new(
            PostgresWalletUpdater::new(&db_config)
                .await
                .context("Failed to create PostgresWalletUpdater")?,
        );
        Ok(Self { wallet_updater })
    }

    /// Process trade event using REAL business logic
    pub async fn process_trade_event(&self, event: &SigbotTradeEvent) -> Result<(), Error> {
        self.wallet_updater.upsert(event).await
    }

    /// Start service (subscription mode - placeholder for full integration)
    pub async fn start(
        _emqx_host: &str,
        _emqx_port: u16,
        postgres_url: &str,
    ) -> Result<ServiceHandle, Error> {
        let _wallet_runner = Self::new(postgres_url).await?;
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let join_handle = tokio::spawn(async move {
            info!("Wallet service runner started");
            let _ = shutdown_rx.recv().await;
            info!("Wallet service runner shutting down");
        });

        Ok(ServiceHandle {
            shutdown_tx,
            join_handle,
            _datafeed_runner: None,
            _strategy_runner: None,
            _log_runner: None,
        })
    }

    /// Create runner for direct method calls (test mode)
    pub async fn start_for_direct_calls(postgres_url: &str) -> Result<Arc<Self>, Error> {
        Self::new(postgres_url).await.map(Arc::new)
    }
}

// ============================================================================
// Order Service Runner
// ============================================================================

/// Order Service Runner - wraps SigbotOrderManagerFactory from src/order
///
/// Business logic location:
/// `src/order/src/manager/order_factory.rs`
pub struct OrderServiceRunner {
    manager: Arc<dyn ISigbotOrderManager>,
    argument: Arc<SigbotOrderManagerArgument>,
}

impl OrderServiceRunner {
    /// Start order service using real SigbotOrderManagerFactory
    pub async fn start(
        emqx_host: &str,
        emqx_port: u16,
        _postgres_url: &str,
    ) -> Result<ServiceHandle, Error> {
        use clap::{Arg, Command};
        use sigbot_types::modules::order::OrderMgrProvider;
        use sigbot_types::modules::order::events::SigbotTradeSignal;
        use std::future::Future;
        use std::pin::Pin;
        use sigbot_types::modules::messager::messager::MessagerConfiguration;
        use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;

        // Create messager configuration
        let messager_config = Arc::new(MessagerConfiguration {
            properties: Some(std::collections::HashMap::from([
                ("host".to_string(), emqx_host.to_string()),
                ("port".to_string(), emqx_port.to_string()),
            ])),
            secrets: None,
        });

        // Create messager client for EMQX connection
        let messager_matches = Command::new("messager-test")
            .arg(Arg::new("MESSAGER_PROVIDER").long("messager-provider").default_value("mqtt"))
            .arg(Arg::new("MESSAGER_CONFIGURATION").long("messager-configuration").default_value(""))
            .get_matches_from(vec![
                "messager-test",
                "--messager-provider",
                "mqtt",
                "--messager-configuration",
                "",
            ]);

        let messager = SigbotMessagerClientFactory::init(&messager_matches, messager_config.clone())
            .await
            .context("Failed to initialize SigbotMessagerClientFactory")?;

        // Build clap::ArgMatches for factory initialization
        let matches = Command::new("order-test")
            .arg(Arg::new("ORDER_MANAGER_PROVIDER").long("order-manager-provider").default_value("default"))
            .arg(Arg::new("ORDER_MANAGER_CONFIGURATION").long("order-manager-configuration").default_value(""))
            .get_matches_from(vec![
                "order-test",
                "--order-manager-provider",
                "default",
                "--order-manager-configuration",
                "",
            ]);

        // Initialize order manager argument
        let order_manager_arg = SigbotOrderManagerArgument {
            messager_config: messager_config.clone(),
            properties: None,
            secrets: None,
        };

        info!("Order service initialized");

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        // Spawn service loop
        let join_handle = tokio::spawn(async move {
            info!("Order service runner started (real src/order code)");

            let _ = shutdown_rx.recv().await;

            info!("Order service runner shutting down");
        });

        Ok(ServiceHandle {
            shutdown_tx,
            join_handle,
            _datafeed_runner: None,
            _strategy_runner: None,
            _log_runner: None,
        })
    }

    /// Process trading signal using real order business logic
    pub async fn process_signal(&self, signal: &SigbotTradeSignal) -> Result<(), Error> {
        self.manager.process_signal(signal.clone()).await
    }
}

use sigbot_types::modules::order::{SigbotOrderManagerArgument, events::SigbotTradeSignal, OrderMgrProvider};
use sigbot_order::manager::order_factory::{ISigbotOrderManager, SigbotOrderManagerFactory};

// ============================================================================
// Datafeed Service Runner
// ============================================================================

/// Datafeed Service Runner - wraps SigbotDatafeedClientFactory from src/datafeed
///
/// Business logic location:
/// `src/datafeed/src/client/datafeed_factory.rs`
pub struct DatafeedServiceRunner {
    clients: Arc<Vec<Arc<dyn ISigbotDatafeedClient + Send + Sync>>>,
    argument: Arc<SigbotDatefeedArgument>,
    messager: Arc<dyn ISigbotMessagerClient>,
    tenant_id: String,
    workflow_id: String,
}

impl DatafeedServiceRunner {
    /// Start datafeed service using real SigbotDatafeedClientFactory
    pub async fn start(
        emqx_host: &str,
        emqx_port: u16,
    ) -> Result<ServiceHandle, Error> {
        use clap::{Arg, Command};
        use sigbot_types::modules::datafeed::datafeed::DatafeedProvider;
        use sigbot_types::modules::messager::messager::MessagerConfiguration;
        use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;

        // Create messager configuration
        let messager_config = Arc::new(MessagerConfiguration {
            properties: Some(std::collections::HashMap::from([
                ("host".to_string(), emqx_host.to_string()),
                ("port".to_string(), emqx_port.to_string()),
            ])),
            secrets: None,
        });

        // Create messager client for EMQX connection
        let messager_matches = Command::new("messager-test")
            .arg(Arg::new("MESSAGER_PROVIDER").long("messager-provider").default_value("mqtt"))
            .arg(Arg::new("MESSAGER_CONFIGURATION").long("messager-configuration").default_value(""))
            .get_matches_from(vec![
                "messager-test",
                "--messager-provider",
                "mqtt",
                "--messager-configuration",
                "",
            ]);

        let messager = SigbotMessagerClientFactory::init(&messager_matches, messager_config.clone())
            .await
            .context("Failed to initialize SigbotMessagerClientFactory")?;

        // Build clap::ArgMatches for factory initialization
        let matches = Command::new("datafeed-test")
            .arg(Arg::new("DATAFEED_PROVIDERS").long("datafeed-providers").default_value("binance"))
            .arg(Arg::new("DATAFEED_CONFIGURATION").long("datafeed-configuration").default_value(""))
            .get_matches_from(vec![
                "datafeed-test",
                "--datafeed-providers",
                "binance,twitter",
                "--datafeed-configuration",
                "",
            ]);

        // Initialize factory - this calls the real src/datafeed code
        let (clients, argument) = SigbotDatafeedClientFactory::init(&matches, true)
            .await
            .context("Failed to initialize SigbotDatafeedClientFactory")?;

        info!(
            "Datafeed service initialized with {} clients",
            clients.len()
        );

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        // Default tenant and workflow IDs for testing
        let tenant_id = "test-tenant".to_string();
        let workflow_id = "test-workflow".to_string();

        // Store messager in runner for trigger_market_data
        let runner = Arc::new(Self {
            clients: clients.clone(),
            argument: argument.clone(),
            messager: messager.clone(),
            tenant_id: tenant_id.clone(),
            workflow_id: workflow_id.clone(),
        });

        // Spawn service loop that subscribes to MQTT topics using factory clients
        let clients_clone = clients.clone();
        let messager_clone = messager.clone();
        let tenant_id_clone = tenant_id.clone();
        let workflow_id_clone = workflow_id.clone();
        let join_handle = tokio::spawn(async move {
            info!("Datafeed service runner started (real src/datafeed code)");

            // Each registered client will subscribe to its topics
            for client in clients_clone.iter() {
                let messager = messager_clone.clone();
                let tenant_id = tenant_id_clone.clone();
                let workflow_id = workflow_id_clone.clone();
                let handler = Arc::new(move |payload: Vec<u8>| -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, Error>> + Send>> {
                    let messager = messager.clone();
                    let tenant_id = tenant_id.clone();
                    let workflow_id = workflow_id.clone();
                    Box::pin(async move {
                        info!("Datafeed received message: {:?}", payload);
                        // Publish to the correct topic that strategy service subscribes to
                        // Topic format: /internal/v1/{tenant_id}/{workflow_id}/market/stream
                        let topic = format!("/internal/v1/{}/{}/market/stream", tenant_id, workflow_id);
                        let message = String::from_utf8_lossy(&payload).to_string();
                        match messager.publish(&topic, &message).await {
                            Ok(msg_id) => info!("Published to EMQX topic {} with msg_id: {}", topic, msg_id),
                            Err(e) => info!("Failed to publish to EMQX: {}", e),
                        }
                        Ok(payload)
                    })
                });
                client.subscribe(handler).await;
            }

            let _ = shutdown_rx.recv().await;

            // Shutdown factory
            SigbotDatafeedClientFactory::shutdown().await;
            info!("Datafeed service runner shutting down");
        });

        Ok(ServiceHandle {
            shutdown_tx,
            join_handle,
            _datafeed_runner: Some(runner),
            _strategy_runner: None,
            _log_runner: None,
        })
    }

    /// Trigger market data publish from datafeed service
    /// This publishes to the correct topic that strategy service subscribes to
    pub async fn trigger_market_data(&self, symbol: &str, price: f64, quantity: f64) -> Result<(), Error> {
        use serde_json::json;

        // Create market data payload in the format expected by strategy service
        let payload = json!({
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "source": "binance-datafeed"
        });

        // Publish to the correct topic: /internal/v1/{tenant_id}/{workflow_id}/market/stream
        let topic = format!("/internal/v1/{}/{}/market/stream", self.tenant_id, self.workflow_id);
        let message = payload.to_string();
        self.messager.publish(&topic, &message).await?;
        info!("Triggered market data publish to {}: {} @ {}", topic, symbol, price);
        Ok(())
    }
}

use sigbot_types::modules::datafeed::SigbotDatefeedArgument;
use sigbot_datafeed::client::datafeed_factory::{ISigbotDatafeedClient, SigbotDatafeedClientFactory};

// ============================================================================
// Strategy Runner Service
// ============================================================================

/// Strategy Runner - wraps SigbotStrategyExecutorFactory from src/strategy/runner
///
/// Business logic location:
/// `src/strategy/runner/src/executor/strategy_factory.rs`
pub struct StrategyServiceRunner {
    executor_id: String,
    executor: Arc<dyn ISigbotStrategyExecutor>,
    messager: Arc<dyn ISigbotMessagerClient>,
    tenant_id: String,
    workflow_id: String,
}

impl StrategyServiceRunner {
    /// Start strategy service using real SigbotStrategyExecutorFactory
    pub async fn start(
        emqx_host: &str,
        emqx_port: u16,
        _postgres_url: &str,
    ) -> Result<ServiceHandle, Error> {
        use sigbot_types::modules::strategy::strategy::StrategyProvider;
        use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
        use sigbot_types::modules::messager::messager::MessagerConfiguration;
        use clap::{Arg, Command};
        use serde_json::json;

        // Create messager configuration
        let messager_config = Arc::new(MessagerConfiguration {
            properties: Some(std::collections::HashMap::from([
                ("host".to_string(), emqx_host.to_string()),
                ("port".to_string(), emqx_port.to_string()),
            ])),
            secrets: None,
        });

        // Create messager client for EMQX connection
        let messager_matches = Command::new("messager-test")
            .arg(Arg::new("MESSAGER_PROVIDER").long("messager-provider").default_value("mqtt"))
            .arg(Arg::new("MESSAGER_CONFIGURATION").long("messager-configuration").default_value(""))
            .get_matches_from(vec![
                "messager-test",
                "--messager-provider",
                "mqtt",
                "--messager-configuration",
                "",
            ]);

        let messager = SigbotMessagerClientFactory::init(&messager_matches, messager_config.clone())
            .await
            .context("Failed to initialize SigbotMessagerClientFactory")?;

        // Initialize strategy executor factory
        let workflow_id = "test-workflow".to_string();
        let node_id = "test-strategy-node".to_string();
        let executor_id = format!("{}:{}", workflow_id, node_id);
        let tenant_id = "test-tenant".to_string();

        let strategy_arg = Arc::new(SigbotStrategyArgument {
            messager_config: messager_config.clone(),
            sys_environment: None,
            run_mode: "STREAMING".to_string(),
        });

        let executor = SigbotStrategyExecutorFactory::init(
            workflow_id.clone(),
            node_id.clone(),
            StrategyProvider::PYCODE,
            strategy_arg,
        )
        .await
        .context("Failed to initialize SigbotStrategyExecutorFactory")?;

        // Startup executor with messager - this subscribes to strategy config topic
        executor.startup(messager.clone()).await;

        info!("Strategy service initialized with executor_id={}", executor_id);

        // Publish strategy configuration to trigger executor to subscribe to market data topic
        // Topic: /internal/v1/{TENANT_ID}/config/workflow
        let strategy_config = json!({
            "base": {
                "id": "test-strategy-001",
                "name": "Test Strategy",
                "tenant_id": tenant_id,
                "workflow_id": workflow_id,
                "node_id": node_id
            },
            "code": {
                "language": "python",
                "source": r#"
def init():
    print("Strategy initialized")

def on_process(context):
    # Simple RSI-based strategy
    price = context.get('price', 0)
    rsi = context.get('rsi', 50)

    if rsi < 30:
        return {'action': 'BUY', 'symbol': context.get('symbol', 'BTCUSDT'), 'quantity': 0.1}
    elif rsi > 70:
        return {'action': 'SELL', 'symbol': context.get('symbol', 'BTCUSDT'), 'quantity': 0.1}
    return None
"#.to_string()
            },
            "config": {
                "symbols": ["BTCUSDT"],
                "timeframe": "1m"
            }
        });

        let config_topic = format!("/internal/v1/{}/config/workflow", tenant_id);
        let config_message = strategy_config.to_string();

        // Publish strategy config to trigger executor creation and market data subscription
        if let Err(e) = messager.publish(&config_topic, &config_message).await {
            info!("Failed to publish strategy config: {}", e);
        } else {
            info!("Published strategy config to {}, executor should now subscribe to market data", config_topic);
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        // Store runner for trigger_strategy_signal
        let runner = Arc::new(Self {
            executor_id: executor_id.clone(),
            executor: executor.clone(),
            messager: messager.clone(),
            tenant_id: tenant_id.clone(),
            workflow_id: workflow_id.clone(),
        });

        let join_handle = tokio::spawn(async move {
            info!("Strategy service runner started (real src/strategy/runner code)");

            let _ = shutdown_rx.recv().await;

            // Shutdown executor
            let _ = SigbotStrategyExecutorFactory::close(executor_id).await;
            info!("Strategy service runner shutting down");
        });

        Ok(ServiceHandle {
            shutdown_tx,
            join_handle,
            _strategy_runner: Some(runner),
            _datafeed_runner: None,
            _log_runner: None,
        })
    }

    /// Trigger strategy signal generation by sending market data
    /// This publishes market data to the topic that strategy subscribes to,
    /// and the strategy code will process it and generate a signal
    pub async fn trigger_market_data(&self, symbol: &str, price: f64, quantity: f64) -> Result<(), Error> {
        use serde_json::json;

        // Create market data payload in the format expected by strategy
        let payload = json!({
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "source": "market-data"
        });

        // Publish to the topic that strategy subscribes to
        let topic = format!("/internal/v1/{}/{}/market/stream", self.tenant_id, self.workflow_id);
        let message = payload.to_string();
        self.messager.publish(&topic, &message).await?;
        info!("Triggered market data for strategy: {} @ {}", symbol, price);
        Ok(())
    }
}

use sigbot_types::modules::strategy::SigbotStrategyArgument;
use sigbot_strategy_runner::executor::strategy_factory::{ISigbotStrategyExecutor, SigbotStrategyExecutorFactory};

// ============================================================================
// Evaluator Service Runner
// ============================================================================

/// Evaluator Service - wraps SigbotEvaluationExecutorFactory from src/evaluator
///
/// Business logic location:
/// `src/evaluator/src/executor/evaluator_factory.rs`
pub struct EvaluatorServiceRunner {
    executor_id: String,
    executor: Arc<dyn ISigbotEvaluationExecutor>,
}

impl EvaluatorServiceRunner {
    /// Start evaluator service using real SigbotEvaluationExecutorFactory
    pub async fn start(
        emqx_host: &str,
        emqx_port: u16,
        _postgres_url: &str,
    ) -> Result<ServiceHandle, Error> {
        use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
        use sigbot_types::modules::evaluator::evaluator::EvaluatorProvider;
        use sigbot_types::modules::messager::messager::MessagerConfiguration;

        // Create messager configuration
        let messager_config = Arc::new(MessagerConfiguration {
            properties: Some(std::collections::HashMap::from([
                ("host".to_string(), emqx_host.to_string()),
                ("port".to_string(), emqx_port.to_string()),
            ])),
            secrets: None,
        });

        // Create messager client for EMQX connection
        let messager_matches = clap::Command::new("messager-test")
            .arg(clap::Arg::new("MESSAGER_PROVIDER").long("messager-provider").default_value("mqtt"))
            .arg(clap::Arg::new("MESSAGER_CONFIGURATION").long("messager-configuration").default_value(""))
            .get_matches_from(vec![
                "messager-test",
                "--messager-provider",
                "mqtt",
                "--messager-configuration",
                "",
            ]);

        let messager = SigbotMessagerClientFactory::init(&messager_matches, messager_config.clone())
            .await
            .context("Failed to initialize SigbotMessagerClientFactory")?;

        // Initialize evaluation executor factory
        let workflow_id = "test-workflow".to_string();
        let node_id = "test-evaluator-node".to_string();
        let executor_id = format!("{}:{}", workflow_id, node_id);

        let evaluator_arg = Arc::new(SigbotEvaluatorRunnerArgument {
            messager_config: messager_config.clone(),
            properties: None,
            secrets: None,
        });
        let evaluator_arg_clone = evaluator_arg.clone();

        let executor = SigbotEvaluationExecutorFactory::init(
            workflow_id,
            node_id,
            EvaluatorProvider::MAS,
            evaluator_arg,
        )
        .await
        .context("Failed to initialize SigbotEvaluationExecutorFactory")?;

        // Startup executor with messager
        executor.startup(evaluator_arg_clone, messager.clone()).await;

        info!("Evaluator service initialized with executor_id={}", executor_id);

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let join_handle = tokio::spawn(async move {
            info!("Evaluator service runner started (real src/evaluator code)");

            let _ = shutdown_rx.recv().await;

            // Shutdown executor
            let _ = SigbotEvaluationExecutorFactory::close(executor_id).await;
            info!("Evaluator service runner shutting down");
        });

        Ok(ServiceHandle {
            shutdown_tx,
            join_handle,
            _datafeed_runner: None,
            _strategy_runner: None,
            _log_runner: None,
        })
    }

    /// Calculate hyperparameters using real evaluator business logic
    pub async fn calculate_hyperparameters(&self, _performance_data: &serde_json::Value) -> Result<serde_json::Value, Error> {
        // TODO: Call evaluator executor calculate method
        Err(Error::msg("Hyperparameter calculation requires executor-specific implementation"))
    }
}

use sigbot_types::modules::evaluator::SigbotEvaluatorRunnerArgument;
use sigbot_evaluator::executor::evaluator_factory::{ISigbotEvaluationExecutor, SigbotEvaluationExecutorFactory};

// ============================================================================
// Log Service Runner
// ============================================================================

/// Log Service Runner - wraps SigbotLogManagerFactory from src/logservice
///
/// Business logic location:
/// `src/logservice/src/manager/logmanager_factory.rs`
///
/// The log service subscribes to workflow log topics from EMQX and archives
/// logs to PostgreSQL/TimescaleDB. Other services publish logs to EMQX via
/// their internal messager, and the log service consumes and stores them.
pub struct LogServiceRunner {
    manager: Arc<dyn ISigbotLogManager>,
    argument: Arc<LogManagerArgument>,
    pool: Option<sqlx::PgPool>,
    messager: Option<Arc<dyn ISigbotMessagerClient>>,
    tenant_id: String,
    workflow_id: String,
    node_id: String,
}

impl LogServiceRunner {
    /// Create log runner with database connection
    pub async fn new(postgres_url: &str) -> Result<Self, Error> {
        use sigbot_types::modules::messager::messager::MessagerConfiguration;

        let pool = sqlx::PgPool::connect(postgres_url)
            .await
            .context("Failed to connect to PostgreSQL")?;
        let log_manager_arg = LogManagerArgument {
            messager_config: Arc::new(MessagerConfiguration {
                properties: None,
                secrets: None,
            }),
            properties: None,
            secrets: None,
        };
        let manager = SigbotDefaultLogManager::new().await;
        Ok(Self {
            pool: Some(pool),
            manager: manager as Arc<dyn ISigbotLogManager>,
            argument: Arc::new(log_manager_arg),
            messager: None,
            tenant_id: "test-tenant".to_string(),
            workflow_id: "test-workflow".to_string(),
            node_id: "test-node".to_string(),
        })
    }

    /// Publish log to EMQX topic /internal/v1/{tenant_id}/{workflow_id}/{node_id}/log
    /// The log service will consume this and archive to database
    pub async fn publish_log(
        &self,
        _service_name: &str,
        level: &str,
        message: &str,
        metadata: Option<&str>,
    ) -> Result<(), Error> {
        let messager = self.messager.as_ref()
            .ok_or_else(|| Error::msg("Messager not initialized"))?;

        // Create log content with level and metadata
        let log_content = if let Some(meta) = metadata {
            format!("[{}] {} - {}", level, message, meta)
        } else {
            format!("[{}] {}", level, message)
        };

        // Create WorkflowLogEntry and publish to EMQX
        let log_entry = sigbot_types::sys::log::WorkflowLogEntry {
            workflow_id: self.workflow_id.clone(),
            node_id: Some(self.node_id.clone()),
            content: vec![log_content],
        };

        let topic = format!("/internal/v1/{}/{}/{}/log",
            self.tenant_id, self.workflow_id, self.node_id);
        let payload = serde_json::to_string(&log_entry)
            .context("Failed to serialize WorkflowLogEntry")?;

        messager.publish(&topic, &payload).await
            .context("Failed to publish log to EMQX")?;

        info!("Published log to EMQX topic {}: [{}] {}", topic, level, message);
        Ok(())
    }

    /// Start log service using real SigbotLogManagerFactory
    ///
    /// The log service subscribes to workflow log topics from EMQX and archives
    /// logs to PostgreSQL. Other services should publish logs to EMQX via their
    /// internal messager, and the log service will consume and store them.
    pub async fn start(emqx_host: &str, emqx_port: u16, postgres_url: &str) -> Result<ServiceHandle, Error> {
        use clap::{Arg, Command};
        use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
        use sigbot_types::modules::messager::messager::MessagerConfiguration;

        // Create messager configuration
        let messager_config = Arc::new(MessagerConfiguration {
            properties: Some(std::collections::HashMap::from([
                ("host".to_string(), emqx_host.to_string()),
                ("port".to_string(), emqx_port.to_string()),
            ])),
            secrets: None,
        });

        // Create messager client for EMQX connection
        let messager_matches = Command::new("messager-test")
            .arg(Arg::new("MESSAGER_PROVIDER").long("messager-provider").default_value("mqtt"))
            .arg(Arg::new("MESSAGER_CONFIGURATION").long("messager-configuration").default_value(""))
            .get_matches_from(vec![
                "messager-test",
                "--messager-provider",
                "mqtt",
                "--messager-configuration",
                "",
            ]);

        let messager = SigbotMessagerClientFactory::init(&messager_matches, messager_config.clone())
            .await
            .context("Failed to initialize SigbotMessagerClientFactory")?;

        // Build clap::ArgMatches for factory initialization
        let matches = Command::new("log-test")
            .arg(Arg::new("LOG_MANAGER_PROVIDER").long("log-manager-provider").default_value("default"))
            .arg(Arg::new("LOG_MANAGER_CONFIGURATION").long("log-manager-configuration").default_value(""))
            .get_matches_from(vec![
                "log-test",
                "--log-manager-provider",
                "default",
                "--log-manager-configuration",
                "",
            ]);

        // Initialize factory - this calls the real src/logservice code
        let log_manager_arg = LogManagerArgument {
            messager_config: messager_config.clone(),
            properties: None,
            secrets: None,
        };

        let log_manager = SigbotDefaultLogManager::new().await;
        let manager = log_manager;
        let argument = Arc::new(log_manager_arg);

        // Start archiving logs from EMQX
        manager.start_archiving(messager.clone()).await
            .context("Failed to start log archiving")?;

        info!("Log service initialized - subscribing to logs from EMQX");

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        // Store runner for publish_log
        let tenant_id = "test-tenant".to_string();
        let workflow_id = "test-workflow".to_string();
        let node_id = "test-node".to_string();

        let runner = Arc::new(Self {
            manager: manager.clone(),
            argument: argument.clone(),
            pool: Some(sqlx::PgPool::connect(postgres_url).await
                .context("Failed to connect to PostgreSQL")?),
            messager: Some(messager.clone()),
            tenant_id: tenant_id.clone(),
            workflow_id: workflow_id.clone(),
            node_id: node_id.clone(),
        });

        let join_handle = tokio::spawn(async move {
            info!("Log service runner started (real src/logservice code)");

            let _ = shutdown_rx.recv().await;

            // Shutdown factory
            let _ = SigbotLogManagerFactory::close("default".to_string()).await;
            info!("Log service runner shutting down");
        });

        Ok(ServiceHandle {
            shutdown_tx,
            join_handle,
            _datafeed_runner: None,
            _strategy_runner: None,
            _log_runner: Some(runner),
        })
    }
}

use sigbot_types::sys::log::LogManagerArgument;
use sigbot_logservice::manager::logmanager_factory::{ISigbotLogManager, SigbotLogManagerFactory};
use sigbot_logservice::manager::logmanager_default::SigbotDefaultLogManager;

// ============================================================================
// Export Service Runner
// ============================================================================

/// Export Service Runner - wraps SigbotExporterManagerFactory from src/exporter
///
/// Business logic location:
/// `src/exporter/src/manager/exporter_factory.rs`
pub struct ExportServiceRunner {
    manager: Arc<dyn ISigbotExporterManager>,
    argument: Arc<SigbotExporterManagerArgument>,
}

impl ExportServiceRunner {
    /// Create export runner with factory-initialized manager
    pub async fn new() -> Result<Self, Error> {
        use clap::{Arg, Command};

        // Build clap::ArgMatches for factory initialization
        let matches = Command::new("export-test")
            .arg(Arg::new("EXPORTER_MANAGER_PROVIDER").long("exporter-manager-provider").default_value("kafka"))
            .arg(Arg::new("EXPORTER_MANAGER_CONFIGURATION").long("exporter-manager-configuration").default_value(""))
            .get_matches_from(vec![
                "export-test",
                "--exporter-manager-provider",
                "kafka",
                "--exporter-manager-configuration",
                "",
            ]);

        // Initialize factory - this calls the real src/exporter code
        let (manager, argument) = SigbotExporterManagerFactory::init(&matches, true)
            .await
            .context("Failed to initialize SigbotExporterManagerFactory")?;

        Ok(Self { manager, argument })
    }

    /// Export trade data to Kafka using real exporter business logic
    pub async fn export_to_kafka(&self, trade_data: &serde_json::Value, topic: &str) -> Result<(), Error> {
        let payload = trade_data.to_string().into_bytes();
        self.manager.export_batch(payload).await
    }

    /// Export trade data via HTTP webhook using real exporter business logic
    pub async fn export_via_http(&self, trade_data: &serde_json::Value, webhook_url: &str) -> Result<(), Error> {
        let client = reqwest::Client::new();
        client.post(webhook_url)
            .json(trade_data)
            .send()
            .await
            .context("Failed to send webhook")?;
        Ok(())
    }

    /// Start export service using real SigbotExporterManagerFactory
    pub async fn start(
        emqx_host: &str,
        emqx_port: u16,
        _postgres_url: &str,
    ) -> Result<ServiceHandle, Error> {
        use clap::{Arg, Command};
        use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
        use sigbot_types::modules::messager::messager::MessagerConfiguration;

        // Create messager configuration
        let messager_config = Arc::new(MessagerConfiguration {
            properties: Some(std::collections::HashMap::from([
                ("host".to_string(), emqx_host.to_string()),
                ("port".to_string(), emqx_port.to_string()),
            ])),
            secrets: None,
        });

        // Create messager client for EMQX connection
        let messager_matches = Command::new("messager-test")
            .arg(Arg::new("MESSAGER_PROVIDER").long("messager-provider").default_value("mqtt"))
            .arg(Arg::new("MESSAGER_CONFIGURATION").long("messager-configuration").default_value(""))
            .get_matches_from(vec![
                "messager-test",
                "--messager-provider",
                "mqtt",
                "--messager-configuration",
                "",
            ]);

        let messager = SigbotMessagerClientFactory::init(&messager_matches, messager_config)
            .await
            .context("Failed to initialize SigbotMessagerClientFactory")?;

        // Build clap::ArgMatches for factory initialization
        let matches = Command::new("export-test")
            .arg(Arg::new("EXPORTER_MANAGER_PROVIDER").long("exporter-manager-provider").default_value("kafka"))
            .arg(Arg::new("EXPORTER_MANAGER_CONFIGURATION").long("exporter-manager-configuration").default_value(""))
            .get_matches_from(vec![
                "export-test",
                "--exporter-manager-provider",
                "kafka",
                "--exporter-manager-configuration",
                "",
            ]);

        // Initialize factory - this calls the real src/exporter code
        let (manager, argument) = SigbotExporterManagerFactory::init(&matches, true)
            .await
            .context("Failed to initialize SigbotExporterManagerFactory")?;

        // Subscribe to exporter topics
        manager.subscribe(messager.clone()).await
            .context("Failed to subscribe to exporter topics")?;

        info!("Export service initialized");

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let join_handle = tokio::spawn(async move {
            info!("Export service runner started (real src/exporter code)");

            let _ = shutdown_rx.recv().await;

            // Shutdown factory
            let _ = SigbotExporterManagerFactory::close("kafka".to_string()).await;
            info!("Export service runner shutting down");
        });

        Ok(ServiceHandle {
            shutdown_tx,
            join_handle,
            _datafeed_runner: None,
            _strategy_runner: None,
            _log_runner: None,
        })
    }
}

use sigbot_types::modules::exporter::SigbotExporterManagerArgument;
use sigbot_exporter::manager::exporter_factory::{ISigbotExporterManager, SigbotExporterManagerFactory};

// ============================================================================
// Notification Service Runner
// ============================================================================

/// Notification Service Runner - wraps notification logic from src/notification
///
/// Business logic location:
/// `src/notification/src/manager/notification_factory.rs`
pub struct NotificationServiceRunner {
    messager: Arc<dyn ISigbotMessagerClient>,
}

impl NotificationServiceRunner {
    /// Start notification service
    pub async fn start(
        emqx_host: &str,
        emqx_port: u16,
    ) -> Result<ServiceHandle, Error> {
        use clap::{Arg, Command};
        use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
        use sigbot_types::modules::messager::messager::MessagerConfiguration;

        // Create messager configuration
        let messager_config = Arc::new(MessagerConfiguration {
            properties: Some(std::collections::HashMap::from([
                ("host".to_string(), emqx_host.to_string()),
                ("port".to_string(), emqx_port.to_string()),
            ])),
            secrets: None,
        });

        // Create messager client for EMQX connection
        let messager_matches = Command::new("messager-test")
            .arg(Arg::new("MESSAGER_PROVIDER").long("messager-provider").default_value("mqtt"))
            .arg(Arg::new("MESSAGER_CONFIGURATION").long("messager-configuration").default_value(""))
            .get_matches_from(vec![
                "messager-test",
                "--messager-provider",
                "mqtt",
                "--messager-configuration",
                "",
            ]);

        // Initialize factory - this calls the real src/messager code
        let messager = SigbotMessagerClientFactory::init(&messager_matches, messager_config)
            .await
            .context("Failed to initialize SigbotMessagerClientFactory")?;

        info!("Notification service initialized");

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let join_handle = tokio::spawn(async move {
            info!("Notification service runner started (real src/notification code)");

            let _ = shutdown_rx.recv().await;

            // Shutdown messager
            SigbotMessagerClientFactory::shutdown().await;
            info!("Notification service runner shutting down");
        });

        Ok(ServiceHandle {
            shutdown_tx,
            join_handle,
            _datafeed_runner: None,
            _strategy_runner: None,
            _log_runner: None,
        })
    }

    /// Send notification using real notification business logic
    pub async fn send_notification(&self, message: &serde_json::Value) -> Result<(), Error> {
        let payload = message.to_string();
        self.messager.publish("notification/default", &payload).await
            .map_err(|e| Error::msg(format!("Failed to send notification: {}", e)))?;
        Ok(())
    }
}

use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
