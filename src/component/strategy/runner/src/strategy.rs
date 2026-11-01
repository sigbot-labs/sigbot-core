// SPDX-License-Identifier: GPL-3.0-only

use crate::checkpoint::checkpoint::{CheckpointStrategyTransaction, ProducerTrace};
use crate::python::worker::runtime_executor::WorkerStrategy;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common_context::IProvider;
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sigbot_core::config::config::get_config;
use sigbot_core::config::config::PostgresStorageProperties;
use sigbot_core::mgmt::apm::logging::LogRouteGuard;
use sigbot_core::mgmt::apm::logging::LogRoutes;
use sigbot_core::modules::messager::handler::messager::MessagerHandler;
use sigbot_logservice::s3::capture::ArchiveCapture;
use sigbot_logservice::s3::capture::LogCaptureOptions;
use sigbot_logservice::s3::capture::LogStreamIdentity;
use sigbot_logservice::s3::s3::LogArchiveConfig;
use sigbot_logservice::s3::s3::S3Logservice;
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::OutboxRelay;
use sigbot_strategy_sdk::sdk::runtime::SerialStrategy;
use sigbot_strategy_sdk::sdk::runtime::StrategyIdentity;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::WorkflowEdge;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::execution::OrderStatusFact;
use sigbot_types::strategy::execution::HyperparametersUpdate;
use sigbot_types::strategy::execution::QueryOperation;
use sigbot_types::strategy::execution::StrategyEvent;
use sigbot_types::strategy::execution::StrategyEventData;
use sigbot_types::strategy::strategy::StrategyProvider;
use sigbot_types::strategy::StrategyArgument;
use sigbot_types::Uuid;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::filter::Targets;

#[async_trait]
pub trait IStrategy: IProvider {
    fn provider(&self) -> StrategyProvider;
    async fn startup(&self, messager: Arc<dyn IMessager>);
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyNodeConfig {
    pub identity: StrategyIdentity,
    pub account_id: Uuid,
    pub code: String,
    pub class_name: String,
    pub configuration: BTreeMap<String, Value>,
    pub edges: Vec<WorkflowEdge>,
    #[serde(default)]
    pub execution_node_ids: Vec<i64>,
    #[serde(default)]
    pub order_node_ids: Vec<i64>,
    #[serde(default)]
    pub wallet_node_ids: Vec<i64>,
    #[serde(default)]
    pub log_archive: Option<LogArchiveConfig>,
    #[serde(default)]
    pub virtual_clock: bool,
}

pub struct StrategyNode {
    store: Arc<CheckpointStrategyTransaction>,
    config: StrategyNodeConfig,
    runtime: SerialStrategy<WorkerStrategy>,
}

impl StrategyNode {
    pub async fn new(
        storage: &PostgresStorageProperties,
        config: StrategyNodeConfig,
        producer_trace: ProducerTrace,
    ) -> Result<Self> {
        Self::with_log_archive(storage, config, producer_trace, None).await
    }

    pub async fn with_log_archive(
        storage: &PostgresStorageProperties,
        config: StrategyNodeConfig,
        producer_trace: ProducerTrace,
        archive: Option<Arc<S3Logservice>>,
    ) -> Result<Self> {
        let virtual_clock = config.virtual_clock;
        config.identity.scope.validate()?;
        Self::create(storage, config, producer_trace, archive, virtual_clock).await
    }

    pub async fn with_replay_clock(
        storage: &PostgresStorageProperties,
        mut config: StrategyNodeConfig,
        producer_trace: ProducerTrace,
        archive: Option<Arc<S3Logservice>>,
    ) -> Result<Self> {
        config.virtual_clock = true;
        Self::with_log_archive(storage, config, producer_trace, archive).await
    }

    async fn create(
        storage: &PostgresStorageProperties,
        config: StrategyNodeConfig,
        producer_trace: ProducerTrace,
        archive: Option<Arc<S3Logservice>>,
        virtual_clock: bool,
    ) -> Result<Self> {
        ensure!(
            config.account_id.get_version_num() == 7,
            "strategy account must be UUIDv7"
        );
        ensure!(
            config
                .execution_node_ids
                .iter()
                .chain(&config.order_node_ids)
                .chain(&config.wallet_node_ids)
                .all(|node| *node > 0 && *node != config.identity.node_id),
            "invalid strategy account input owner"
        );
        let mut store =
            CheckpointStrategyTransaction::connect(storage, config.edges.clone(), producer_trace, virtual_clock)
                .await?;
        if let Some(archive) = archive {
            store = store.with_log_archive(archive);
        }
        let store = Arc::new(store);
        let mut runtime = SerialStrategy::new(
            config.identity.clone(),
            WorkerStrategy::new(config.code.clone(), config.class_name.clone())?,
            store.clone(),
            config.configuration.clone(),
        )?;
        if virtual_clock {
            runtime = runtime.with_replay_clock();
        }
        Ok(Self { store, config, runtime })
    }

    pub fn messager(&self) -> MessagerHandler {
        self.store.messager()
    }

    pub fn producer(&self) -> String {
        CheckpointStrategyTransaction::producer(&self.config.identity)
    }

    pub async fn start(self: Arc<Self>, messager: Arc<dyn IMessager>) -> Result<()> {
        let mut topics = HashSet::new();
        for edge in &self.config.edges {
            if edge.target_node_id == self.config.identity.node_id {
                topics.insert(edge.kind);
            }
        }
        topics.insert(WorkflowTopic::RuntimeEvent);
        for kind in topics {
            let topic = kind.topic(&self.config.identity.scope, self.config.identity.workflow_id)?;
            let node = self.clone();
            messager
                .subscribe(
                    &topic,
                    Arc::new(move |bytes| {
                        let node = node.clone();
                        Box::pin(async move {
                            node.consume(kind, &bytes).await?;
                            Ok(String::new())
                        })
                    }),
                )
                .await?;
        }
        for (kind, nodes) in [
            (AccountTopic::ExecutionEvent, &self.config.execution_node_ids),
            (AccountTopic::OrderEvent, &self.config.order_node_ids),
            (AccountTopic::PortfolioSnapshot, &self.config.wallet_node_ids),
        ] {
            if nodes.is_empty() {
                continue;
            }
            let topic = kind.topic(&self.config.identity.scope, self.config.account_id)?;
            let node = self.clone();
            messager
                .subscribe(
                    &topic,
                    Arc::new(move |bytes| {
                        let node = node.clone();
                        Box::pin(async move {
                            node.consume_account(kind, &bytes).await?;
                            Ok("committed".into())
                        })
                    }),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn consume_account(&self, kind: AccountTopic, bytes: &[u8]) -> Result<bool> {
        let wire: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        wire.validate()?;
        let identity = &self.config.identity;
        let topic = kind.topic(&identity.scope, self.config.account_id)?;
        wire.durable.validate_topic(&topic)?;
        ensure!(wire.durable.kind == MessageKind::Event, "account input must be a fact");
        let owners = match kind {
            AccountTopic::ExecutionEvent => &self.config.execution_node_ids,
            AccountTopic::OrderEvent => &self.config.order_node_ids,
            AccountTopic::PortfolioSnapshot => &self.config.wallet_node_ids,
            _ => anyhow::bail!("unsupported strategy account input"),
        };
        let reason = if wire.durable.target_node_id.is_some_and(|node| node != identity.node_id) {
            Some("other_target")
        } else if !owners.contains(&wire.durable.node_id) {
            Some("outside_input_edge")
        } else if wire.durable.workflow_id != Some(identity.workflow_id) {
            Some("outside_workflow")
        } else if wire.durable.run_id != identity.run_id {
            Some("inactive_run")
        } else if kind == AccountTopic::ExecutionEvent && wire.durable.payload.get("exchange_trade_id").is_none() {
            Some("other_account_fact")
        } else if kind == AccountTopic::OrderEvent && wire.durable.payload.get("receipt").is_none() {
            Some("acceptance_receipt")
        } else {
            None
        };
        if let Some(reason) = reason {
            return self.quarantine(&topic, &wire, reason).await;
        }
        let (source, symbol, data) = match kind {
            AccountTopic::ExecutionEvent => {
                let fact: ExecutionFact = serde_json::from_value(wire.durable.payload.clone())?;
                fact.validate()?;
                ensure!(
                    fact.account_id == self.config.account_id && fact.executed_at == wire.durable.event_time,
                    "execution account/time mismatch"
                );
                (
                    fact.exchange.clone(),
                    Some(fact.symbol.clone()),
                    StrategyEventData::Execution(fact),
                )
            }
            AccountTopic::OrderEvent => {
                let fact: OrderStatusFact = serde_json::from_value(wire.durable.payload.clone())?;
                fact.validate()?;
                ensure!(
                    fact.account_id == self.config.account_id && fact.occurred_at == wire.durable.event_time,
                    "order account/time mismatch"
                );
                (fact.exchange.clone(), None, StrategyEventData::OrderStatus(fact))
            }
            AccountTopic::PortfolioSnapshot => (
                "wallet".into(),
                None,
                StrategyEventData::Portfolio(wire.durable.payload.clone()),
            ),
            _ => unreachable!(),
        };
        let now = self.clock(&wire).await?;
        let event = StrategyEvent {
            source,
            symbol,
            event_time: wire.durable.event_time,
            available_time: wire.durable.available_time,
            data,
        };
        self.runtime
            .on_data(
                wire.durable.map_payload(|_| event),
                now,
                Arc::new(self.portfolio().await?),
            )
            .await
    }

    async fn clock(&self, input: &MessageEnvelope<Value>) -> Result<DateTime<Utc>> {
        if !self.config.virtual_clock {
            return Ok(Utc::now());
        }
        self.store.replay_clock(&self.config.identity, &input.durable).await
    }

    /// A Workflow topic can carry several nodes and runs. Persist a delivery
    /// outside this instance's route before ACK, without consuming the normal
    /// Inbox identity of the intended node/run.
    pub async fn consume(&self, topic: WorkflowTopic, bytes: &[u8]) -> Result<bool> {
        let wire: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        if !self.admit(topic, &wire).await? {
            return Ok(false);
        }
        let now = self.clock(&wire).await?;
        self.execute(topic, wire, now).await
    }

    /// Replay hosts supply their virtual clock and advance only after commit/settlement.
    pub async fn handle(&self, topic: WorkflowTopic, bytes: &[u8], now: DateTime<Utc>) -> Result<bool> {
        let wire: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        if !self.admit(topic, &wire).await? {
            return Ok(false);
        }
        self.execute(topic, wire, now).await
    }

    async fn admit(&self, kind: WorkflowTopic, wire: &MessageEnvelope<Value>) -> Result<bool> {
        let identity = &self.config.identity;
        wire.validate()?;
        let topic = kind.topic(&identity.scope, identity.workflow_id)?;
        wire.durable.validate_topic(&topic)?;
        let self_timer = kind == WorkflowTopic::RuntimeEvent && wire.durable.node_id == identity.node_id;
        let input_edge = self_timer
            || self
                .config
                .edges
                .iter()
                .any(|edge| edge.permits(wire.durable.node_id, identity.node_id, kind));
        let reason = if wire.durable.target_node_id.is_some_and(|node| node != identity.node_id) {
            Some("other_target")
        } else if !input_edge {
            Some("outside_input_edge")
        } else if wire.durable.run_id != identity.run_id {
            Some("inactive_run")
        } else {
            None
        };
        let Some(reason) = reason else {
            return Ok(true);
        };
        self.quarantine(&topic, wire, reason).await
    }

    async fn quarantine(&self, topic: &str, wire: &MessageEnvelope<Value>, reason: &str) -> Result<bool> {
        let identity = &self.config.identity;
        let consumer = format!(
            "strategy.routing:{}:{}:{}",
            identity.workflow_id, identity.node_id, identity.run_id
        );
        self.store.quarantine(&consumer, topic, wire, reason).await
    }

    async fn execute(&self, topic: WorkflowTopic, wire: MessageEnvelope<Value>, now: DateTime<Utc>) -> Result<bool> {
        let portfolio = Arc::new(self.portfolio().await?);
        if topic == WorkflowTopic::EvaluationSnapshot {
            let update: HyperparametersUpdate = serde_json::from_value(wire.durable.payload.clone())?;
            return self
                .runtime
                .on_hyperparameters(wire.durable.map_payload(|_| update), now, portfolio)
                .await;
        }
        let event = Self::event(topic, &wire)?;
        self.runtime
            .on_data(wire.durable.map_payload(|_| event), now, portfolio)
            .await
    }

    fn event(topic: WorkflowTopic, wire: &MessageEnvelope<Value>) -> Result<StrategyEvent> {
        let payload = &wire.durable.payload;
        if matches!(topic, WorkflowTopic::NotificationResult | WorkflowTopic::ExportResult) {
            return Ok(StrategyEvent {
                source: if topic == WorkflowTopic::NotificationResult {
                    "notifier"
                } else {
                    "export"
                }
                .into(),
                symbol: None,
                event_time: wire.durable.event_time,
                available_time: wire.durable.available_time,
                data: StrategyEventData::QueryResult {
                    operation: if topic == WorkflowTopic::NotificationResult {
                        QueryOperation::Notification
                    } else {
                        QueryOperation::Export
                    },
                    result: payload.clone(),
                },
            });
        }
        if payload.get("data").is_some() {
            return Ok(serde_json::from_value(payload.clone())?);
        }
        let source = payload
            .get("source")
            .and_then(Value::as_str)
            .context("datafeed source required")?
            .to_owned();
        let symbol = payload.get("symbol").and_then(Value::as_str).map(str::to_owned);
        let event_time = serde_json::from_value(payload.get("event_time").cloned().context("event time required")?)?;
        let available_time = serde_json::from_value(
            payload
                .get("available_time")
                .cloned()
                .context("available time required")?,
        )?;
        let data = match topic {
            WorkflowTopic::FeedNews => StrategyEventData::News {
                source_event_id: payload
                    .get("source_event_id")
                    .and_then(Value::as_str)
                    .context("source fact ID required")?
                    .to_owned(),
                text: payload
                    .get("content")
                    .and_then(Value::as_str)
                    .context("news content required")?
                    .to_owned(),
                url: payload.pointer("/raw/url").and_then(Value::as_str).map(str::to_owned),
            },
            WorkflowTopic::FeedMarket => {
                let price = payload
                    .pointer("/raw/price")
                    .or_else(|| payload.pointer("/raw/lastPrice"))
                    .context("normalized market price required")?;
                let price = Decimal::from_str(&price.as_str().map(str::to_owned).unwrap_or_else(|| price.to_string()))?;
                StrategyEventData::Market {
                    price,
                    fields: payload
                        .get("raw")
                        .and_then(Value::as_object)
                        .map(|fields| fields.iter().map(|(key, value)| (key.clone(), value.clone())).collect())
                        .unwrap_or_default(),
                }
            }
            _ => anyhow::bail!("typed strategy event required for this workflow topic"),
        };
        Ok(StrategyEvent {
            source,
            symbol,
            event_time,
            available_time,
            data,
        })
    }

    async fn portfolio(&self) -> Result<Value> {
        self.store
            .portfolio(&self.config.identity.scope, self.config.account_id)
            .await
    }
}

pub struct StrategyService {
    relay: OutboxRelay,
    archive_task: Option<tokio::task::JoinHandle<()>>,
    capture: Option<Arc<ArchiveCapture>>,
    route: Mutex<Option<LogRouteGuard>>,
    messager: Arc<dyn IMessager>,
}

impl StrategyService {
    pub async fn start(argument: &StrategyArgument, messager: Arc<dyn IMessager>) -> Result<Arc<Self>> {
        common_telemetry::readiness::RuntimeReadiness::business("strategy", false);
        let properties = argument
            .properties
            .as_ref()
            .context("strategy runtime properties required")?;
        let config: StrategyNodeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("typed strategy runtime_config required")?,
        )?;
        let archive = match &config.log_archive {
            Some(config) => {
                Some(sigbot_logservice::server::SigbotLogServer::archive(config, &get_config().storage.postgres).await?)
            }
            None => None,
        };
        let (capture, route) = if let Some(archive) = &archive {
            let (layer, capture) = ArchiveCapture::start(
                archive.clone(),
                LogStreamIdentity {
                    scope: config.identity.scope.clone(),
                    workflow_id: config.identity.workflow_id,
                    node_id: config.identity.node_id,
                    run_id: config.identity.run_id.clone(),
                },
                LogCaptureOptions::default(),
            )?;
            let targets = Targets::new()
                .with_target("sigbot_", LevelFilter::INFO)
                .with_target("common_telemetry", LevelFilter::INFO);
            (Some(capture), Some(LogRoutes::install(Box::new(layer), targets)?))
        } else {
            (None, None)
        };
        let node = Arc::new(
            StrategyNode::with_log_archive(
                &get_config().storage.postgres,
                config,
                Arc::new(|parent| Ok(MessageTrace::child("strategy.publish", parent).traceparent())),
                archive.clone(),
            )
            .await?,
        );
        let producer = node.producer();
        let message_store = node.messager();
        let observed_messager = capture
            .as_ref()
            .map(|capture| capture.wrap_messager(messager.clone()))
            .unwrap_or_else(|| messager.clone());
        node.start(observed_messager).await?;
        let relay = OutboxRelay::start(message_store, producer, messager.clone());
        let archive_task = archive.map(|archive| {
            let readiness = common_telemetry::readiness::RuntimeReadiness::task("strategy.archive");
            tokio::spawn(async move {
                readiness.set(true);
                loop {
                    if let Err(error) = archive.archive_pending(64).await {
                        common_telemetry::warn!(target:"sigbot_archive_internal", "strategy log archive retry: {}", error);
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            })
        });
        tracing::info!("Strategy runtime subscriptions and archive are ready");
        if let Some(capture) = &capture {
            capture.flush_durable().await?;
        }
        common_telemetry::readiness::RuntimeReadiness::business("strategy", true);
        Ok(Arc::new(Self {
            relay,
            archive_task,
            capture,
            route: Mutex::new(route),
            messager,
        }))
    }

    pub async fn close(&self) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("strategy", false);
        self.relay.close();
        let delivery = self.messager.close().await; // Stop callbacks before the final durable barrier.
        let result = if let Some(capture) = &self.capture {
            capture.shutdown().await
        } else {
            Ok(())
        };
        self.route.lock().unwrap_or_else(|error| error.into_inner()).take();
        if let Some(task) = &self.archive_task {
            task.abort();
        }
        delivery.and(result)
    }
}

#[async_trait::async_trait]
impl IProvider for StrategyService {
    async fn close(&self) -> anyhow::Result<()> {
        StrategyService::close(self).await
    }
}
