// SPDX-License-Identifier: GPL-3.0-only
//! v2 service host: subscriptions route into the SDK; the SDK owns decisions.

use crate::executor::{
    pyexec::runtime_executor::PythonStrategy,
    runtime_postgres::{PostgresStrategyTransaction, ProducerTrace},
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sigbot_logservice::manager::archive::{LogArchive, LogArchiveConfig};
use sigbot_messager::{client::messager_factory::ISigbotMessagerClient, storage::postgres::PostgresMessageStore};
use sigbot_strategy_sdk::sdk::runtime::{SerialStrategy, StrategyIdentity};
use sigbot_types::modules::{
    messager::{
        envelope::{MessageEnvelope, MessageKind},
        topic::{AccountTopic, WorkflowEdge, WorkflowTopic},
    },
    order::execution::{ExecutionFact, OrderStatusFact},
    strategy::runtime::{HyperparametersUpdate, QueryOperation, StrategyEvent, StrategyEventData},
};
use sqlx::{PgPool, Row};
use std::{
    collections::{BTreeMap, HashSet},
    str::FromStr,
    sync::Arc,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyNodeConfig {
    pub identity: StrategyIdentity,
    pub account_id: i64,
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
    pool: PgPool,
    config: StrategyNodeConfig,
    runtime: SerialStrategy<PythonStrategy>,
}

impl StrategyNode {
    pub fn new(pool: PgPool, config: StrategyNodeConfig, producer_trace: ProducerTrace) -> Result<Self> {
        Self::with_log_archive(pool, config, producer_trace, None)
    }

    pub fn with_log_archive(
        pool: PgPool,
        config: StrategyNodeConfig,
        producer_trace: ProducerTrace,
        archive: Option<Arc<LogArchive>>,
    ) -> Result<Self> {
        let virtual_clock = config.virtual_clock;
        ensure!(
            !virtual_clock
                || matches!(config.identity.scope.env.as_str(), "replay" | "backtest")
                || config.identity.scope.env.starts_with("replay-"),
            "virtual clock requires an isolated replay environment"
        );
        Self::create(pool, config, producer_trace, archive, virtual_clock)
    }

    pub fn with_replay_clock(
        pool: PgPool,
        mut config: StrategyNodeConfig,
        producer_trace: ProducerTrace,
        archive: Option<Arc<LogArchive>>,
    ) -> Result<Self> {
        config.virtual_clock = true;
        Self::with_log_archive(pool, config, producer_trace, archive)
    }

    fn create(
        pool: PgPool,
        config: StrategyNodeConfig,
        producer_trace: ProducerTrace,
        archive: Option<Arc<LogArchive>>,
        virtual_clock: bool,
    ) -> Result<Self> {
        ensure!(config.account_id > 0, "strategy account is required");
        ensure!(
            config
                .execution_node_ids
                .iter()
                .chain(&config.order_node_ids)
                .chain(&config.wallet_node_ids)
                .all(|node| *node > 0 && *node != config.identity.node_id),
            "invalid strategy account input owner"
        );
        let mut store = PostgresStrategyTransaction::new(pool.clone(), config.edges.clone(), producer_trace);
        if virtual_clock {
            store = store.with_replay_clock();
        }
        if let Some(archive) = archive {
            store = store.with_log_archive(archive);
        }
        let store = Arc::new(store);
        let mut runtime = SerialStrategy::new(
            config.identity.clone(),
            PythonStrategy::new(config.code.clone(), config.class_name.clone())?,
            store,
            config.configuration.clone(),
        )?;
        if virtual_clock {
            runtime = runtime.with_replay_clock();
        }
        Ok(Self { pool, config, runtime })
    }

    pub fn producer(&self) -> String {
        PostgresStrategyTransaction::producer(&self.config.identity)
    }

    pub async fn start(self: Arc<Self>, messager: Arc<dyn ISigbotMessagerClient>) -> Result<()> {
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
        let identity = &self.config.identity;
        let checkpoint=sqlx::query("SELECT virtual_time,state FROM backtest_checkpoint WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4")
            .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&identity.run_id).fetch_one(&self.pool).await?;
        if checkpoint.get::<String, _>("state") != "running" {
            let processed: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM messager_inbox WHERE env=$1 AND tenant_id=$2 AND space_id=$3
                AND consumer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7 AND state='done')",
            )
            .bind(&identity.scope.env)
            .bind(identity.scope.tenant_id)
            .bind(identity.scope.space_id)
            .bind(format!("strategy.{}", identity.node_id))
            .bind(input.durable.node_id)
            .bind(&identity.run_id)
            .bind(input.durable.seq)
            .fetch_one(&self.pool)
            .await?;
            ensure!(processed, "completed replay cannot admit another decision");
        }
        Ok(checkpoint.get("virtual_time"))
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
        let mut tx = self.pool.begin().await?;
        if PostgresMessageStore::accept(&mut tx, &consumer, &topic, wire).await? {
            sqlx::query("UPDATE messager_inbox SET state='quarantined',completed_at=NOW(),quarantine_reason=$8
                WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND consumer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7 AND state='pending'")
                .bind(&identity.scope.env).bind(identity.scope.tenant_id).bind(identity.scope.space_id).bind(&consumer)
                .bind(wire.durable.node_id).bind(&wire.durable.run_id).bind(wire.durable.seq).bind(reason).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(false)
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
                    "notification"
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
        let scope = &self.config.identity.scope;
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let wallet = sqlx::query("SELECT id,portfolio_version FROM s_wallets WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND account_id=$4")
            .bind(&scope.env).bind(scope.tenant_id.to_string()).bind(scope.space_id).bind(self.config.account_id)
            .fetch_one(&mut *tx).await.context("strategy account wallet missing")?;
        let wallet_id: i64 = wallet.get("id");
        let balances = sqlx::query("SELECT asset,available,locked FROM s_balances WHERE wallet_id=$1 ORDER BY asset")
            .bind(wallet_id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(|row| {
                json!({
                "asset":row.get::<String,_>("asset"),"available":row.get::<Decimal,_>("available"),
                "locked":row.get::<Decimal,_>("locked") })
            })
            .collect::<Vec<_>>();
        let positions = sqlx::query(
            "SELECT symbol,side,size,entry_price,realized_pnl FROM s_positions WHERE wallet_id=$1 ORDER BY symbol,side",
        )
        .bind(wallet_id)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| {
            json!({
                "symbol":row.get::<String,_>("symbol"),"side":row.get::<String,_>("side"),
                "size":row.get::<Decimal,_>("size"),"entry_price":row.get::<Decimal,_>("entry_price"),
                "realized_pnl":row.get::<Decimal,_>("realized_pnl") })
        })
        .collect::<Vec<_>>();
        tx.commit().await?;
        Ok(
            json!({"account_id":self.config.account_id,"wallet_id":wallet_id,"version":wallet.get::<i64,_>("portfolio_version"),
            "balances":balances,"positions":positions}),
        )
    }
}
