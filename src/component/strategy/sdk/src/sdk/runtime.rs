// SPDX-License-Identifier: GPL-3.0-only
//! Single-owner decisions: state is a transaction draft, and side effects are data.

use anyhow::ensure;

use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::strategy::execution::ComputePlan;
use sigbot_types::strategy::execution::Effects;
use sigbot_types::strategy::execution::HyperparametersUpdate;
use sigbot_types::strategy::execution::MarketRegime;
use sigbot_types::strategy::execution::SnapshotMode;
use sigbot_types::strategy::execution::SnapshotQuery;
use sigbot_types::strategy::execution::StrategyEvent;
use sigbot_types::strategy::execution::StrategyRequest;
use sigbot_types::Uuid;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyIdentity {
    pub scope: MessageScope,
    pub workflow_id: Uuid,
    pub node_id: i64,
    pub run_id: Uuid,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StrategyState {
    pub regime: Option<MarketRegime>,
    pub features: BTreeMap<String, Value>,
    pub position_controls: BTreeMap<String, Value>,
    pub requests: BTreeMap<String, Value>,
    pub user: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StrategyCheckpoint {
    pub version: i64,
    pub state: StrategyState,
    pub hyperparameters: Option<HyperparametersUpdate>,
}

pub struct StrategyContext {
    identity: StrategyIdentity,
    clock: DateTime<Utc>,
    virtual_clock: bool,
    configuration: Arc<BTreeMap<String, Value>>,
    hyperparameters: Option<HyperparametersUpdate>,
    portfolio: Arc<Value>,
    pending: RefCell<Vec<StrategyRequest>>,
    logs: Vec<StrategyLog>,
    pub state: StrategyState,
}

impl StrategyContext {
    pub fn record_log(&mut self, level: &str, source: &str, content: String) -> Result<()> {
        ensure!(
            self.logs.len() < 8 && content.len() <= 131_072,
            "strategy log batch exceeds bounds"
        );
        if !content.is_empty() {
            self.logs.push(StrategyLog {
                level: level.into(),
                source: source.into(),
                content,
            });
        }
        Ok(())
    }
    pub fn identity(&self) -> &StrategyIdentity {
        &self.identity
    }
    pub fn now(&self) -> DateTime<Utc> {
        self.clock
    }
    pub fn configuration(&self) -> &BTreeMap<String, Value> {
        &self.configuration
    }
    pub fn hyperparameters(&self) -> Option<&HyperparametersUpdate> {
        self.hyperparameters.as_ref()
    }
    pub fn portfolio(&self) -> &Value {
        &self.portfolio
    }
    pub fn market(&self) -> MarketClient<'_> {
        MarketClient { context: self }
    }
    pub fn compute(&self) -> ComputeClient<'_> {
        ComputeClient { context: self }
    }

    fn finish(&mut self, mut effects: Effects) -> Result<Effects> {
        effects.requests.extend(self.pending.take());
        if self.virtual_clock {
            for request in &mut effects.requests {
                if let StrategyRequest::Snapshot(query) = request {
                    if matches!(query.mode, SnapshotMode::Latest) {
                        query.mode = SnapshotMode::AsOf(self.clock);
                    }
                }
            }
        }
        effects.validate(self.clock)?;
        ensure!(
            serde_json::to_vec(&self.state)?.len() <= 1_048_576,
            "strategy state exceeds 1 MiB checkpoint bound"
        );
        Ok(effects)
    }
}

pub struct MarketClient<'a> {
    context: &'a StrategyContext,
}
impl MarketClient<'_> {
    /// Queues a request. Its asynchronous result is delivered through on_data.
    pub fn snapshot(&self, query: SnapshotQuery) -> Result<()> {
        query.validate(self.context.clock)?;
        ensure!(
            self.context.pending.borrow().len() < 64,
            "strategy request limit reached"
        );
        self.context.pending.borrow_mut().push(StrategyRequest::Snapshot(query));
        Ok(())
    }
}

pub struct ComputeClient<'a> {
    context: &'a StrategyContext,
}
impl ComputeClient<'_> {
    pub fn submit(&self, plan: ComputePlan) -> Result<()> {
        plan.validate(self.context.clock)?;
        ensure!(
            self.context.pending.borrow().len() < 64,
            "strategy request limit reached"
        );
        self.context.pending.borrow_mut().push(StrategyRequest::Compute(plan));
        Ok(())
    }
}

pub trait Strategy: Send {
    fn on_hyperparameters(&mut self, context: &mut StrategyContext, update: &HyperparametersUpdate) -> Result<Effects>;
    fn on_data(&mut self, context: &mut StrategyContext, event: &StrategyEvent) -> Result<Effects>;
}

pub struct StrategyLog {
    pub level: String,
    pub source: String,
    pub content: String,
}

#[async_trait]
pub trait StrategyTransaction: Send + Sync {
    async fn load(&self, identity: &StrategyIdentity) -> Result<StrategyCheckpoint>;
    async fn processed(&self, identity: &StrategyIdentity, input: &DurableMessage<Value>) -> Result<bool>;
    async fn record_logs(
        &self,
        _identity: &StrategyIdentity,
        _input: &DurableMessage<Value>,
        logs: Vec<StrategyLog>,
    ) -> Result<()> {
        ensure!(logs.is_empty(), "host must configure durable strategy output capture");
        Ok(())
    }
    /// Must atomically commit inbox, state/checkpoint and every effect's outbox row.
    async fn commit(
        &self,
        identity: &StrategyIdentity,
        input: &DurableMessage<Value>,
        expected_version: i64,
        checkpoint: StrategyCheckpoint,
        effects: Effects,
    ) -> Result<bool>;
}

pub struct SerialStrategy<S: Strategy> {
    identity: StrategyIdentity,
    strategy: Mutex<S>,
    store: Arc<dyn StrategyTransaction>,
    configuration: Arc<BTreeMap<String, Value>>,
    virtual_clock: bool,
}

impl<S: Strategy> SerialStrategy<S> {
    pub fn new(
        identity: StrategyIdentity,
        strategy: S,
        store: Arc<dyn StrategyTransaction>,
        configuration: BTreeMap<String, Value>,
    ) -> Result<Self> {
        identity.scope.validate()?;
        ensure!(
            !identity.workflow_id.is_nil() && identity.node_id > 0 && !identity.run_id.is_nil(),
            "invalid strategy identity"
        );
        Ok(Self {
            identity,
            strategy: Mutex::new(strategy),
            store,
            configuration: Arc::new(configuration),
            virtual_clock: false,
        })
    }

    pub fn with_replay_clock(mut self) -> Self {
        self.virtual_clock = true;
        self
    }

    pub async fn on_data(
        &self,
        input: DurableMessage<StrategyEvent>,
        now: DateTime<Utc>,
        portfolio: Arc<Value>,
    ) -> Result<bool> {
        self.validate_input(&input)?;
        ensure!(input.available_time <= now, "strategy input is not yet available");
        let durable: DurableMessage<Value> = serde_json::from_value(serde_json::to_value(&input)?)?;
        if self.store.processed(&self.identity, &durable).await? {
            return Ok(false);
        }
        ensure!(
            input.payload.event_time <= input.payload.available_time && input.payload.available_time <= now,
            "strategy cannot observe future data"
        );
        let mut strategy = self.strategy.lock().await;
        let checkpoint = self.store.load(&self.identity).await?;
        let expected = checkpoint.version;
        let mut context = self.context(checkpoint, now, portfolio);
        let effects = strategy.on_data(&mut context, &input.payload);
        self.store
            .record_logs(&self.identity, &durable, std::mem::take(&mut context.logs))
            .await?;
        let effects = context.finish(effects?)?;
        self.store
            .commit(
                &self.identity,
                &durable,
                expected,
                StrategyCheckpoint {
                    version: expected + 1,
                    state: context.state,
                    hyperparameters: context.hyperparameters,
                },
                effects,
            )
            .await
    }

    pub async fn on_hyperparameters(
        &self,
        input: DurableMessage<HyperparametersUpdate>,
        now: DateTime<Utc>,
        portfolio: Arc<Value>,
    ) -> Result<bool> {
        self.validate_input(&input)?;
        ensure!(
            input.available_time <= now,
            "strategy hyperparameters are not yet available"
        );
        let durable: DurableMessage<Value> = serde_json::from_value(serde_json::to_value(&input)?)?;
        if self.store.processed(&self.identity, &durable).await? {
            return Ok(false);
        }
        let mut strategy = self.strategy.lock().await;
        let mut checkpoint = self.store.load(&self.identity).await?;
        if let Some(previous) = &checkpoint.hyperparameters {
            if input.payload.version == previous.version {
                ensure!(
                    serde_json::to_value(&input.payload)? == serde_json::to_value(previous)?,
                    "hyperparameter version reused with different content"
                );
            }
            if input.payload.version <= previous.version {
                let expected = checkpoint.version;
                checkpoint.version += 1;
                return self
                    .store
                    .commit(&self.identity, &durable, expected, checkpoint, Effects::default())
                    .await;
            }
        }
        // A delayed, well-formed update is a terminal no-op. Revalidating its
        // wall-clock expiry on every QoS retry would block the serial instance
        // forever and prevent newer market data from reaching the strategy.
        input.payload.validate(std::cmp::min(now, input.payload.as_of))?;
        if input.payload.valid_until <= now {
            let expected = checkpoint.version;
            checkpoint.version += 1;
            return self
                .store
                .commit(&self.identity, &durable, expected, checkpoint, Effects::default())
                .await;
        }
        let expected = checkpoint.version;
        checkpoint.hyperparameters = Some(input.payload.clone());
        let mut context = self.context(checkpoint, now, portfolio);
        context.state.regime = Some(input.payload.probabilities.dominant()?);
        let effects = strategy.on_hyperparameters(&mut context, &input.payload);
        self.store
            .record_logs(&self.identity, &durable, std::mem::take(&mut context.logs))
            .await?;
        let effects = context.finish(effects?)?;
        self.store
            .commit(
                &self.identity,
                &durable,
                expected,
                StrategyCheckpoint {
                    version: expected + 1,
                    state: context.state,
                    hyperparameters: context.hyperparameters,
                },
                effects,
            )
            .await
    }

    fn context(&self, checkpoint: StrategyCheckpoint, clock: DateTime<Utc>, portfolio: Arc<Value>) -> StrategyContext {
        StrategyContext {
            identity: self.identity.clone(),
            clock,
            virtual_clock: self.virtual_clock,
            configuration: self.configuration.clone(),
            hyperparameters: checkpoint
                .hyperparameters
                .filter(|update| update.as_of <= clock && clock < update.valid_until),
            portfolio,
            pending: RefCell::new(Vec::new()),
            logs: Vec::new(),
            state: checkpoint.state,
        }
    }

    fn validate_input<T>(&self, input: &DurableMessage<T>) -> Result<()> {
        input.validate()?;
        ensure!(
            input.scope == self.identity.scope
                && input.workflow_id == Some(self.identity.workflow_id)
                && input.run_id == self.identity.run_id
                && input.target_node_id.is_none_or(|node| node == self.identity.node_id),
            "strategy input belongs to a different owner"
        );
        Ok(())
    }
}
