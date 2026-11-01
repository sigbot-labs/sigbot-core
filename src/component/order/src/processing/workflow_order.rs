// SPDX-License-Identifier: GPL-3.0-only
use crate::execution::events::OrderEventWriter;
use crate::execution::groups::ExecutionGroups;
use crate::execution::tracker::OrderExecutionTracker;
use crate::execution::venue::VenueExecutionPoller;
use crate::order::IOrder;
use crate::processing::processor::OrderProcessor;
use crate::processing::reservation::OrderReservation;
use crate::processing::risk::InstrumentRules;
use crate::processing::risk::OrderRiskRejection;
use crate::processing::risk::RiskLimits;
use crate::processing::risk::RiskPolicy;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use common_context::IProvider;
use common_telemetry::tracing_context::MessageTrace;
use serde::Deserialize;
use serde_json::Value;
use sigbot_core::config::config::get_config;
use sigbot_core::modules::order::handler::order::{OrderHandler, OrderRecordHandler, SignalAdmission};
use sigbot_exchange::exchange::ExecutionAdapterConfig;
use sigbot_exchange::exchange::ExecutionMode;
use sigbot_exchange::server::ExchangeServer;
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::SubscriptionSet;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::Uuid;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderNodeConfig {
    pub scope: MessageScope,
    pub workflow_id: Uuid,
    pub node_id: i64,
    pub account_id: Uuid,
    pub input_node_ids: Vec<i64>,
    pub execution_node_ids: Vec<i64>,
    pub wallet_node_id: i64,
    #[serde(default)]
    pub execution_group_coordinator_ids: Vec<i64>,
    #[serde(default)]
    pub virtual_clock: bool,
    pub instrument: InstrumentRules,
    pub risk_limits: RiskLimits,
    pub exchange: ExecutionAdapterConfig,
}

pub struct WorkflowOrderProcessor {
    storage: OrderHandler,
    config: OrderNodeConfig,
    processor: OrderProcessor,
    tracker: OrderExecutionTracker,
    reservation: Arc<OrderReservation>,
    groups: ExecutionGroups,
    venue: Option<VenueExecutionPoller>,
    consumer: String,
    messager: OnceLock<Arc<dyn IMessager>>,
    subscriptions: SubscriptionSet,
    subscribed_accounts: tokio::sync::Mutex<HashSet<Uuid>>,
    tasks: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl WorkflowOrderProcessor {
    pub async fn from_properties(properties: &HashMap<String, String>) -> Result<Arc<Self>> {
        let config: OrderNodeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("order runtime_config required")?,
        )?;
        config.scope.validate()?;
        ensure!(
            !config.virtual_clock || config.exchange.mode == ExecutionMode::Backtest,
            "virtual order clock requires the isolated backtest adapter"
        );
        ensure!(
            config.workflow_id.get_version_num() == 7
                && config.node_id > 0
                && config.account_id.get_version_num() == 7
                && !config.input_node_ids.is_empty(),
            "order Workflow edges required"
        );
        let database_url = get_config().storage.postgres.connection_url()?;
        let storage = OrderHandler::connect(&database_url).await?;
        let consumer = OrderEventWriter::owner(&config.scope, config.workflow_id, config.node_id);
        let reservation = Arc::new(OrderReservation::new(
            &storage,
            config.scope.clone(),
            config.node_id,
            config.wallet_node_id,
            consumer.clone(),
            config.workflow_id,
        )?);
        let adapter = ExchangeServer::connect(&config.exchange).await?;
        let processor = OrderProcessor::new(
            OrderRecordHandler::new(&storage),
            reservation.clone(),
            adapter.clone(),
            RiskPolicy {
                limits: config.risk_limits.clone(),
                instrument: config.instrument.clone(),
            },
            config.node_id,
            config.scope.clone(),
            config.workflow_id,
        )?;
        let mut source_node_ids = config.execution_node_ids.clone();
        let venue = if config.exchange.mode == ExecutionMode::Live {
            source_node_ids.push(config.node_id);
            Some(VenueExecutionPoller::new(
                storage.clone(),
                config.scope.clone(),
                config.workflow_id,
                config.account_id,
                config.node_id,
                consumer.clone(),
                adapter,
                config.instrument.clone(),
            ))
        } else {
            None
        };
        let tracker = OrderExecutionTracker::new(
            &storage,
            config.scope.clone(),
            config.node_id,
            source_node_ids,
            config.workflow_id,
        )?
        .with_instrument(config.instrument.clone())?;
        let groups = ExecutionGroups::new(
            &storage,
            config.scope.clone(),
            config.workflow_id,
            config.node_id,
            consumer.clone(),
            config.virtual_clock,
        );
        Ok(Arc::new(Self {
            venue,
            groups,
            storage,
            config,
            processor,
            tracker,
            reservation,
            consumer,
            messager: OnceLock::new(),
            subscriptions: SubscriptionSet::default(),
            subscribed_accounts: Default::default(),
            tasks: Default::default(),
        }))
    }

    pub async fn start(self: &Arc<Self>, messager: Arc<dyn IMessager>) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("order", false);
        ensure!(
            self.messager.set(messager.clone()).is_ok(),
            "order runtime already started"
        );
        self.subscriptions.bind(messager.clone())?;
        self.subscribe_account(self.config.account_id).await?;
        let topic = WorkflowTopic::StrategySignal.topic(&self.config.scope, self.config.workflow_id)?;
        let runtime = self.clone();
        self.subscriptions
            .subscribe(
                topic,
                format!("{}:strategy", self.consumer),
                Arc::new(move |data| {
                    let runtime = runtime.clone();
                    Box::pin(async move {
                        runtime.consume(&data).await?;
                        Ok("committed".into())
                    })
                }),
            )
            .await?;
        let runtime = self.clone();
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("order.recovery");
        let task = tokio::spawn(async move {
            readiness.set(true);
            let store = runtime.storage.messager();
            loop {
                if let Err(error) = sigbot_messager::messager::OutboxRelay::dispatch_pending(
                    &store,
                    &runtime.consumer,
                    messager.as_ref(),
                    32,
                    &|parent| Ok(MessageTrace::child("order.retry", parent).traceparent()),
                )
                .await
                {
                    tracing::warn!(%error, "order outbox retry pending");
                }
                if let Err(error) = runtime.resume_pending().await {
                    tracing::warn!(%error, "order reconciliation pending");
                }
                if let Some(venue) = &runtime.venue {
                    if let Err(error) = venue.tick(&runtime.tracker, messager.as_ref()).await {
                        tracing::warn!(%error,"venue executions await reconciliation");
                    }
                }
                if runtime
                    .config
                    .execution_group_coordinator_ids
                    .contains(&runtime.config.node_id)
                {
                    if let Err(error) = runtime.groups.advance(Utc::now()).await {
                        tracing::warn!(%error,"execution group awaits reconciliation");
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
        self.tasks.lock().await.push(task);
        common_telemetry::readiness::RuntimeReadiness::business("order", true);
        Ok(())
    }

    pub async fn consume(self: &Arc<Self>, bytes: &[u8]) -> Result<()> {
        let value: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        let wire: MessageEnvelope<TradeIntent> = serde_json::from_value(serde_json::to_value(&value)?)?;
        wire.validate()?;
        let input = &wire.durable;
        ensure!(
            input.scope == self.config.scope && input.workflow_id == Some(self.config.workflow_id),
            "order scope mismatch"
        );
        if input.target_node_id.is_some_and(|id| id != self.config.node_id)
            || input.payload.account_id != self.config.account_id
        {
            return self
                .quarantine_route(
                    &value,
                    &WorkflowTopic::StrategySignal.topic(&self.config.scope, self.config.workflow_id)?,
                )
                .await;
        }
        if !self.config.input_node_ids.contains(&input.node_id)
            && (!self.config.execution_group_coordinator_ids.contains(&input.node_id)
                || !self.storage.is_compensation(input, self.config.node_id).await?)
        {
            return self
                .quarantine_route(
                    &value,
                    &WorkflowTopic::StrategySignal.topic(&self.config.scope, self.config.workflow_id)?,
                )
                .await;
        }
        let clock = if self.config.virtual_clock {
            self.storage.virtual_time(&input.scope, &input.run_id).await?
        } else if self.config.exchange.mode == ExecutionMode::Backtest {
            input.available_time
        } else {
            Utc::now()
        };
        // Wait for broker SUBACK before any venue can publish a fast execution.
        self.subscribe_account(input.payload.account_id).await?;
        let _span = MessageTrace::child("order.consume", &input.traceparent);
        let topic = WorkflowTopic::StrategySignal.topic(&self.config.scope, self.config.workflow_id)?;
        match self
            .storage
            .admit_signal(
                &self.consumer,
                &topic,
                &value,
                input,
                self.config.node_id,
                &self.config.execution_group_coordinator_ids,
                clock,
            )
            .await?
        {
            SignalAdmission::Duplicate => return Ok(()),
            SignalAdmission::Rejected(reason) => {
                tracing::warn!(run_id=%input.run_id,%reason,"execution group admission rejected");
                return Ok(());
            }
            SignalAdmission::Accepted => {}
        }

        match self.processor.process(input, clock).await {
            Ok(OrderState::Pending | OrderState::Unknown) => return Ok(()),
            Err(error) if error.downcast_ref::<OrderRiskRejection>().is_some() => {
                self.storage
                    .reject_signal(&self.consumer, input, self.config.node_id, clock)
                    .await?;
                tracing::warn!(run_id=%input.run_id,%error,"order rejected by hard risk policy");
                return Ok(());
            }
            Err(error) => {
                // Intake already committed. Let the durable recovery loop retry while
                // the MQTT worker remains free to receive Wallet/venue responses.
                tracing::warn!(run_id=%input.run_id,%error,"order durable processing remains pending");
                return Ok(());
            }
            Ok(_) => {}
        }
        self.storage.complete_signal(&self.consumer, &topic, &value).await
    }

    async fn resume_pending(self: &Arc<Self>) -> Result<()> {
        for durable in self.storage.pending(&self.consumer, self.config.workflow_id).await? {
            let wire = MessageEnvelope::from_durable(durable)?;
            if let Err(error) = self.consume(&serde_json::to_vec(&wire)?).await {
                tracing::warn!(run_id=%wire.durable.run_id, %error, "pending order requires reconciliation");
            }
        }
        Ok(())
    }

    async fn subscribe_account(self: &Arc<Self>, account_id: Uuid) -> Result<()> {
        ensure!(
            account_id == self.config.account_id,
            "order worker is bound to a different account"
        );
        let mut registered = self.subscribed_accounts.lock().await;
        if registered.contains(&account_id) {
            return Ok(());
        }
        let owned = self.storage.owns_account(&self.config.scope, account_id).await?;
        ensure!(owned, "account does not belong to the order scope");
        self.messager.get().context("order runtime is not started")?;
        let topic = AccountTopic::WalletReservationResult.topic(&self.config.scope, account_id)?;
        let runtime = self.clone();
        self.subscriptions
            .subscribe(
                topic,
                format!("{}:{}", self.consumer, AccountTopic::WalletReservationResult.suffix()),
                Arc::new(move |bytes| {
                    let runtime = runtime.clone();
                    Box::pin(async move {
                        runtime
                            .consume_account(AccountTopic::WalletReservationResult, &bytes)
                            .await?;
                        Ok("committed".into())
                    })
                }),
            )
            .await?;
        for kind in [AccountTopic::ExecutionEvent, AccountTopic::OrderEvent] {
            let topic = kind.topic(&self.config.scope, account_id)?;
            let runtime = self.clone();
            self.subscriptions
                .subscribe(
                    topic,
                    format!("{}:{}", self.consumer, kind.suffix()),
                    Arc::new(move |bytes| {
                        let runtime = runtime.clone();
                        Box::pin(async move {
                            runtime.consume_account(kind, &bytes).await?;
                            Ok("committed".into())
                        })
                    }),
                )
                .await?;
        }
        registered.insert(account_id);
        Ok(())
    }

    /// Persist account broadcasts outside this Workflow in a separate routing Inbox.
    /// The broker acknowledges only after this method commits or the tracker commits.
    pub async fn consume_account(&self, kind: AccountTopic, bytes: &[u8]) -> Result<()> {
        ensure!(
            matches!(
                kind,
                AccountTopic::WalletReservationResult | AccountTopic::ExecutionEvent | AccountTopic::OrderEvent
            ),
            "unsupported order account input"
        );
        let wire: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        wire.validate()?;
        let bytes = serde_json::to_vec(&wire)?;
        let topic = kind.topic(&self.config.scope, self.config.account_id)?;
        wire.durable.validate_topic(&topic)?;
        let allowed = wire.durable.workflow_id == Some(self.config.workflow_id)
            && match kind {
                AccountTopic::WalletReservationResult => {
                    wire.durable.node_id == self.config.wallet_node_id
                        && wire.durable.target_node_id == Some(self.config.node_id)
                }
                _ => {
                    wire.durable.node_id != self.config.node_id
                        && self.config.execution_node_ids.contains(&wire.durable.node_id)
                }
            };
        if !allowed {
            return self.quarantine_route(&wire, &topic).await;
        }
        match kind {
            AccountTopic::WalletReservationResult => self.reservation.consume(&bytes).await.map(|_| ()),
            AccountTopic::ExecutionEvent => self.tracker.process(&serde_json::from_slice(&bytes)?).await.map(|_| ()),
            AccountTopic::OrderEvent => self
                .tracker
                .process_status(&serde_json::from_slice(&bytes)?)
                .await
                .map(|_| ()),
            _ => unreachable!("account input checked above"),
        }
    }

    async fn quarantine_route(&self, wire: &MessageEnvelope<Value>, topic: &str) -> Result<()> {
        wire.durable.validate_topic(topic)?;
        let consumer = format!("{}:routing", self.consumer);
        self.storage.quarantine_route(&consumer, topic, wire).await
    }
}

#[async_trait]
impl IOrder for WorkflowOrderProcessor {
    async fn consume(self: Arc<Self>, bytes: &[u8]) -> Result<()> {
        WorkflowOrderProcessor::consume(&self, bytes).await
    }
}

#[async_trait]
impl IProvider for WorkflowOrderProcessor {
    async fn close(&self) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("order", false);
        let mut tasks = self.tasks.lock().await;
        for task in tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
        self.subscriptions.close().await?;
        Ok(())
    }
}
