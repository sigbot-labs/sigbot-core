// SPDX-License-Identifier: GPL-3.0-only
use crate::manager::{
    execution_group::ExecutionGroups,
    execution_tracker::OrderExecutionTracker,
    order_events::OrderEventWriter,
    order_processor::OrderProcessor,
    order_reservation::OrderReservation,
    risk::{InstrumentRules, OrderRiskRejection, RiskLimits, RiskPolicy},
    venue_poller::VenueExecutionPoller,
};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use serde::Deserialize;
use serde_json::Value;
use sigbot_core::modules::order::store::{
    group_postgres::ExecutionGroupPostgresRepository, postgres::OrderPostgresRepository,
};
use sigbot_exchange::client::execution_adapter::{ExecutionAdapterConfig, ExecutionAdapterFactory, ExecutionMode};
use sigbot_messager::{client::messager_factory::ISigbotMessagerClient, storage::postgres::PostgresMessageStore};
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageEnvelope},
        topic::{AccountTopic, MessageScope, WorkflowTopic},
    },
    order::{
        execution::{ExecutionFact, OrderState, OrderStatusFact},
        intent::TradeIntent,
    },
};
use sqlx::{PgPool, Row};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
    time::Duration,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderNodeConfig {
    pub scope: MessageScope,
    pub workflow_id: i64,
    pub node_id: i64,
    pub account_id: i64,
    pub input_node_ids: Vec<i64>,
    pub execution_node_ids: Vec<i64>,
    pub wallet_node_id: i64,
    #[serde(default)]
    pub execution_group_coordinator_ids: Vec<i64>,
    #[serde(default)]
    pub virtual_clock: bool,
    pub database_url_env: String,
    pub instrument: InstrumentRules,
    pub risk_limits: RiskLimits,
    pub exchange: ExecutionAdapterConfig,
}

pub struct OrderRuntime {
    pool: PgPool,
    config: OrderNodeConfig,
    processor: OrderProcessor,
    tracker: OrderExecutionTracker,
    reservation: Arc<OrderReservation>,
    groups: ExecutionGroups,
    venue: Option<VenueExecutionPoller>,
    consumer: String,
    messager: OnceLock<Arc<dyn ISigbotMessagerClient>>,
    subscribed_accounts: tokio::sync::Mutex<HashSet<i64>>,
}

impl OrderRuntime {
    pub async fn from_properties(properties: &HashMap<String, String>) -> Result<Arc<Self>> {
        let config: OrderNodeConfig = serde_json::from_str(
            properties
                .get("runtime_config")
                .context("order runtime_config required")?,
        )?;
        config.scope.validate()?;
        ensure!(
            !config.virtual_clock || config.exchange.mode == ExecutionMode::Simulated,
            "virtual order clock requires the isolated simulated adapter"
        );
        ensure!(
            config.workflow_id > 0 && config.node_id > 0 && config.account_id > 0 && !config.input_node_ids.is_empty(),
            "order Workflow edges required"
        );
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&std::env::var(&config.database_url_env)?)
            .await?;
        let consumer = OrderEventWriter::owner(&config.scope, config.workflow_id, config.node_id);
        let reservation = Arc::new(OrderReservation::new(
            pool.clone(),
            config.scope.clone(),
            config.node_id,
            config.wallet_node_id,
            consumer.clone(),
            config.workflow_id,
        )?);
        let adapter = ExecutionAdapterFactory::create(&config.exchange)?;
        let processor = OrderProcessor::new(
            OrderPostgresRepository::new(pool.clone()),
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
                pool.clone(),
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
            pool.clone(),
            config.scope.clone(),
            config.node_id,
            source_node_ids,
            config.workflow_id,
        )?
        .with_instrument(config.instrument.clone())?;
        let groups = ExecutionGroups::new(
            pool.clone(),
            config.scope.clone(),
            config.workflow_id,
            config.node_id,
            consumer.clone(),
            config.virtual_clock,
        );
        Ok(Arc::new(Self {
            venue,
            groups,
            pool,
            config,
            processor,
            tracker,
            reservation,
            consumer,
            messager: OnceLock::new(),
            subscribed_accounts: Default::default(),
        }))
    }

    pub async fn start(self: &Arc<Self>, messager: Arc<dyn ISigbotMessagerClient>) -> Result<()> {
        common_telemetry::readiness::RuntimeReadiness::business("order", false);
        ensure!(
            self.messager.set(messager.clone()).is_ok(),
            "order runtime already started"
        );
        self.subscribe_account(self.config.account_id).await?;
        let topic = WorkflowTopic::StrategySignal.topic(&self.config.scope, self.config.workflow_id)?;
        let runtime = self.clone();
        messager
            .subscribe(
                &topic,
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
        tokio::spawn(async move {
            readiness.set(true);
            let store = PostgresMessageStore::new(runtime.pool.clone());
            loop {
                if let Err(error) = store
                    .dispatch_pending(&runtime.consumer, messager.as_ref(), 32, &|parent| {
                        Ok(MessageTrace::child("order.retry", parent).traceparent())
                    })
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
        common_telemetry::readiness::RuntimeReadiness::business("order", true);
        Ok(())
    }

    pub async fn consume(self: &Arc<Self>, bytes: &[u8]) -> Result<()> {
        let mut value: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        PostgresMessageStore::new(self.pool.clone())
            .restore_legacy_workflow(&mut value)
            .await?;
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
        if !self.config.input_node_ids.contains(&input.node_id) {
            if !self.config.execution_group_coordinator_ids.contains(&input.node_id)
                || !ExecutionGroupPostgresRepository::is_compensation(&self.pool, input, self.config.node_id).await?
            {
                return self
                    .quarantine_route(
                        &value,
                        &WorkflowTopic::StrategySignal.topic(&self.config.scope, self.config.workflow_id)?,
                    )
                    .await;
            }
        }
        let clock = if self.config.virtual_clock {
            sqlx::query_scalar::<_, chrono::DateTime<Utc>>(
                "SELECT virtual_time FROM backtest_checkpoint WHERE env=$1 AND tenant_id=$2 AND space_id=$3
                AND run_id=$4 AND state='running'",
            )
            .bind(&input.scope.env)
            .bind(input.scope.tenant_id)
            .bind(input.scope.space_id)
            .bind(&input.run_id)
            .fetch_optional(&self.pool)
            .await?
            .context("replay Order requires a running scoped clock checkpoint")?
        } else if self.config.exchange.mode == ExecutionMode::Simulated {
            input.available_time
        } else {
            Utc::now()
        };
        // Wait for broker SUBACK before any venue can publish a fast execution.
        self.subscribe_account(input.payload.account_id).await?;
        let _span = MessageTrace::child("order.consume", &input.traceparent);
        let topic = WorkflowTopic::StrategySignal.topic(&self.config.scope, self.config.workflow_id)?;
        let mut tx = self.pool.begin().await?;
        if !PostgresMessageStore::accept(&mut tx, &self.consumer, &topic, &value).await? {
            tx.commit().await?;
            return Ok(());
        }
        if let Err(error) = ExecutionGroupPostgresRepository::register(
            &mut tx,
            input,
            self.config.node_id,
            &self.config.execution_group_coordinator_ids,
            clock,
        )
        .await
        {
            if error.downcast_ref::<sqlx::Error>().is_some() {
                return Err(error);
            }
            self.quarantine(&mut tx, input, clock).await?;
            tx.commit().await?;
            tracing::warn!(run_id=%input.run_id,%error,"execution group admission rejected");
            return Ok(());
        }
        tx.commit().await?; // Durable intake survives external submit/reconcile.

        match self.processor.process(input, clock).await {
            Ok(OrderState::Pending | OrderState::Unknown) => return Ok(()),
            Err(error) if error.downcast_ref::<OrderRiskRejection>().is_some() => {
                let mut tx = self.pool.begin().await?;
                self.quarantine(&mut tx, input, clock).await?;
                tx.commit().await?;
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
        let mut tx = self.pool.begin().await?;
        if PostgresMessageStore::accept(&mut tx, &self.consumer, &topic, &value).await? {
            PostgresMessageStore::complete(&mut tx, &self.consumer, &value.durable).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn quarantine(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        input: &DurableMessage<TradeIntent>,
        clock: chrono::DateTime<Utc>,
    ) -> Result<()> {
        let changed=sqlx::query("UPDATE messager_inbox SET state='quarantined',completed_at=NOW()
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND consumer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7 AND state='pending'")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.scope.space_id).bind(&self.consumer)
            .bind(input.node_id).bind(&input.run_id).bind(input.seq).execute(&mut **tx).await?.rows_affected();
        if changed == 1 {
            ExecutionGroupPostgresRepository::reject(tx, input).await?;
            OrderEventWriter::enqueue(
                tx,
                &self.consumer,
                self.config.node_id,
                input,
                OrderState::Rejected,
                None,
                clock,
            )
            .await?;
        }
        Ok(())
    }

    async fn resume_pending(self: &Arc<Self>) -> Result<()> {
        let rows = sqlx::query(
            "SELECT payload FROM messager_inbox WHERE consumer=$1 AND state='pending'
             AND (payload->>'workflow_id')::bigint=$2 ORDER BY received_at LIMIT 32",
        )
        .bind(&self.consumer)
        .bind(self.config.workflow_id)
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let durable: DurableMessage<Value> = serde_json::from_value(row.get("payload"))?;
            let wire = MessageEnvelope::from_durable(durable)?;
            if let Err(error) = self.consume(&serde_json::to_vec(&wire)?).await {
                tracing::warn!(run_id=%wire.durable.run_id, %error, "pending order requires reconciliation");
            }
        }
        Ok(())
    }

    async fn subscribe_account(self: &Arc<Self>, account_id: i64) -> Result<()> {
        ensure!(
            account_id == self.config.account_id,
            "order worker is bound to a different account"
        );
        let mut registered = self.subscribed_accounts.lock().await;
        if registered.contains(&account_id) {
            return Ok(());
        }
        let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM s_wallets WHERE account_id=$1 AND env=$2 AND tenant_id=$3 AND space_id=$4 AND del_flag=FALSE)")
            .bind(account_id).bind(&self.config.scope.env).bind(self.config.scope.tenant_id.to_string()).bind(self.config.scope.space_id)
            .fetch_one(&self.pool).await?;
        ensure!(owned, "account does not belong to the order scope");
        let messager = self.messager.get().context("order runtime is not started")?;
        let topic = AccountTopic::WalletReservationResult.topic(&self.config.scope, account_id)?;
        let runtime = self.clone();
        messager
            .subscribe(
                &topic,
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
            messager
                .subscribe(
                    &topic,
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
        let mut wire: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        PostgresMessageStore::new(self.pool.clone())
            .restore_legacy_workflow(&mut wire)
            .await?;
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
        let mut tx = self.pool.begin().await?;
        if PostgresMessageStore::accept(&mut tx, &consumer, topic, wire).await? {
            sqlx::query("UPDATE messager_inbox SET state='quarantined',completed_at=NOW(),quarantine_reason='outside_order_route'
                WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND consumer=$4 AND node_id=$5 AND run_id=$6 AND seq=$7")
                .bind(&wire.durable.scope.env).bind(wire.durable.scope.tenant_id).bind(wire.durable.scope.space_id)
                .bind(&consumer).bind(wire.durable.node_id).bind(&wire.durable.run_id).bind(wire.durable.seq)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
