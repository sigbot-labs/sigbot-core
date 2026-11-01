// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::backtest::storage::postgres::clock::BacktestClockPostgresRepository;
use crate::modules::backtest::storage::postgres::history::BacktestHistoryPostgresRepository;
use crate::modules::backtest::storage::postgres::session::BacktestReplayPostgresRepository;
use crate::modules::backtest::storage::postgres::BacktestPostgresRepository;
use crate::modules::messager::handler::messager::MessagerHandler;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::datafeed::event::DatafeedEvent;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::intent::TradeIntent;
use uuid::Uuid;

pub use crate::modules::backtest::storage::postgres::clock::{BarrierPayloadKind, BarrierQuery, BarrierRoute};
pub use crate::modules::backtest::storage::postgres::session::ReplayDelivery;
pub use crate::modules::backtest::storage::postgres::{BacktestTransaction, RestingOrder, RestingOrderCursor};
pub use sigbot_types::backtest::{HistoricalRecord, HistoryCursor, ReplayCheckpoint, ReplayHistory, ReplayIdentity};

#[derive(Clone)]
pub struct BacktestHandler {
    repository: BacktestPostgresRepository,
}

pub struct BacktestReplayHandler {
    repository: BacktestReplayPostgresRepository,
    history: BacktestHistoryPostgresRepository,
    clock: BacktestClockPostgresRepository,
}

#[derive(Clone)]
pub struct BacktestBarrierHandler {
    repository: BacktestClockPostgresRepository,
}

impl BacktestHandler {
    pub async fn connect(database_url: &str, scope: MessageScope, node_id: i64) -> Result<Self> {
        Ok(Self {
            repository: BacktestPostgresRepository::connect(database_url, scope, node_id).await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        self.repository.messager()
    }

    pub async fn begin(&self) -> Result<BacktestTransaction> {
        self.repository.begin().await
    }

    pub async fn virtual_time(&self, run_id: &Uuid) -> Result<DateTime<Utc>> {
        self.repository.virtual_time(run_id).await
    }

    pub async fn quarantine_route(&self, consumer: &str, topic: &str, message: &MessageEnvelope<Value>) -> Result<()> {
        self.repository.quarantine_route(consumer, topic, message).await
    }

    pub async fn reconcile(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<String>> {
        self.repository.reconcile(message).await
    }

    pub async fn insert_order(
        &self,
        transaction: &mut BacktestTransaction,
        input: &DurableMessage<TradeIntent>,
        exchange_order_id: &str,
        accepted_at: DateTime<Utc>,
        matching_configuration: &Value,
    ) -> Result<bool> {
        self.repository
            .insert_order(
                transaction,
                input,
                exchange_order_id,
                accepted_at,
                matching_configuration,
            )
            .await
    }

    pub async fn accept_market(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<Option<BacktestTransaction>> {
        self.repository.accept_market(consumer, topic, message).await
    }

    pub async fn verify_tick(
        &self,
        transaction: &mut BacktestTransaction,
        input: &DurableMessage<DatafeedEvent>,
    ) -> Result<()> {
        self.repository.verify_tick(transaction, input).await
    }

    pub async fn resting_orders(
        &self,
        transaction: &mut BacktestTransaction,
        input: &DurableMessage<DatafeedEvent>,
        cursor: Option<&RestingOrderCursor>,
        exchange: &str,
        symbol: &str,
    ) -> Result<Vec<RestingOrder>> {
        self.repository
            .resting_orders(transaction, input, cursor, exchange, symbol)
            .await
    }

    pub async fn fill<T: serde::Serialize>(
        &self,
        transaction: &mut BacktestTransaction,
        producer: &str,
        topic: &str,
        order: &DurableMessage<TradeIntent>,
        event_time: DateTime<Utc>,
        output: &DurableMessage<T>,
    ) -> Result<()> {
        self.repository
            .fill(transaction, producer, topic, order, event_time, output)
            .await
    }

    pub async fn next_sequence(&self, transaction: &mut BacktestTransaction, run_id: &Uuid) -> Result<i64> {
        self.repository.next_sequence(transaction, run_id).await
    }

    pub async fn complete_market(
        &self,
        transaction: BacktestTransaction,
        consumer: &str,
        input: &DurableMessage<Value>,
    ) -> Result<()> {
        self.repository.complete_market(transaction, consumer, input).await
    }
}

impl BacktestReplayHandler {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            repository: BacktestReplayPostgresRepository::connect(database_url).await?,
            history: BacktestHistoryPostgresRepository::connect(database_url).await?,
            clock: BacktestClockPostgresRepository::connect(database_url).await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        self.repository.messager()
    }

    pub async fn acquire_owner(&self, identity: ReplayIdentity<'_>, replay_of: &Uuid) -> Result<()> {
        self.repository.acquire_owner(identity, replay_of).await
    }

    pub async fn release_owner(&self, run_id: &Uuid) {
        self.repository.release_owner(run_id).await;
    }

    pub async fn assert_owner(&self) -> Result<()> {
        self.repository.assert_owner().await
    }

    pub async fn begin_delivery(
        &self,
        identity: ReplayIdentity<'_>,
        history: ReplayHistory<'_>,
        record: &HistoricalRecord,
        node_id: i64,
        producer: &str,
        clock: DateTime<Utc>,
    ) -> Result<ReplayDelivery> {
        self.repository
            .begin_delivery(identity, history, record, node_id, producer, clock)
            .await
    }

    pub async fn checkpoint(
        &self,
        identity: ReplayIdentity<'_>,
        checkpoint: ReplayCheckpoint,
        complete: bool,
    ) -> Result<()> {
        self.repository.checkpoint(identity, checkpoint, complete).await
    }

    pub async fn page(
        &self,
        history: ReplayHistory<'_>,
        after: Option<&HistoryCursor>,
        limit: u32,
    ) -> Result<Vec<HistoricalRecord>> {
        self.history.page(history, after, limit).await
    }

    pub async fn restore(
        &self,
        identity: ReplayIdentity<'_>,
        configuration: &Value,
        frozen_at: DateTime<Utc>,
        initial_time: DateTime<Utc>,
    ) -> Result<ReplayCheckpoint> {
        self.history
            .restore(identity, configuration, frozen_at, initial_time)
            .await
    }

    pub async fn import_records(
        &self,
        identity: ReplayIdentity<'_>,
        history: ReplayHistory<'_>,
        records: &[HistoricalRecord],
        virtual_time: DateTime<Utc>,
    ) -> Result<()> {
        self.history
            .import_records(identity, history, records, virtual_time)
            .await
    }

    pub async fn next_timer(&self, identity: ReplayIdentity<'_>) -> Result<Option<DateTime<Utc>>> {
        self.clock.next_timer(identity).await
    }

    pub async fn fire_timers(&self, identity: ReplayIdentity<'_>, clock: DateTime<Utc>, limit: u32) -> Result<u32> {
        self.clock.fire_timers(identity, clock, limit).await
    }
}

impl BacktestBarrierHandler {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            repository: BacktestClockPostgresRepository::connect(database_url).await?,
        })
    }

    pub async fn settled(&self, query: BarrierQuery<'_>) -> Result<bool> {
        self.repository.settled(query).await
    }
}
