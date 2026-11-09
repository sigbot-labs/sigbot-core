// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::datafeed::storage::postgres::{MarketQueryPostgresRepository, MarketQueryPostgresSession};
use crate::modules::messager::handler::messager::MessagerHandler;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sigbot_types::datafeed::event::DatafeedEvent;
use sigbot_types::datafeed::event::DatafeedPublication;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::strategy::execution::{ComputePlan, SnapshotPoint, SnapshotQuery, SnapshotResult, StrategyEventData};
use uuid::Uuid;

#[derive(Clone)]
pub struct MarketQueryHandler {
    repository: MarketQueryPostgresRepository,
}

pub struct MarketQuerySession {
    repository: MarketQueryPostgresSession,
}

impl MarketQueryHandler {
    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self {
            repository: MarketQueryPostgresRepository::from_pool(pool),
        }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            repository: MarketQueryPostgresRepository::connect(database_url).await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::from_repository(self.repository.message_store())
    }

    pub async fn admit(&self, owner: &str, topic: &str, message: &MessageEnvelope<Value>) -> Result<bool> {
        self.repository.admit(owner, topic, message).await
    }

    pub async fn begin(
        &self,
        owner: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<Option<MarketQuerySession>> {
        Ok(self
            .repository
            .begin(owner, topic, message)
            .await?
            .map(|repository| MarketQuerySession { repository }))
    }

    pub async fn query(&self) -> Result<MarketQuerySession> {
        Ok(MarketQuerySession {
            repository: self.repository.query().await?,
        })
    }

    pub fn statement(plan: &ComputePlan, timescale: bool) -> String {
        MarketQueryPostgresSession::statement(plan, timescale)
    }
}

impl MarketQuerySession {
    pub async fn virtual_time(&mut self, scope: &MessageScope, run_id: &Uuid) -> Result<DateTime<Utc>> {
        self.repository.virtual_time(scope, run_id).await
    }

    pub async fn persist(
        &mut self,
        publication: &DatafeedPublication<'_>,
        event: DatafeedEvent,
    ) -> Result<DatafeedEvent> {
        self.repository.persist(publication, event).await
    }

    pub async fn snapshot(
        &mut self,
        scope: &MessageScope,
        run_id: &Uuid,
        query: &SnapshotQuery,
        now: DateTime<Utc>,
    ) -> Result<SnapshotResult> {
        self.repository.snapshot(scope, run_id, query, now).await
    }

    pub async fn history(
        &mut self,
        scope: &MessageScope,
        run_id: &Uuid,
        query: &SnapshotQuery,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SnapshotPoint>> {
        self.repository.history(scope, run_id, query, start, end, as_of).await
    }

    pub async fn compute(
        &mut self,
        scope: &MessageScope,
        run_id: &Uuid,
        plan: &ComputePlan,
    ) -> Result<StrategyEventData> {
        self.repository.compute(scope, run_id, plan).await
    }

    pub async fn next_sequence(&mut self, scope: &MessageScope, node_id: i64, run_id: &Uuid) -> Result<i64> {
        self.repository.next_sequence(scope, node_id, run_id).await
    }

    pub async fn enqueue<T: Serialize>(&mut self, owner: &str, topic: &str, message: &DurableMessage<T>) -> Result<()> {
        self.repository.enqueue(owner, topic, message).await
    }

    pub async fn checkpoint_compute(
        &mut self,
        scope: &MessageScope,
        workflow_id: Uuid,
        node_id: i64,
        input: &DurableMessage<Value>,
        result: &DurableMessage<Value>,
        watermark: DateTime<Utc>,
    ) -> Result<()> {
        self.repository
            .checkpoint_compute(scope, workflow_id, node_id, input, result, watermark)
            .await
    }

    pub async fn complete(self, owner: &str, input: &DurableMessage<Value>) -> Result<()> {
        self.repository.complete(owner, input).await
    }

    pub async fn rollback(self) -> Result<()> {
        self.repository.rollback().await
    }
}
