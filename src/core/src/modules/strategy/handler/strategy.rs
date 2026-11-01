// SPDX-License-Identifier: GPL-3.0-only

use crate::config::config::PostgresStorageProperties;
use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::strategy::handler::input::StrategyInputHandler;
use crate::modules::strategy::storage::postgres::StrategyPostgresRepository;
use crate::modules::wallet::handler::wallet::WalletPortfolioHandler;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::strategy::execution::Effects;
use uuid::Uuid;

pub use crate::modules::strategy::storage::postgres::{ProducerTrace, StrategyStorageIdentity};

pub struct StrategyHandler {
    repository: StrategyPostgresRepository,
    input: StrategyInputHandler,
    portfolio: WalletPortfolioHandler,
}

impl StrategyHandler {
    pub async fn connect(storage: &PostgresStorageProperties, producer_trace: ProducerTrace) -> Result<Self> {
        Ok(Self {
            repository: StrategyPostgresRepository::connect(storage, producer_trace).await?,
            input: StrategyInputHandler::connect(storage).await?,
            portfolio: WalletPortfolioHandler::connect(storage).await?,
        })
    }

    pub fn with_replay_clock(mut self) -> Self {
        self.repository = self.repository.with_replay_clock();
        self
    }

    pub fn messager(&self) -> MessagerHandler {
        self.repository.messager()
    }

    pub fn producer(identity: StrategyStorageIdentity<'_>) -> String {
        StrategyPostgresRepository::producer(identity)
    }

    pub async fn load(&self, identity: StrategyStorageIdentity<'_>) -> Result<Option<Value>> {
        self.repository.load(identity).await
    }

    pub async fn processed(
        &self,
        identity: StrategyStorageIdentity<'_>,
        input: &DurableMessage<Value>,
    ) -> Result<bool> {
        self.input.processed(identity, input).await
    }

    pub async fn replay_clock(
        &self,
        identity: StrategyStorageIdentity<'_>,
        input: &DurableMessage<Value>,
    ) -> Result<DateTime<Utc>> {
        self.input.replay_clock(identity, input).await
    }

    pub async fn quarantine(
        &self,
        consumer: &str,
        topic: &str,
        wire: &MessageEnvelope<Value>,
        reason: &str,
    ) -> Result<bool> {
        self.input.quarantine(consumer, topic, wire, reason).await
    }

    pub async fn portfolio(&self, scope: &MessageScope, account_id: Uuid) -> Result<Value> {
        self.portfolio.snapshot(scope, account_id).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit(
        &self,
        identity: StrategyStorageIdentity<'_>,
        input_topic: &str,
        input: &DurableMessage<Value>,
        expected_version: i64,
        checkpoint_version: i64,
        checkpoint: Value,
        effects: Effects,
    ) -> Result<bool> {
        self.repository
            .commit(
                identity,
                input_topic,
                input,
                expected_version,
                checkpoint_version,
                checkpoint,
                effects,
            )
            .await
    }
}
