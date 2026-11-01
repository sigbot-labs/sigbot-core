// SPDX-License-Identifier: GPL-3.0-only

use crate::config::config::PostgresStorageProperties;
use crate::modules::strategy::storage::postgres::StrategyInputPostgresRepository;
use crate::modules::strategy::storage::postgres::StrategyStorageIdentity;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};

pub struct StrategyInputHandler {
    repository: StrategyInputPostgresRepository,
}

impl StrategyInputHandler {
    pub async fn connect(storage: &PostgresStorageProperties) -> Result<Self> {
        Ok(Self {
            repository: StrategyInputPostgresRepository::connect(storage).await?,
        })
    }

    pub async fn processed(
        &self,
        identity: StrategyStorageIdentity<'_>,
        input: &DurableMessage<Value>,
    ) -> Result<bool> {
        self.repository.processed(identity, input).await
    }

    pub async fn replay_clock(
        &self,
        identity: StrategyStorageIdentity<'_>,
        input: &DurableMessage<Value>,
    ) -> Result<DateTime<Utc>> {
        self.repository.replay_clock(identity, input).await
    }

    pub async fn quarantine(
        &self,
        consumer: &str,
        topic: &str,
        wire: &MessageEnvelope<Value>,
        reason: &str,
    ) -> Result<bool> {
        self.repository.quarantine(consumer, topic, wire, reason).await
    }
}
