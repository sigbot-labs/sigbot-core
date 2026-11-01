// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::messager::storage::postgres::{MessagerPostgresRepository, PendingPostgresMessage};
use anyhow::Result;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::messager::topic::MessageScope;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone)]
pub struct MessagerHandler {
    repository: MessagerPostgresRepository,
}

pub struct PendingMessage<'a> {
    repository: PendingPostgresMessage<'a>,
}

impl MessagerHandler {
    pub fn new(pool: PgPool) -> Self {
        Self {
            repository: MessagerPostgresRepository::new(pool),
        }
    }

    pub(crate) fn from_repository(repository: MessagerPostgresRepository) -> Self {
        Self { repository }
    }

    pub async fn enqueue(
        &self,
        producer: &str,
        topic: &str,
        message: &DurableMessage<serde_json::Value>,
    ) -> Result<bool> {
        let mut transaction = self.repository.pool().begin().await?;
        let inserted = MessagerPostgresRepository::enqueue(&mut transaction, producer, topic, message).await?;
        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn accept(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<serde_json::Value>,
    ) -> Result<bool> {
        let mut transaction = self.repository.pool().begin().await?;
        let accepted = MessagerPostgresRepository::accept(&mut transaction, consumer, topic, message).await?;
        transaction.commit().await?;
        Ok(accepted)
    }

    pub async fn claim(&self, producer: &str) -> Result<Option<PendingMessage<'_>>> {
        Ok(self
            .repository
            .claim(producer)
            .await?
            .map(|repository| PendingMessage { repository }))
    }

    pub async fn replay(
        &self,
        producer: &str,
        scope: &MessageScope,
        node_id: i64,
        run_id: &Uuid,
        sequences: std::ops::RangeInclusive<i64>,
    ) -> Result<u64> {
        self.repository
            .replay(producer, scope, node_id, run_id, sequences)
            .await
    }
}

impl PendingMessage<'_> {
    pub fn message_mut(&mut self) -> &mut DurableMessage<serde_json::Value> {
        &mut self.repository.message
    }

    pub fn topic(&self) -> &str {
        &self.repository.topic
    }

    pub fn attempt(&self) -> i32 {
        self.repository.attempt
    }

    pub async fn complete(self, delivered: bool) -> Result<()> {
        self.repository.complete(delivered).await
    }
}
