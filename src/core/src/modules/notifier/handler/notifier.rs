// SPDX-License-Identifier: GPL-3.0-only

use crate::context::identity::{RequestIdentity, ScopeQuery};
use crate::context::state::SigbotState;
use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::notifier::storage::postgres::NotifierPostgresRepository;
use anyhow::{ensure, Result};
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::notifier::request::NotificationRequest;
use sigbot_types::notifier::request::{NotificationScope, PendingNotification};
use uuid::Uuid;

/// Owns notification delivery business operations. Callers never access the
/// persistence adapter directly.
pub struct NotifierHandler {
    repository: NotifierPostgresRepository,
}

impl NotifierHandler {
    pub async fn connect(database_url: &str, scope: NotificationScope) -> Result<Self> {
        Ok(Self {
            repository: NotifierPostgresRepository::connect(database_url, scope).await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::from_repository(self.repository.message_store())
    }

    pub async fn acquire_owner(&self, database_url: &str) -> Result<()> {
        self.repository.acquire_owner(database_url).await
    }

    pub async fn release_owner(&self) {
        self.repository.release_owner().await;
    }

    pub async fn ensure_owner(&self) -> Result<()> {
        self.repository.ensure_owner().await
    }

    pub async fn queue(
        &self,
        topic: &str,
        message: &MessageEnvelope<Value>,
        source: &DurableMessage<NotificationRequest>,
        template_version: &str,
        title: &str,
        body: &str,
        result_topic: &str,
    ) -> Result<Option<DurableMessage<Value>>> {
        ensure!(!title.is_empty() || !body.is_empty(), "empty notification rejected");
        self.repository
            .queue(topic, message, source, template_version, title, body, result_topic)
            .await
    }

    pub async fn recover_sending(&self, result_topic: &str) -> Result<()> {
        self.repository.recover_sending(result_topic).await
    }

    pub async fn claim_pending(&self) -> Result<Option<PendingNotification>> {
        self.repository.claim_pending().await
    }

    pub async fn finish(
        &self,
        pending: &PendingNotification,
        state: &str,
        receipt: Option<&str>,
        result_topic: &str,
    ) -> Result<DurableMessage<Value>> {
        ensure!(
            matches!(state, "pending" | "delivered" | "rejected" | "unknown"),
            "invalid notification delivery state"
        );
        self.repository.finish(pending, state, receipt, result_topic).await
    }

    pub async fn list(state: &SigbotState, identity: &RequestIdentity, scope: &ScopeQuery) -> Result<Vec<Value>> {
        let tenant_id = scope.authorize(identity, "notification")?;
        let pool = state.postgres_pool().await?;
        NotifierPostgresRepository::list(&pool, tenant_id, &scope.env, identity.principal_id()).await
    }

    pub async fn mark_read(
        state: &SigbotState,
        identity: &RequestIdentity,
        scope: &ScopeQuery,
        run_id: &Uuid,
        seq: i64,
        producer_node_id: i64,
    ) -> Result<bool> {
        ensure!(
            seq >= 0 && run_id.get_version_num() == 7 && producer_node_id > 0,
            "invalid notification identity"
        );
        let tenant_id = scope.authorize(identity, "notification")?;
        let pool = state.postgres_pool().await?;
        NotifierPostgresRepository::mark_read(
            &pool,
            tenant_id,
            &scope.env,
            identity.principal_id(),
            run_id,
            seq,
            producer_node_id,
        )
        .await
    }
}
