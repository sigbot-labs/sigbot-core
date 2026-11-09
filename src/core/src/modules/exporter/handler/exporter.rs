use crate::modules::exporter::storage::postgres::ExporterPostgresRepository;
use crate::modules::messager::handler::messager::MessagerHandler;
use anyhow::Result;
use serde_json::Value;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::topic::MessageScope;
use uuid::Uuid;

pub use crate::modules::exporter::storage::postgres::ExportClaim;

pub struct ExporterHandler {
    repository: ExporterPostgresRepository,
}

impl ExporterHandler {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            repository: ExporterPostgresRepository::connect(database_url).await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::from_repository(self.repository.message_store())
    }

    pub async fn connection_exists(&self, connection_id: Uuid, scope: &MessageScope) -> Result<bool> {
        self.repository.connection_exists(connection_id, scope).await
    }

    pub async fn quarantine(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
        reason: &str,
    ) -> Result<()> {
        self.repository.quarantine(consumer, topic, message, reason).await
    }

    pub async fn claim(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<Option<ExportClaim<'_>>> {
        self.repository.claim(consumer, topic, message).await
    }
}
