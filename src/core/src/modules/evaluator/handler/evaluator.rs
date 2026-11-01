use crate::modules::evaluator::storage::postgres::EvaluatorPostgresRepository;
use crate::modules::messager::handler::messager::MessagerHandler;
use anyhow::Result;
use serde_json::Value;
use sigbot_types::messager::envelope::MessageEnvelope;

pub use crate::modules::evaluator::storage::postgres::EvaluationClaim;
pub use crate::modules::evaluator::storage::postgres::EvaluationEvidence;

pub struct EvaluatorHandler {
    repository: EvaluatorPostgresRepository,
}

impl EvaluatorHandler {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            repository: EvaluatorPostgresRepository::connect(database_url).await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::from_repository(self.repository.message_store())
    }

    pub async fn claim(
        &self,
        consumer: &str,
        topic: &str,
        message: &MessageEnvelope<Value>,
    ) -> Result<Option<EvaluationClaim<'_>>> {
        self.repository.claim(consumer, topic, message).await
    }
}
