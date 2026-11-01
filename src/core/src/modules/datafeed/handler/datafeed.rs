// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::datafeed::storage::postgres::DatafeedPostgresRepository;
use crate::modules::messager::handler::messager::MessagerHandler;
use anyhow::Result;
use sigbot_types::datafeed::event::DatafeedEvent;
use sigbot_types::datafeed::event::DatafeedPublication;

/// Datafeed application service. It validates the component-facing boundary
/// and owns access to the persistence adapter.
#[derive(Clone)]
pub struct DatafeedHandler {
    repository: DatafeedPostgresRepository,
}

impl DatafeedHandler {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            repository: DatafeedPostgresRepository::connect(database_url).await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::from_repository(self.repository.message_store())
    }

    pub async fn ingest(&self, publication: &DatafeedPublication<'_>, event: DatafeedEvent) -> Result<DatafeedEvent> {
        self.repository.ingest(publication, event).await
    }
}
