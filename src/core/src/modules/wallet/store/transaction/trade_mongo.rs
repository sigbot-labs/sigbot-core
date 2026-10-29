// SPDX-License-Identifier: GPL-3.0-only
//! Legacy Mongo read repositories remain available for migration. Financial writes
//! require the PostgreSQL wallet transaction and its immutable journal constraints.

use super::IWalletUpdater;
use crate::config::config::MongoAppDBProperties;
use anyhow::{bail, Error};
use async_trait::async_trait;
use sigbot_types::modules::order::events::SigbotTradeEvent;

pub struct MongoWalletUpdater;

impl MongoWalletUpdater {
    pub async fn new(_config: &MongoAppDBProperties) -> Result<Self, Error> {
        bail!("wallet financial writes require PostgreSQL; migrate legacy Mongo wallet facts before enabling writes")
    }
}

#[async_trait]
impl IWalletUpdater for MongoWalletUpdater {
    async fn upsert(&self, _event: &SigbotTradeEvent) -> Result<(), Error> {
        bail!("Mongo wallet writes are disabled: no atomic immutable-journal contract")
    }
}
