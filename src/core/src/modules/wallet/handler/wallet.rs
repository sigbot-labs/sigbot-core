// SPDX-License-Identifier: GPL-3.0-only

use crate::config::config::PostgresStorageProperties;
use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::wallet::storage::postgres::funding::WalletFundingPostgresRepository;
use crate::modules::wallet::storage::postgres::portfolio::WalletPortfolioPostgresRepository;
use crate::modules::wallet::storage::postgres::request::WalletRequestPostgresRepository;
use crate::modules::wallet::storage::postgres::status::WalletStatusPostgresRepository;
use crate::modules::wallet::storage::postgres::WalletPostgresRepository;
use anyhow::Result;
use serde_json::Value;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::{ExecutionFact, OrderStatusFact};
use sigbot_types::wallet::funding::FundingFact;
use sigbot_types::wallet::reservation::ReservationRequest;
use uuid::Uuid;

pub struct WalletHandler {
    repository: WalletPostgresRepository,
    funding: WalletFundingPostgresRepository,
    status: WalletStatusPostgresRepository,
}

pub struct WalletReservationHandler {
    repository: WalletRequestPostgresRepository,
}

pub struct WalletPortfolioHandler {
    repository: WalletPortfolioPostgresRepository,
}

impl WalletHandler {
    pub async fn connect(
        storage: &PostgresStorageProperties,
        scope: MessageScope,
        account_id: Uuid,
        node_id: i64,
    ) -> Result<Self> {
        let repository = WalletPostgresRepository::connect(storage, scope, account_id, node_id).await?;
        Ok(Self {
            funding: WalletFundingPostgresRepository::new(repository.clone()),
            status: WalletStatusPostgresRepository::new(repository.clone()),
            repository,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        self.repository.messager()
    }

    pub fn producer(&self) -> String {
        self.repository.producer()
    }

    pub async fn process(&self, message: &MessageEnvelope<ExecutionFact>, traceparent: String) -> Result<bool> {
        self.repository.process(message, traceparent).await
    }

    pub async fn process_funding(&self, message: &MessageEnvelope<FundingFact>, traceparent: String) -> Result<bool> {
        self.funding.process_funding(message, traceparent).await
    }

    pub async fn rebuild_balances(&self, wallet_id: Uuid) -> Result<()> {
        self.funding.rebuild_balances(wallet_id).await
    }

    pub async fn process_status(
        &self,
        message: &MessageEnvelope<OrderStatusFact>,
        traceparent: String,
    ) -> Result<bool> {
        self.status.process_status(message, traceparent).await
    }
}

impl WalletReservationHandler {
    pub async fn connect(
        storage: &PostgresStorageProperties,
        scope: MessageScope,
        account_id: Uuid,
        node_id: i64,
        order_node_ids: Vec<i64>,
    ) -> Result<Self> {
        Ok(Self {
            repository: WalletRequestPostgresRepository::connect(storage, scope, account_id, node_id, order_node_ids)
                .await?,
        })
    }

    pub async fn process(&self, message: &MessageEnvelope<ReservationRequest>) -> Result<ReservationProcessing> {
        self.repository.process(message).await
    }
}

impl WalletPortfolioHandler {
    pub async fn connect(storage: &PostgresStorageProperties) -> Result<Self> {
        Ok(Self {
            repository: WalletPortfolioPostgresRepository::connect(storage).await?,
        })
    }

    pub async fn snapshot(&self, scope: &MessageScope, account_id: Uuid) -> Result<Value> {
        self.repository.snapshot(scope, account_id).await
    }
}

pub use crate::modules::wallet::storage::postgres::request::ReservationProcessing;
