// SPDX-License-Identifier: GPL-3.0-only
//! The wallet is the sole writer of financial facts and portfolio projections.

use self::portfolio::WalletPortfolioPostgresRepository;
use self::settlement::WalletSettlementPostgresRepository;
use self::status::WalletStatusPostgresRepository;
use crate::config::config::PostgresStorageProperties;
use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::storage::postgres::PostgresRepository;
use anyhow::{ensure, Result};
use chrono::Utc;
use serde_json::Value;
use sigbot_types::messager::envelope::{MessageEnvelope, MessageKind};
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::ExecutionFact;
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) mod funding;
pub(crate) mod portfolio;
pub(crate) mod request;
pub(crate) mod reservation;
pub(crate) mod settlement;
pub(crate) mod status;

#[derive(Clone)]
pub struct WalletPostgresRepository {
    pub(crate) pool: PgPool,
    pub(crate) scope: MessageScope,
    pub(crate) account_id: Uuid,
    pub(crate) node_id: i64,
}

impl WalletPostgresRepository {
    pub async fn connect(
        storage: &PostgresStorageProperties,
        scope: MessageScope,
        account_id: Uuid,
        node_id: i64,
    ) -> Result<Self> {
        let repository = PostgresRepository::<()>::get_or_init(storage).await?;
        Self::new(repository.get_pool().clone(), scope, account_id, node_id)
    }

    pub fn messager(&self) -> MessagerHandler {
        MessagerHandler::new(self.pool.clone())
    }

    pub fn producer(&self) -> String {
        format!(
            "wallet:{}:{}:{}:{}",
            self.scope.env, self.scope.tenant_id, self.account_id, self.node_id
        )
    }

    pub fn new(pool: PgPool, scope: MessageScope, account_id: Uuid, node_id: i64) -> Result<Self> {
        scope.validate()?;
        ensure!(
            account_id.get_version_num() == 7 && node_id > 0,
            "wallet account must be UUIDv7 and node positive"
        );
        Ok(Self {
            pool,
            scope,
            account_id,
            node_id,
        })
    }

    pub async fn process(
        &self,
        message: &MessageEnvelope<ExecutionFact>,
        producer_traceparent: String,
    ) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        ensure!(
            input.available_time <= Utc::now(),
            "execution fact is not yet available"
        );
        ensure!(
            input.scope == self.scope
                && input.account_id == Some(self.account_id)
                && fact.account_id == self.account_id,
            "wallet execution isolation scope mismatch"
        );
        ensure!(
            input.kind == MessageKind::Event && fact.executed_at == input.event_time,
            "execution must be a fact with its actual event time"
        );
        let topic = AccountTopic::ExecutionEvent.topic(&self.scope, self.account_id)?;
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        let mut transaction = self.pool.begin().await?;
        let wallet = sqlx::query(
            "SELECT portfolio_version FROM ledger_info WHERE id=$1 AND tenant_id=$2
            AND env=$3 AND exchange=$4 AND account_id=$5 FOR UPDATE",
        )
        .bind(fact.wallet_id)
        .bind(self.scope.tenant_id)
        .bind(&self.scope.env)
        .bind(&fact.exchange)
        .bind(self.account_id)
        .fetch_optional(&mut *transaction)
        .await?;
        ensure!(wallet.is_some(), "execution wallet does not belong to this account");
        let consumer = format!("wallet.execution.{}", self.node_id);
        if !MessagerPostgresRepository::accept(&mut transaction, &consumer, &topic, &wire).await? {
            transaction.commit().await?;
            return Ok(false);
        }
        fact.validate()?;
        let payload = serde_json::to_value(fact)?;
        let inserted = sqlx::query(
            "INSERT INTO wlt_journal(wallet_id,run_id,seq,source,fact_ref,occurred_at,payload,producer_node_id)
            VALUES($1,$2,$3,'execution',$4,$5,$6,$7) ON CONFLICT DO NOTHING",
        )
        .bind(fact.wallet_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .bind(&fact.exchange_trade_id)
        .bind(fact.executed_at)
        .bind(&payload)
        .bind(input.node_id)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if inserted == 0 {
            let prior = sqlx::query_scalar::<_, Value>(
                "SELECT payload FROM wlt_journal WHERE wallet_id=$1
                AND source='execution' AND fact_ref=$2",
            )
            .bind(fact.wallet_id)
            .bind(&fact.exchange_trade_id)
            .fetch_optional(&mut *transaction)
            .await?;
            ensure!(
                prior == Some(payload),
                "execution identity was reused with different facts"
            );
            MessagerPostgresRepository::complete(&mut transaction, &consumer, &wire.durable).await?;
            transaction.commit().await?;
            return Ok(false);
        }
        let projection = sigbot_types::wallet::projection::TradeProjection::from_execution(fact)?;
        WalletSettlementPostgresRepository::settle(&mut transaction, input, &projection).await?;
        WalletSettlementPostgresRepository::post(&mut transaction, input, &projection).await?;
        WalletSettlementPostgresRepository::release_completed(&mut transaction, input).await?;
        WalletStatusPostgresRepository::release_pending_statuses(self, &mut transaction, fact.wallet_id).await?;
        WalletSettlementPostgresRepository::rebuild_position(&mut transaction, fact).await?;
        WalletPortfolioPostgresRepository::publish(
            self,
            &mut transaction,
            &wire.durable,
            fact.wallet_id,
            producer_traceparent,
        )
        .await?;
        MessagerPostgresRepository::complete(&mut transaction, &consumer, &wire.durable).await?;
        transaction.commit().await?;
        Ok(true)
    }
}
