// SPDX-License-Identifier: GPL-3.0-only

use super::portfolio::WalletPortfolioPostgresRepository;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::wallet::storage::postgres::WalletPostgresRepository;
use anyhow::{ensure, Result};
use chrono::Utc;
use serde_json::Value;
use sigbot_types::messager::envelope::{MessageEnvelope, MessageKind};
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::wallet::funding::FundingFact;
use uuid::Uuid;

pub struct WalletFundingPostgresRepository {
    wallet: WalletPostgresRepository,
}

impl WalletFundingPostgresRepository {
    pub fn new(wallet: WalletPostgresRepository) -> Self {
        Self { wallet }
    }

    pub async fn process_funding(&self, message: &MessageEnvelope<FundingFact>, traceparent: String) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        fact.validate()?;
        ensure!(input.available_time <= Utc::now(), "funding fact is not yet available");
        ensure!(
            input.scope == self.wallet.scope
                && input.account_id == Some(self.wallet.account_id)
                && fact.account_id == self.wallet.account_id,
            "funding isolation scope mismatch"
        );
        ensure!(
            input.kind == MessageKind::Event && input.event_time == fact.occurred_at,
            "funding requires an observed fact"
        );
        let mut tx = self.wallet.pool.begin().await?;
        let wallet = sqlx::query(
            "SELECT id FROM ledger_info WHERE id=$1 AND tenant_id=$2 AND env=$3
            AND exchange=$4 AND account_id=$5 FOR UPDATE",
        )
        .bind(fact.wallet_id)
        .bind(self.wallet.scope.tenant_id)
        .bind(&self.wallet.scope.env)
        .bind(&fact.exchange)
        .bind(self.wallet.account_id)
        .fetch_optional(&mut *tx)
        .await?;
        ensure!(wallet.is_some(), "funding wallet is outside this account");
        let topic = AccountTopic::ExecutionEvent.topic(&self.wallet.scope, self.wallet.account_id)?;
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        let consumer = format!("wallet.funding.{}", self.wallet.node_id);
        if !MessagerPostgresRepository::accept(&mut tx, &consumer, &topic, &wire).await? {
            tx.commit().await?;
            return Ok(false);
        }
        let source = format!("funding/{}", fact.source);
        let payload = serde_json::to_value(fact)?;
        let inserted = sqlx::query(
            "INSERT INTO wlt_journal(wallet_id,run_id,seq,source,fact_ref,occurred_at,payload,producer_node_id)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING",
        )
        .bind(fact.wallet_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .bind(&source)
        .bind(&fact.source_event_id)
        .bind(fact.occurred_at)
        .bind(&payload)
        .bind(input.node_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted == 1 {
            for (line, account, amount) in [(0, "available", fact.amount), (1, "clearing", -fact.amount)] {
                sqlx::query("INSERT INTO wlt_posting(wallet_id,run_id,seq,line_no,account,asset,amount,producer_node_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(fact.wallet_id).bind(&input.run_id).bind(input.seq).bind(line).bind(account).bind(&fact.asset).bind(amount).bind(input.node_id)
                    .execute(&mut *tx).await?;
            }
            sqlx::query("INSERT INTO wlt_balance(wallet_id,asset,available,locked) VALUES($1,$2,$3,0)
                ON CONFLICT(wallet_id,asset) DO UPDATE SET available=wlt_balance.available+EXCLUDED.available,updated_at=NOW()")
                .bind(fact.wallet_id).bind(&fact.asset).bind(fact.amount).execute(&mut *tx).await?;
            WalletPortfolioPostgresRepository::publish(
                &self.wallet,
                &mut tx,
                &wire.durable,
                fact.wallet_id,
                traceparent,
            )
            .await?;
        } else {
            let previous: Option<Value> =
                sqlx::query_scalar("SELECT payload FROM wlt_journal WHERE wallet_id=$1 AND source=$2 AND fact_ref=$3")
                    .bind(fact.wallet_id)
                    .bind(source)
                    .bind(&fact.source_event_id)
                    .fetch_optional(&mut *tx)
                    .await?;
            ensure!(
                previous == Some(payload),
                "funding identity reused with different facts"
            );
        }
        MessagerPostgresRepository::complete(&mut tx, &consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(inserted == 1)
    }

    /// Opening balances must first be represented by an immutable funding journal.
    /// Reservations do not change asset ownership: available = total - remaining locks.
    pub async fn rebuild_balances(&self, wallet_id: Uuid) -> Result<()> {
        let mut tx = self.wallet.pool.begin().await?;
        let exists = sqlx::query(
            "SELECT id FROM ledger_info WHERE id=$1 AND env=$2 AND tenant_id=$3 AND account_id=$4 FOR UPDATE",
        )
        .bind(wallet_id)
        .bind(&self.wallet.scope.env)
        .bind(self.wallet.scope.tenant_id)
        .bind(self.wallet.account_id)
        .fetch_optional(&mut *tx)
        .await?;
        ensure!(exists.is_some(), "wallet cannot be rebuilt outside its account");
        let journaled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM wlt_journal WHERE wallet_id=$1)")
            .bind(wallet_id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(journaled, "rebuild requires journaled opening balances");
        sqlx::query("DELETE FROM wlt_balance WHERE wallet_id=$1")
            .bind(wallet_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO wlt_balance(wallet_id,asset,available,locked)
            WITH totals AS (SELECT asset,SUM(amount) amount FROM wlt_posting WHERE wallet_id=$1 AND account='available' GROUP BY asset),
            reserves AS (SELECT asset,SUM(remaining) amount FROM wlt_reservation WHERE wallet_id=$1 AND state='reserved' GROUP BY asset)
            SELECT $1,COALESCE(t.asset,r.asset),COALESCE(t.amount,0)-COALESCE(r.amount,0),COALESCE(r.amount,0)
            FROM totals t FULL OUTER JOIN reserves r ON t.asset=r.asset")
            .bind(wallet_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
