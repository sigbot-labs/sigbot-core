// SPDX-License-Identifier: GPL-3.0-only

use crate::config::config::PostgresStorageProperties;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::wallet::storage::postgres::WalletPostgresRepository;
use crate::storage::postgres::PostgresRepository;
use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sigbot_types::messager::envelope::{DurableMessage, MessageKind};
use sigbot_types::messager::topic::{AccountTopic, MessageScope};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

pub struct WalletPortfolioPostgresRepository {
    pool: sqlx::PgPool,
}

impl WalletPortfolioPostgresRepository {
    pub async fn connect(storage: &PostgresStorageProperties) -> Result<Self> {
        let repository = PostgresRepository::<()>::get_or_init(storage).await?;
        Ok(Self {
            pool: repository.get_pool().clone(),
        })
    }

    pub async fn snapshot(&self, scope: &MessageScope, account_id: Uuid) -> Result<Value> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let wallet =
            sqlx::query("SELECT id,portfolio_version FROM ledger_info WHERE env=$1 AND tenant_id=$2 AND account_id=$3")
                .bind(&scope.env)
                .bind(scope.tenant_id)
                .bind(account_id)
                .fetch_one(&mut *transaction)
                .await
                .context("strategy account wallet missing")?;
        let wallet_id: Uuid = wallet.get("id");
        let balances = sqlx::query("SELECT asset,available,locked FROM wlt_balance WHERE wallet_id=$1 ORDER BY asset")
            .bind(wallet_id)
            .fetch_all(&mut *transaction)
            .await?
            .into_iter()
            .map(|row| {
                json!({
                    "asset":row.get::<String,_>("asset"),"available":row.get::<Decimal,_>("available"),
                    "locked":row.get::<Decimal,_>("locked")
                })
            })
            .collect::<Vec<_>>();
        let positions = sqlx::query(
            "SELECT symbol,side,size,entry_price,realized_pnl FROM wlt_position WHERE wallet_id=$1 ORDER BY symbol,side",
        )
        .bind(wallet_id)
        .fetch_all(&mut *transaction)
        .await?
        .into_iter()
        .map(|row| {
            json!({
                "symbol":row.get::<String,_>("symbol"),"side":row.get::<String,_>("side"),
                "size":row.get::<Decimal,_>("size"),"entry_price":row.get::<Decimal,_>("entry_price"),
                "realized_pnl":row.get::<Decimal,_>("realized_pnl")
            })
        })
        .collect::<Vec<_>>();
        transaction.commit().await?;
        Ok(json!({
            "account_id":account_id,"wallet_id":wallet_id,"version":wallet.get::<i64,_>("portfolio_version"),
            "balances":balances,"positions":positions
        }))
    }

    pub(crate) async fn publish(
        wallet: &WalletPostgresRepository,
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<Value>,
        wallet_id: Uuid,
        producer_traceparent: String,
    ) -> Result<()> {
        let version: i64 = sqlx::query_scalar(
            "UPDATE ledger_info SET portfolio_version=portfolio_version+1 WHERE id=$1 RETURNING portfolio_version",
        )
        .bind(wallet_id)
        .fetch_one(&mut **tx)
        .await?;
        let balances = sqlx::query("SELECT asset,available,locked FROM wlt_balance WHERE wallet_id=$1 ORDER BY asset")
            .bind(wallet_id)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .map(|row| {
                json!({
                "asset":row.get::<String,_>("asset"),"available":row.get::<Decimal,_>("available"),
                "locked":row.get::<Decimal,_>("locked") })
            })
            .collect::<Vec<_>>();
        let positions = sqlx::query(
            "SELECT symbol,side,size,entry_price,realized_pnl FROM wlt_position WHERE wallet_id=$1 ORDER BY symbol,side",
        )
        .bind(wallet_id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|row| {
            json!({
                "symbol":row.get::<String,_>("symbol"),"side":row.get::<String,_>("side"),
                "size":row.get::<Decimal,_>("size"),"entry_price":row.get::<Decimal,_>("entry_price"),
                "realized_pnl":row.get::<Decimal,_>("realized_pnl") })
        })
        .collect::<Vec<_>>();
        let seq = MessagerPostgresRepository::next_sequence(tx, &wallet.scope, wallet.node_id, &input.run_id).await?;
        let portfolio = DurableMessage {
            version: 2,
            scope: wallet.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: Some(wallet.account_id),
            node_id: wallet.node_id,
            target_node_id: None,
            run_id: input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: input.event_time,
            available_time: input.available_time,
            produced_at: Utc::now(),
            traceparent: producer_traceparent,
            payload: json!({"version":version,"wallet_id":wallet_id,"balances":balances,"positions":positions}),
        };
        let output_topic = AccountTopic::PortfolioSnapshot.topic(&wallet.scope, wallet.account_id)?;
        MessagerPostgresRepository::enqueue(tx, &wallet.producer(), &output_topic, &portfolio).await?;
        Ok(())
    }
}
