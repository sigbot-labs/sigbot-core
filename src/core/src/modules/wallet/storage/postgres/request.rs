// SPDX-License-Identifier: GPL-3.0-only
//! Distributed reservations have one financial writer: the wallet consumer.
use super::portfolio::WalletPortfolioPostgresRepository;
use super::reservation::WalletReservationPostgresRepository;
use crate::config::config::PostgresStorageProperties;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::wallet::storage::postgres::WalletPostgresRepository;
use crate::storage::postgres::PostgresRepository;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::wallet::reservation::ReservationAction;
use sigbot_types::wallet::reservation::ReservationRequest;
use sigbot_types::wallet::reservation::ReservationResult;
use sigbot_types::wallet::reservation::ReservationState;
use sqlx::Acquire;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

pub struct ReservationProcessing {
    pub processed: bool,
    pub response: Option<DurableMessage<Value>>,
}

pub struct WalletRequestPostgresRepository {
    pool: PgPool,
    scope: MessageScope,
    account_id: Uuid,
    node_id: i64,
    order_node_ids: Vec<i64>,
    wallet: WalletPostgresRepository,
}

impl WalletRequestPostgresRepository {
    pub async fn connect(
        storage: &PostgresStorageProperties,
        scope: MessageScope,
        account_id: Uuid,
        node_id: i64,
        order_node_ids: Vec<i64>,
    ) -> Result<Self> {
        let repository = PostgresRepository::<()>::get_or_init(storage).await?;
        Self::new(
            repository.get_pool().clone(),
            scope,
            account_id,
            node_id,
            order_node_ids,
        )
    }

    pub fn new(
        pool: PgPool,
        scope: MessageScope,
        account_id: Uuid,
        node_id: i64,
        order_node_ids: Vec<i64>,
    ) -> Result<Self> {
        ensure!(
            !order_node_ids.is_empty() && order_node_ids.iter().all(|id| *id > 0 && *id != node_id),
            "wallet order source nodes required"
        );
        let wallet = WalletPostgresRepository::new(pool.clone(), scope.clone(), account_id, node_id)?;
        Ok(Self {
            pool,
            scope,
            account_id,
            node_id,
            order_node_ids,
            wallet,
        })
    }

    pub async fn process(&self, message: &MessageEnvelope<ReservationRequest>) -> Result<ReservationProcessing> {
        message.validate()?;
        let input = &message.durable;
        let request = &input.payload;
        request.validate()?;
        ensure!(
            input.scope == self.scope
                && input.account_id == Some(self.account_id)
                && request.account_id == self.account_id
                && input.kind == MessageKind::Request
                && self.order_node_ids.contains(&input.node_id)
                && input.target_node_id.is_none_or(|node| node == self.node_id)
                && input.available_time <= Utc::now(),
            "untrusted reservation request"
        );
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        let mut tx = self.pool.begin().await?;
        let wallet = sqlx::query("SELECT id FROM ledger_info WHERE id=$1 AND account_id=$2 AND env=$3 AND tenant_id=$4 AND del_flag=FALSE FOR UPDATE")
            .bind(request.wallet_id).bind(self.account_id).bind(&self.scope.env).bind(self.scope.tenant_id)
            .fetch_optional(&mut *tx).await?;
        ensure!(wallet.is_some(), "reservation wallet is outside its account");
        let consumer = format!("{}.reservation", self.wallet.producer());
        let topic = AccountTopic::WalletReservationRequest.topic(&self.scope, self.account_id)?;
        if !MessagerPostgresRepository::accept(&mut tx, &consumer, &topic, &wire).await? {
            tx.commit().await?;
            return Ok(ReservationProcessing {
                processed: false,
                response: None,
            });
        }
        // A rejected reservation still has a committed Inbox and Result. Database
        // faults roll back the whole intake so redelivery can retry safely.
        let mut operation = tx.begin().await?;
        let (state, reason) = match self.apply(&mut operation, input).await {
            Ok(state) => {
                operation.commit().await?;
                (state, None)
            }
            Err(error) => {
                operation.rollback().await?;
                if error.downcast_ref::<sqlx::Error>().is_some() {
                    return Err(error);
                }
                (ReservationState::Rejected, Some(error.to_string()))
            }
        };
        let trace = MessageTrace::child("wallet.reservation.result", &input.traceparent);
        if state != ReservationState::Rejected {
            WalletPortfolioPostgresRepository::publish(
                &self.wallet,
                &mut tx,
                &wire.durable,
                request.wallet_id,
                trace.traceparent(),
            )
            .await?;
        }
        let seq = MessagerPostgresRepository::next_sequence(&mut tx, &self.scope, self.node_id, &input.run_id).await?;
        let output = DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: Some(self.account_id),
            node_id: self.node_id,
            target_node_id: Some(input.node_id),
            run_id: input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Result,
            event_time: input.event_time,
            available_time: input.available_time,
            produced_at: Utc::now(),
            traceparent: trace.traceparent(),
            payload: serde_json::to_value(ReservationResult {
                request: request.clone(),
                state,
                reason,
            })?,
        };
        MessagerPostgresRepository::enqueue(
            &mut tx,
            &self.wallet.producer(),
            &AccountTopic::WalletReservationResult.topic(&self.scope, self.account_id)?,
            &output,
        )
        .await?;
        MessagerPostgresRepository::complete(&mut tx, &consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(ReservationProcessing {
            processed: true,
            response: Some(output),
        })
    }

    async fn apply(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<ReservationRequest>,
    ) -> Result<ReservationState> {
        let request = &input.payload;
        let order = sqlx::query(
            "SELECT state,reserve_asset,reserve_amount,intent,exchange_order_id FROM order_info
            WHERE env=$1 AND tenant_id=$2 AND wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(request.wallet_id)
        .bind(request.reservation_node_id)
        .bind(&input.run_id)
        .bind(request.reservation_seq)
        .fetch_optional(&mut **tx)
        .await?
        .context("reservation requires a persisted risk-accepted intent")?;
        match &request.action {
            ReservationAction::Reserve { asset, amount } => {
                ensure!(
                    order.get::<String, _>("reserve_asset") == *asset
                        && order.get::<Decimal, _>("reserve_amount") == *amount,
                    "reservation differs from accepted risk terms"
                );
                ensure!(
                    matches!(order.get::<String, _>("state").as_str(), "pending" | "reserved"),
                    "order cannot acquire a new reservation in its current state"
                );
                WalletReservationPostgresRepository::reserve_in(
                    tx,
                    request.wallet_id,
                    self.scope.tenant_id,
                    &self.scope.env,
                    request.reservation_node_id,
                    &input.run_id,
                    request.reservation_seq,
                    asset,
                    *amount,
                )
                .await?;
                Ok(ReservationState::Reserved)
            }
            ReservationAction::Release => {
                WalletReservationPostgresRepository::release_order_in(tx, self.account_id, input, &order).await
            }
        }
    }
}
