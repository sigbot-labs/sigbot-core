// SPDX-License-Identifier: GPL-3.0-only
//! Distributed reservations have one financial writer: the wallet consumer.
use super::execution_postgres::WalletExecutionProcessor;
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_core::modules::wallet::store::transaction::reservation_postgres::PostgresWalletReservation;
use sigbot_messager::{client::reply_pairing::ReplyPairingCache, storage::postgres::PostgresMessageStore};
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{AccountTopic, MessageScope},
    },
    wallet::reservation::{ReservationAction, ReservationRequest, ReservationResult, ReservationState},
};
use sqlx::{Acquire, PgPool, Postgres, Row, Transaction};

pub struct WalletReservationProcessor {
    pool: PgPool,
    scope: MessageScope,
    account_id: i64,
    node_id: i64,
    order_node_ids: Vec<i64>,
    portfolio: WalletExecutionProcessor,
}

impl WalletReservationProcessor {
    pub fn new(
        pool: PgPool,
        scope: MessageScope,
        account_id: i64,
        node_id: i64,
        order_node_ids: Vec<i64>,
    ) -> Result<Self> {
        ensure!(
            !order_node_ids.is_empty() && order_node_ids.iter().all(|id| *id > 0 && *id != node_id),
            "wallet order source nodes required"
        );
        let portfolio = WalletExecutionProcessor::new(pool.clone(), scope.clone(), account_id, node_id)?;
        Ok(Self {
            pool,
            scope,
            account_id,
            node_id,
            order_node_ids,
            portfolio,
        })
    }

    pub async fn process(&self, message: &MessageEnvelope<ReservationRequest>) -> Result<bool> {
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
        let wallet = sqlx::query("SELECT id FROM s_wallets WHERE id=$1 AND account_id=$2 AND env=$3 AND tenant_id=$4 AND space_id=$5 AND del_flag=FALSE FOR UPDATE")
            .bind(request.wallet_id).bind(self.account_id).bind(&self.scope.env).bind(self.scope.tenant_id.to_string()).bind(self.scope.space_id)
            .fetch_optional(&mut *tx).await?;
        ensure!(wallet.is_some(), "reservation wallet is outside its account");
        let consumer = format!("{}.reservation", self.portfolio.producer());
        let topic = AccountTopic::WalletReservationRequest.topic(&self.scope, self.account_id)?;
        if !PostgresMessageStore::accept(&mut tx, &consumer, &topic, &wire).await? {
            tx.commit().await?;
            return Ok(false);
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
            self.portfolio
                .publish_portfolio(&mut tx, &wire.durable, request.wallet_id, trace.traceparent())
                .await?;
        }
        let seq = PostgresMessageStore::next_sequence(&mut tx, &self.scope, self.node_id, &input.run_id).await?;
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
        PostgresMessageStore::enqueue(
            &mut tx,
            &self.portfolio.producer(),
            &AccountTopic::WalletReservationResult.topic(&self.scope, self.account_id)?,
            &output,
        )
        .await?;
        PostgresMessageStore::complete(&mut tx, &consumer, &wire.durable).await?;
        if let Some(pair) = message.request_span_id.as_deref() {
            ReplyPairingCache::shared().remember(&output, pair)?;
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn apply(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<ReservationRequest>,
    ) -> Result<ReservationState> {
        let request = &input.payload;
        let order = sqlx::query("SELECT state,reserve_asset,reserve_amount,intent,exchange_order_id FROM order_record
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND wallet_id=$4 AND producer_node_id=$5 AND run_id=$6 AND seq=$7")
            .bind(&self.scope.env).bind(self.scope.tenant_id.to_string()).bind(self.scope.space_id).bind(request.wallet_id)
            .bind(request.reservation_node_id).bind(&input.run_id).bind(request.reservation_seq).fetch_optional(&mut **tx).await?
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
                PostgresWalletReservation::reserve_in(
                    tx,
                    request.wallet_id,
                    &self.scope.tenant_id.to_string(),
                    &self.scope.env,
                    self.scope.space_id,
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
                let state: String = order.get("state");
                let booked: Decimal = sqlx::query_scalar("SELECT COALESCE(SUM((payload->>'quantity')::numeric),0) FROM wallet_journal
                    WHERE wallet_id=$1 AND source='execution' AND run_id=$2 AND (payload->>'reservation_node_id')::bigint=$3 AND (payload->>'reservation_seq')::bigint=$4")
                    .bind(request.wallet_id).bind(&input.run_id).bind(request.reservation_node_id).bind(request.reservation_seq).fetch_one(&mut **tx).await?;
                let mut safe = if state == "rejected" {
                    booked.is_zero() && order.get::<Option<String>, _>("exchange_order_id").is_none()
                } else if state == "filled" {
                    booked
                        == serde_json::from_value::<sigbot_types::modules::order::intent::TradeIntent>(
                            order.get("intent"),
                        )?
                        .quantity
                } else {
                    false
                };
                if !safe && matches!(state.as_str(), "cancelled" | "rejected") {
                    // The tracked terminal fact is authoritative for fills that
                    // may still be in transit; current execution row counts alone are not.
                    let terminal: Option<Value> = sqlx::query_scalar("SELECT payload->'payload' FROM messager_outbox WHERE env=$1 AND tenant_id=$2 AND space_id=$3
                        AND node_id=$4 AND run_id=$5 AND (payload->'payload'->>'wallet_id')::bigint=$6
                        AND (payload->'payload'->>'reservation_node_id')::bigint=$7 AND (payload->'payload'->>'reservation_seq')::bigint=$8
                        AND payload->'payload'->'receipt'->>'state' IN ('cancelled','rejected') ORDER BY seq DESC LIMIT 1")
                        .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.scope.space_id).bind(input.node_id).bind(&input.run_id)
                        .bind(request.wallet_id).bind(request.reservation_node_id).bind(request.reservation_seq).fetch_optional(&mut **tx).await?;
                    if let Some(value) = terminal {
                        let fact: sigbot_types::modules::order::execution::OrderStatusFact =
                            serde_json::from_value(value)?;
                        safe = fact.account_id == self.account_id && fact.executed_quantity == booked;
                    }
                }
                ensure!(safe, "release requires fully booked executions or a rejection before venue submission; cancellation requires reconciliation");
                let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM wallet_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4)")
                    .bind(request.wallet_id).bind(request.reservation_node_id).bind(&input.run_id).bind(request.reservation_seq).fetch_one(&mut **tx).await?;
                if !exists {
                    return Ok(ReservationState::Released);
                }
                PostgresWalletReservation::release_in(
                    tx,
                    request.wallet_id,
                    request.reservation_node_id,
                    &input.run_id,
                    request.reservation_seq,
                )
                .await?;
                Ok(ReservationState::Released)
            }
        }
    }
}
