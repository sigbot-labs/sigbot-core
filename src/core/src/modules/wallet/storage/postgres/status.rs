// SPDX-License-Identifier: GPL-3.0-only

use super::portfolio::WalletPortfolioPostgresRepository;
use super::reservation::WalletReservationPostgresRepository;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::wallet::storage::postgres::WalletPostgresRepository;
use anyhow::{ensure, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope, MessageKind};
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::order::execution::{OrderState, OrderStatusFact};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

pub struct WalletStatusPostgresRepository {
    wallet: WalletPostgresRepository,
}

impl WalletStatusPostgresRepository {
    pub fn new(wallet: WalletPostgresRepository) -> Self {
        Self { wallet }
    }

    /// Terminal venue status may precede its fills. Persist it and acknowledge
    /// intake without blocking the execution subscription that will finish it.
    pub async fn process_status(
        &self,
        message: &MessageEnvelope<OrderStatusFact>,
        traceparent: String,
    ) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        fact.validate()?;
        ensure!(
            input.scope == self.wallet.scope
                && input.account_id == Some(self.wallet.account_id)
                && fact.account_id == self.wallet.account_id
                && input.kind == MessageKind::Event
                && input.event_time == fact.occurred_at
                && input.available_time <= Utc::now(),
            "wallet status scope or time mismatch"
        );
        let mut tx = self.wallet.pool.begin().await?;
        let owned: Option<Uuid> = sqlx::query_scalar("SELECT id FROM ledger_info WHERE id=$1 AND account_id=$2 AND env=$3 AND tenant_id=$4 AND exchange=$5 FOR UPDATE")
            .bind(fact.wallet_id).bind(self.wallet.account_id).bind(&self.wallet.scope.env).bind(self.wallet.scope.tenant_id)
            .bind(&fact.exchange).fetch_optional(&mut *tx).await?;
        ensure!(owned.is_some(), "terminal status wallet is outside its account");
        let consumer = format!("{}.status", self.wallet.producer());
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        if !MessagerPostgresRepository::accept(
            &mut tx,
            &consumer,
            &AccountTopic::OrderEvent.topic(&self.wallet.scope, self.wallet.account_id)?,
            &wire,
        )
        .await?
        {
            tx.commit().await?;
            return Ok(false);
        }
        if let Some(changed) = Self::apply_status(&mut tx, input).await? {
            if changed {
                WalletPortfolioPostgresRepository::publish(
                    &self.wallet,
                    &mut tx,
                    &wire.durable,
                    fact.wallet_id,
                    traceparent,
                )
                .await?;
            }
            MessagerPostgresRepository::complete(&mut tx, &consumer, &wire.durable).await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    pub(crate) async fn release_pending_statuses(
        wallet: &WalletPostgresRepository,
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: Uuid,
    ) -> Result<()> {
        let consumer = format!("{}.status", wallet.producer());
        let pending: Vec<Value> = sqlx::query_scalar(
            "SELECT payload FROM msg_inbox WHERE consumer=$1 AND env=$2 AND tenant_id=$3
            AND state='pending' AND (payload->'payload'->>'wallet_id')::uuid=$4 ORDER BY received_at LIMIT 32",
        )
        .bind(&consumer)
        .bind(&wallet.scope.env)
        .bind(wallet.scope.tenant_id)
        .bind(wallet_id)
        .fetch_all(&mut **tx)
        .await?;
        for value in pending {
            let status: DurableMessage<OrderStatusFact> = serde_json::from_value(value)?;
            if Self::apply_status(tx, &status).await?.is_some() {
                let value = serde_json::to_value(&status.payload)?;
                MessagerPostgresRepository::complete(tx, &consumer, &status.map_payload(|_| value)).await?;
            }
        }
        Ok(())
    }

    async fn apply_status(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<OrderStatusFact>,
    ) -> Result<Option<bool>> {
        let fact = &input.payload;
        if !matches!(
            fact.receipt.state,
            OrderState::Filled | OrderState::Cancelled | OrderState::Rejected
        ) {
            return Ok(Some(false));
        }
        let intent: Value = sqlx::query_scalar(
            "SELECT intent FROM order_info WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4",
        )
        .bind(fact.wallet_id)
        .bind(fact.reservation_node_id)
        .bind(&input.run_id)
        .bind(fact.reservation_seq)
        .fetch_one(&mut **tx)
        .await?;
        let intent: sigbot_types::order::intent::TradeIntent = serde_json::from_value(intent)?;
        ensure!(
            fact.executed_quantity <= intent.quantity
                && (fact.receipt.state != OrderState::Filled || fact.executed_quantity == intent.quantity),
            "terminal status has inconsistent cumulative quantity"
        );
        let booked: Decimal = sqlx::query_scalar("SELECT COALESCE(SUM((payload->>'quantity')::numeric),0) FROM wlt_journal WHERE wallet_id=$1
            AND source='execution' AND run_id=$2 AND (payload->>'reservation_node_id')::bigint=$3 AND (payload->>'reservation_seq')::bigint=$4")
            .bind(fact.wallet_id).bind(&input.run_id).bind(fact.reservation_node_id).bind(fact.reservation_seq).fetch_one(&mut **tx).await?;
        ensure!(
            booked <= fact.executed_quantity,
            "terminal status omits executions; reconciliation required"
        );
        if booked < fact.executed_quantity {
            return Ok(None);
        }
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM wlt_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4)")
            .bind(fact.wallet_id).bind(fact.reservation_node_id).bind(&input.run_id).bind(fact.reservation_seq).fetch_one(&mut **tx).await?;
        if !exists {
            return Ok(Some(false));
        }
        Ok(Some(
            WalletReservationPostgresRepository::release_in(
                tx,
                fact.wallet_id,
                fact.reservation_node_id,
                &input.run_id,
                fact.reservation_seq,
            )
            .await?,
        ))
    }
}
