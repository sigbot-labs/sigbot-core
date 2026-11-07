// SPDX-License-Identifier: GPL-3.0-only
use super::{OrderExposure, OrderReservationPostgresRepository, OrderRiskPostgresRepository};
use crate::modules::order::storage::postgres::OrderPostgresRepository;
use anyhow::ensure;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::wallet::reservation::ReservationAction;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;

#[derive(Clone)]
pub struct OrderRecordPostgresRepository {
    pool: PgPool,
    risk: OrderRiskPostgresRepository,
}

impl OrderRecordPostgresRepository {
    pub fn new(storage: &OrderPostgresRepository) -> Self {
        Self {
            pool: storage.pool.clone(),
            risk: OrderRiskPostgresRepository::new(storage),
        }
    }
    pub async fn accept_with<F>(&self, message: &DurableMessage<TradeIntent>, decide: F) -> Result<Option<String>>
    where
        F: FnOnce(&OrderExposure) -> Result<(String, Decimal)>,
    {
        let mut transaction = self.pool.begin().await?;
        let exposure = self.risk.exposure(&mut transaction, message).await?;
        let existing = self.risk.find_in(&mut transaction, message).await?;
        if existing.is_none() {
            let reservation = decide(&exposure)?;
            Self::insert(&mut transaction, message, &reservation.0, reservation.1).await?;
        }
        transaction.commit().await?;
        Ok(existing)
    }

    pub async fn persist_transition(
        &self,
        reservation: &OrderReservationPostgresRepository,
        producer: &str,
        node_id: i64,
        input: &DurableMessage<TradeIntent>,
        state: OrderState,
        exchange_order_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let mut transaction = self.pool.begin().await?;
        if !Self::record(&mut transaction, input, state.as_str(), exchange_order_id).await? {
            transaction.commit().await?;
            return Ok(false);
        }
        OrderPostgresRepository::enqueue(
            &mut transaction,
            producer,
            node_id,
            input,
            state,
            exchange_order_id,
            now,
        )
        .await?;
        if matches!(state, OrderState::Rejected | OrderState::Cancelled) {
            reservation
                .request_in(&mut transaction, input, ReservationAction::Release)
                .await?;
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn insert(
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
        asset: &str,
        amount: Decimal,
    ) -> Result<bool> {
        Ok(sqlx::query("INSERT INTO order_info(env,tenant_id,wallet_id,run_id,seq,exchange,symbol,intent,reserve_asset,reserve_amount,producer_node_id,state)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending') ON CONFLICT DO NOTHING")
            .bind(&message.scope.env).bind(message.scope.tenant_id)
            .bind(message.payload.wallet_id).bind(&message.run_id).bind(message.seq).bind(&message.payload.exchange)
            .bind(&message.payload.symbol).bind(serde_json::to_value(&message.payload)?).bind(asset).bind(amount).bind(message.node_id)
            .execute(&mut **tx).await?.rows_affected() == 1)
    }

    pub async fn claim_submission(&self, message: &DurableMessage<TradeIntent>) -> Result<bool> {
        Ok(sqlx::query("UPDATE order_info SET state='submitting',updated_at=NOW() WHERE env=$1 AND tenant_id=$2
            AND wallet_id=$3 AND run_id=$4 AND seq=$5 AND producer_node_id=$6 AND state='reserved'
            AND EXISTS(SELECT 1 FROM wlt_reservation r WHERE r.wallet_id=$3 AND r.run_id=$4 AND r.seq=$5 AND r.producer_node_id=$6 AND r.state='reserved')")
            .bind(&message.scope.env).bind(message.scope.tenant_id)
            .bind(message.payload.wallet_id).bind(&message.run_id).bind(message.seq).bind(message.node_id)
            .execute(&self.pool).await?.rows_affected() == 1)
    }

    pub async fn wallet_mode(
        &self,
        message: &DurableMessage<TradeIntent>,
    ) -> Result<sigbot_types::exchange::mode::ExecutionMode> {
        self.risk.wallet_mode(message).await
    }

    pub async fn find(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<String>> {
        self.risk.find(message).await
    }

    pub async fn reservation(&self, message: &DurableMessage<TradeIntent>) -> Result<(String, Decimal)> {
        self.risk.reservation(message).await
    }

    pub async fn prepared(&self, input: &DurableMessage<TradeIntent>) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT prepared_submission FROM order_info WHERE env=$1 AND tenant_id=$2
            AND wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6",
        )
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(input.payload.wallet_id)
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn persist_prepared(&self, input: &DurableMessage<TradeIntent>, candidate: &Value) -> Result<Value> {
        ensure!(
            serde_json::to_vec(candidate)?.len() <= 1_048_576,
            "prepared order exceeds storage bound"
        );
        ensure!(
            candidate
                .get("venue_order_id")
                .and_then(Value::as_str)
                .is_some_and(|id| !id.is_empty()),
            "prepared order has no venue identity"
        );
        sqlx::query("UPDATE order_info SET prepared_submission=COALESCE(prepared_submission,$7)
            WHERE env=$1 AND tenant_id=$2 AND wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6 AND state='reserved'")
            .bind(&input.scope.env).bind(input.scope.tenant_id)
            .bind(input.payload.wallet_id).bind(input.node_id).bind(&input.run_id).bind(input.seq).bind(candidate)
            .execute(&self.pool).await?;
        self.prepared(input)
            .await?
            .ok_or_else(|| anyhow::anyhow!("order is no longer reserved for SDK preparation"))
    }

    pub async fn record(
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
        state: &str,
        exchange_order_id: Option<&str>,
    ) -> Result<bool> {
        let previous = sqlx::query(
            "SELECT state,exchange_order_id FROM order_info WHERE env=$1 AND tenant_id=$2
            AND wallet_id=$3 AND run_id=$4 AND seq=$5 AND producer_node_id=$6 FOR UPDATE",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(message.payload.wallet_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .bind(message.node_id)
        .fetch_one(&mut **tx)
        .await?;
        let previous_state: String = previous.get("state");
        let previous_id: Option<String> = previous.get("exchange_order_id");
        ensure!(
            previous_id
                .as_deref()
                .zip(exchange_order_id)
                .is_none_or(|(old, new)| old == new),
            "venue order identity changed"
        );
        // A fill can race ahead of the submit response. Receipts cannot undo facts.
        if (matches!(previous_state.as_str(), "filled" | "cancelled" | "rejected") && state != previous_state)
            || (previous_state == "partially_filled" && !matches!(state, "partially_filled" | "filled" | "cancelled"))
            || (previous_state == "acknowledged" && matches!(state, "pending" | "reserved" | "submitting" | "unknown"))
        {
            return Ok(false);
        }
        let rows = sqlx::query(
            "UPDATE order_info SET state=$7,exchange_order_id=COALESCE($8,exchange_order_id),updated_at=NOW()
            WHERE env=$1 AND tenant_id=$2 AND wallet_id=$3 AND run_id=$4 AND seq=$5 AND producer_node_id=$6
            AND (state IS DISTINCT FROM $7 OR exchange_order_id IS DISTINCT FROM COALESCE($8,exchange_order_id))",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(message.payload.wallet_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .bind(message.node_id)
        .bind(state)
        .bind(exchange_order_id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        Ok(rows == 1)
    }
}
