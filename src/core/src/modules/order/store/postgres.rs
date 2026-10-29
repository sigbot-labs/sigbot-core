// SPDX-License-Identifier: GPL-3.0-only
use anyhow::{ensure, Result};
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::modules::{messager::envelope::DurableMessage, order::intent::TradeIntent};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub struct OrderExposure {
    pub position: Decimal,
    pub pending_quantity: Decimal,
    pub pending_notional: Decimal,
}

#[derive(Clone)]
pub struct OrderPostgresRepository {
    pool: PgPool,
}

impl OrderPostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn exposure(
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
    ) -> Result<OrderExposure> {
        let intent = &message.payload;
        let wallet = sqlx::query(
            "SELECT id FROM s_wallets WHERE id=$1 AND account_id=$2 AND tenant_id=$3
            AND env=$4 AND space_id=$5 AND exchange=$6 AND del_flag=FALSE FOR UPDATE",
        )
        .bind(intent.wallet_id)
        .bind(intent.account_id)
        .bind(message.scope.tenant_id.to_string())
        .bind(&message.scope.env)
        .bind(message.scope.space_id)
        .bind(&intent.exchange)
        .fetch_optional(&mut **tx)
        .await?;
        ensure!(
            wallet.is_some(),
            "wallet/account does not belong to this tenant, environment, space and exchange"
        );
        let observed: Option<Decimal> = sqlx::query_scalar(
            "SELECT (payload->>'price')::numeric FROM market_event
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND source=$4 AND symbol=$5
            AND event_at=$6 AND available_at=$7 AND event_type='market' ORDER BY revision DESC LIMIT 1",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(message.scope.space_id)
        .bind(&intent.market.source)
        .bind(&intent.symbol)
        .bind(intent.market.event_time)
        .bind(intent.market.available_time)
        .fetch_optional(&mut **tx)
        .await?;
        ensure!(
            observed == Some(intent.market.price),
            "market evidence is not a persisted datafeed fact"
        );
        let other: i64 =
            sqlx::query_scalar("SELECT count(*) FROM s_positions WHERE wallet_id=$1 AND symbol<>$2 AND size<>0")
                .bind(intent.wallet_id)
                .bind(&intent.symbol)
                .fetch_one(&mut **tx)
                .await?;
        ensure!(
            other == 0,
            "cross-instrument exposure requires complete portfolio valuation"
        );
        let position = sqlx::query_scalar(
            "SELECT COALESCE(SUM(CASE WHEN side='LONG' THEN size ELSE -size END),0)
            FROM s_positions WHERE wallet_id=$1 AND symbol=$2",
        )
        .bind(intent.wallet_id)
        .bind(&intent.symbol)
        .fetch_one(&mut **tx)
        .await?;
        let pending = sqlx::query("SELECT COALESCE(SUM((intent->>'quantity')::numeric),0) AS quantity,
            COALESCE(SUM((intent->>'quantity')::numeric * (intent->'market'->>'price')::numeric),0) AS notional
            FROM order_record o WHERE wallet_id=$1 AND (state IN ('pending','reserved','submitting','unknown','acknowledged','partially_filled')
            OR EXISTS(SELECT 1 FROM wallet_reservation r WHERE r.wallet_id=o.wallet_id AND r.producer_node_id=o.producer_node_id
                AND r.run_id=o.run_id AND r.seq=o.seq AND r.state='reserved'))")
            .bind(intent.wallet_id).fetch_one(&mut **tx).await?;
        Ok(OrderExposure {
            position,
            pending_quantity: pending.get("quantity"),
            pending_notional: pending.get("notional"),
        })
    }

    pub async fn find(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<String>> {
        let mut tx = self.pool.begin().await?;
        let found = Self::find_in(&mut tx, message).await?;
        tx.commit().await?;
        Ok(found)
    }

    pub async fn find_in(
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT state,intent FROM order_record WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND wallet_id=$4 AND run_id=$5 AND seq=$6 AND producer_node_id=$7",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id.to_string())
        .bind(message.scope.space_id)
        .bind(message.payload.wallet_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .bind(message.node_id)
        .fetch_optional(&mut **tx)
        .await?;
        row.map(|row| {
            ensure!(
                row.get::<Value, _>("intent") == serde_json::to_value(&message.payload)?,
                "intent identity was reused with different terms"
            );
            Ok(row.get("state"))
        })
        .transpose()
    }

    pub async fn reservation(&self, message: &DurableMessage<TradeIntent>) -> Result<(String, Decimal)> {
        Ok(sqlx::query_as(
            "SELECT reserve_asset,reserve_amount FROM order_record WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND wallet_id=$4 AND run_id=$5 AND seq=$6 AND producer_node_id=$7",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id.to_string())
        .bind(message.scope.space_id)
        .bind(message.payload.wallet_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .bind(message.node_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn insert(
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
        asset: &str,
        amount: Decimal,
    ) -> Result<bool> {
        Ok(sqlx::query("INSERT INTO order_record(env,tenant_id,space_id,wallet_id,run_id,seq,exchange,symbol,intent,reserve_asset,reserve_amount,producer_node_id,state)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'pending') ON CONFLICT DO NOTHING")
            .bind(&message.scope.env).bind(message.scope.tenant_id.to_string()).bind(message.scope.space_id)
            .bind(message.payload.wallet_id).bind(&message.run_id).bind(message.seq).bind(&message.payload.exchange)
            .bind(&message.payload.symbol).bind(serde_json::to_value(&message.payload)?).bind(asset).bind(amount).bind(message.node_id)
            .execute(&mut **tx).await?.rows_affected() == 1)
    }

    pub async fn claim_submission(&self, message: &DurableMessage<TradeIntent>) -> Result<bool> {
        Ok(sqlx::query("UPDATE order_record SET state='submitting',updated_at=NOW() WHERE env=$1 AND tenant_id=$2
            AND space_id=$3 AND wallet_id=$4 AND run_id=$5 AND seq=$6 AND producer_node_id=$7 AND state='reserved'
            AND EXISTS(SELECT 1 FROM wallet_reservation r WHERE r.wallet_id=$4 AND r.run_id=$5 AND r.seq=$6 AND r.producer_node_id=$7 AND r.state='reserved')")
            .bind(&message.scope.env).bind(message.scope.tenant_id.to_string()).bind(message.scope.space_id)
            .bind(message.payload.wallet_id).bind(&message.run_id).bind(message.seq).bind(message.node_id)
            .execute(&self.pool).await?.rows_affected() == 1)
    }

    pub async fn record(
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
        state: &str,
        exchange_order_id: Option<&str>,
    ) -> Result<bool> {
        let previous = sqlx::query(
            "SELECT state,exchange_order_id FROM order_record WHERE env=$1 AND tenant_id=$2
            AND space_id=$3 AND wallet_id=$4 AND run_id=$5 AND seq=$6 AND producer_node_id=$7 FOR UPDATE",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id.to_string())
        .bind(message.scope.space_id)
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
        let rows = sqlx::query("UPDATE order_record SET state=$8,exchange_order_id=COALESCE($9,exchange_order_id),updated_at=NOW()
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND wallet_id=$4 AND run_id=$5 AND seq=$6 AND producer_node_id=$7
            AND (state IS DISTINCT FROM $8 OR exchange_order_id IS DISTINCT FROM COALESCE($9,exchange_order_id))")
            .bind(&message.scope.env).bind(message.scope.tenant_id.to_string()).bind(message.scope.space_id)
            .bind(message.payload.wallet_id).bind(&message.run_id).bind(message.seq).bind(message.node_id).bind(state).bind(exchange_order_id)
            .execute(&mut **tx).await?.rows_affected();
        Ok(rows == 1)
    }
}
