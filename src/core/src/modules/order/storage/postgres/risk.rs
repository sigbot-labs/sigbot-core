// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::order::storage::postgres::OrderPostgresRepository;
use anyhow::{ensure, Result};
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::order::intent::TradeIntent;
use sqlx::{PgPool, Postgres, Row, Transaction};

pub struct OrderExposure {
    pub mode: sigbot_types::exchange::mode::ExecutionMode,
    pub position: Decimal,
    pub pending_quantity: Decimal,
    pub pending_notional: Decimal,
}

#[derive(Clone)]
pub struct OrderRiskPostgresRepository {
    pool: PgPool,
}

impl OrderRiskPostgresRepository {
    pub fn new(storage: &OrderPostgresRepository) -> Self {
        Self {
            pool: storage.pool.clone(),
        }
    }

    pub async fn wallet_mode(
        &self,
        message: &DurableMessage<TradeIntent>,
    ) -> Result<sigbot_types::exchange::mode::ExecutionMode> {
        let mut tx = self.pool.begin().await?;
        let mode = Self::wallet_mode_in(&mut tx, message).await?;
        tx.commit().await?;
        Ok(mode)
    }

    async fn wallet_mode_in(
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
    ) -> Result<sigbot_types::exchange::mode::ExecutionMode> {
        let intent = &message.payload;
        let wallet = sqlx::query(
            "SELECT id,mode FROM wlt_wallet WHERE id=$1 AND account_id=$2 AND tenant_id=$3
            AND env=$4 AND exchange=$5 AND del_flag=FALSE FOR UPDATE",
        )
        .bind(intent.wallet_id)
        .bind(intent.account_id)
        .bind(message.scope.tenant_id)
        .bind(&message.scope.env)
        .bind(&intent.exchange)
        .fetch_optional(&mut **tx)
        .await?;
        ensure!(
            wallet.is_some(),
            "wallet/account does not belong to this tenant, environment and exchange"
        );
        Ok(serde_json::from_value(serde_json::json!(wallet
            .expect("validated wallet")
            .get::<String, _>("mode")))?)
    }

    pub async fn exposure(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
    ) -> Result<OrderExposure> {
        let intent = &message.payload;
        let mode = Self::wallet_mode_in(tx, message).await?;
        let observed: Option<Decimal> = sqlx::query_scalar(
            "SELECT (payload->>'price')::numeric FROM mkt_event
            WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND source=$4 AND symbol=$5
            AND event_at=$6 AND available_at=$7 AND event_type='market' ORDER BY revision DESC LIMIT 1",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(&message.run_id)
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
            sqlx::query_scalar("SELECT count(*) FROM wlt_position WHERE wallet_id=$1 AND symbol<>$2 AND size<>0")
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
            FROM wlt_position WHERE wallet_id=$1 AND symbol=$2",
        )
        .bind(intent.wallet_id)
        .bind(&intent.symbol)
        .fetch_one(&mut **tx)
        .await?;
        let pending = sqlx::query("SELECT COALESCE(SUM((intent->>'quantity')::numeric),0) AS quantity,
            COALESCE(SUM((intent->>'quantity')::numeric * (intent->'market'->>'price')::numeric),0) AS notional
            FROM ord_order o WHERE wallet_id=$1 AND (state IN ('pending','reserved','submitting','unknown','acknowledged','partially_filled')
            OR EXISTS(SELECT 1 FROM wlt_reservation r WHERE r.wallet_id=o.wallet_id AND r.producer_node_id=o.producer_node_id
                AND r.run_id=o.run_id AND r.seq=o.seq AND r.state='reserved'))")
            .bind(intent.wallet_id).fetch_one(&mut **tx).await?;
        Ok(OrderExposure {
            mode,
            position,
            pending_quantity: pending.get("quantity"),
            pending_notional: pending.get("notional"),
        })
    }

    pub async fn find(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<String>> {
        let mut tx = self.pool.begin().await?;
        let found = self.find_in(&mut tx, message).await?;
        tx.commit().await?;
        Ok(found)
    }

    pub async fn find_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        message: &DurableMessage<TradeIntent>,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT state,intent FROM ord_order WHERE env=$1 AND tenant_id=$2
            AND wallet_id=$3 AND run_id=$4 AND seq=$5 AND producer_node_id=$6",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
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
            "SELECT reserve_asset,reserve_amount FROM ord_order WHERE env=$1 AND tenant_id=$2
            AND wallet_id=$3 AND run_id=$4 AND seq=$5 AND producer_node_id=$6",
        )
        .bind(&message.scope.env)
        .bind(message.scope.tenant_id)
        .bind(message.payload.wallet_id)
        .bind(&message.run_id)
        .bind(message.seq)
        .bind(message.node_id)
        .fetch_one(&self.pool)
        .await?)
    }
}
