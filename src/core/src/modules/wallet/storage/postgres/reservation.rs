// SPDX-License-Identifier: GPL-3.0-only
//! Wallet-owned reservation transactions used by the distributed wallet consumer.

use anyhow::ensure;

use anyhow::Context;

use anyhow::Result;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::wallet::reservation::{ReservationRequest, ReservationState};
use sqlx::postgres::PgRow;
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

pub struct WalletReservationPostgresRepository;

impl WalletReservationPostgresRepository {
    #[allow(clippy::too_many_arguments)]
    pub async fn reserve_in(
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: Uuid,
        tenant_id: Uuid,
        env: &str,
        producer_node_id: i64,
        run_id: &Uuid,
        seq: i64,
        asset: &str,
        amount: Decimal,
    ) -> Result<bool> {
        ensure!(
            run_id.get_version_num() == 7
                && seq >= 0
                && producer_node_id > 0
                && !asset.is_empty()
                && amount > Decimal::ZERO,
            "invalid reservation"
        );
        Self::lock_wallet(tx, wallet_id, tenant_id, env).await?;
        let existing = sqlx::query("SELECT asset,amount,state FROM wlt_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4")
            .bind(wallet_id).bind(producer_node_id).bind(run_id).bind(seq).fetch_optional(&mut **tx).await?;
        if let Some(row) = existing {
            ensure!(
                row.get::<String, _>("asset") == asset && row.get::<Decimal, _>("amount") == amount,
                "reservation identity reused with different terms"
            );
            ensure!(
                row.get::<String, _>("state") == "reserved",
                "reservation is already closed"
            );
            return Ok(false);
        }
        let rows = sqlx::query(
            "UPDATE wlt_balance SET available=available-$3,locked=locked+$3,updated_at=NOW()
            WHERE wallet_id=$1 AND asset=$2 AND available >= $3",
        )
        .bind(wallet_id)
        .bind(asset)
        .bind(amount)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        ensure!(rows == 1, "insufficient available balance for reservation");
        sqlx::query(
            "INSERT INTO wlt_reservation(wallet_id,producer_node_id,run_id,seq,asset,amount,remaining,state)
            VALUES($1,$2,$3,$4,$5,$6,$6,'reserved')",
        )
        .bind(wallet_id)
        .bind(producer_node_id)
        .bind(run_id)
        .bind(seq)
        .bind(asset)
        .bind(amount)
        .execute(&mut **tx)
        .await?;
        Ok(true)
    }

    pub async fn release_in(
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: Uuid,
        producer_node_id: i64,
        run_id: &Uuid,
        seq: i64,
    ) -> Result<bool> {
        let row = sqlx::query("SELECT asset,remaining,state FROM wlt_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4 FOR UPDATE")
            .bind(wallet_id).bind(producer_node_id).bind(run_id).bind(seq).fetch_one(&mut **tx).await.context("reservation missing")?;
        if row.get::<String, _>("state") != "reserved" {
            return Ok(false);
        }
        let remaining: Decimal = row.get("remaining");
        let asset: String = row.get("asset");
        let rows = sqlx::query(
            "UPDATE wlt_balance SET available=available+$3,locked=locked-$3,updated_at=NOW()
            WHERE wallet_id=$1 AND asset=$2 AND locked >= $3",
        )
        .bind(wallet_id)
        .bind(asset)
        .bind(remaining)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        ensure!(
            rows == 1,
            "reservation projection is inconsistent; reconcile before releasing"
        );
        sqlx::query("UPDATE wlt_reservation SET state='released',remaining=0,updated_at=NOW() WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4")
            .bind(wallet_id).bind(producer_node_id).bind(run_id).bind(seq).execute(&mut **tx).await?;
        Ok(true)
    }

    pub async fn release_order_in(
        tx: &mut Transaction<'_, Postgres>,
        account_id: Uuid,
        input: &DurableMessage<ReservationRequest>,
        order: &PgRow,
    ) -> Result<ReservationState> {
        let request = &input.payload;
        let state: String = order.get("state");
        let booked: Decimal = sqlx::query_scalar("SELECT COALESCE(SUM((payload->>'quantity')::numeric),0) FROM wlt_journal
            WHERE wallet_id=$1 AND source='execution' AND run_id=$2 AND (payload->>'reservation_node_id')::bigint=$3 AND (payload->>'reservation_seq')::bigint=$4")
            .bind(request.wallet_id).bind(&input.run_id).bind(request.reservation_node_id).bind(request.reservation_seq)
            .fetch_one(&mut **tx).await?;
        let mut safe = if state == "rejected" {
            booked.is_zero() && order.get::<Option<String>, _>("exchange_order_id").is_none()
        } else if state == "filled" {
            booked == serde_json::from_value::<sigbot_types::order::intent::TradeIntent>(order.get("intent"))?.quantity
        } else {
            false
        };
        if !safe && matches!(state.as_str(), "cancelled" | "rejected") {
            let terminal: Option<Value> = sqlx::query_scalar("SELECT payload->'payload' FROM msg_outbox WHERE env=$1 AND tenant_id=$2
                AND node_id=$3 AND run_id=$4 AND (payload->'payload'->>'wallet_id')::uuid=$5
                AND (payload->'payload'->>'reservation_node_id')::bigint=$6 AND (payload->'payload'->>'reservation_seq')::bigint=$7
                AND payload->'payload'->'receipt'->>'state' IN ('cancelled','rejected') ORDER BY seq DESC LIMIT 1")
                .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.node_id).bind(&input.run_id)
                .bind(request.wallet_id).bind(request.reservation_node_id).bind(request.reservation_seq)
                .fetch_optional(&mut **tx).await?;
            if let Some(value) = terminal {
                let fact: sigbot_types::order::execution::OrderStatusFact = serde_json::from_value(value)?;
                safe = fact.account_id == account_id && fact.executed_quantity == booked;
            }
        }
        ensure!(safe, "release requires fully booked executions or a rejection before venue submission; cancellation requires reconciliation");
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM wlt_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4)")
            .bind(request.wallet_id).bind(request.reservation_node_id).bind(&input.run_id).bind(request.reservation_seq)
            .fetch_one(&mut **tx).await?;
        if exists {
            Self::release_in(
                tx,
                request.wallet_id,
                request.reservation_node_id,
                &input.run_id,
                request.reservation_seq,
            )
            .await?;
        }
        Ok(ReservationState::Released)
    }

    /// Move consumed reserve back to available immediately before applying the real
    /// execution debit in the same transaction. An order ACK must never call this.
    pub async fn settle_in(
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: Uuid,
        producer_node_id: i64,
        run_id: &Uuid,
        seq: i64,
        spent: Decimal,
    ) -> Result<()> {
        ensure!(spent > Decimal::ZERO, "settlement amount must be positive");
        let row = sqlx::query("SELECT asset,remaining,state FROM wlt_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4 FOR UPDATE")
            .bind(wallet_id).bind(producer_node_id).bind(run_id).bind(seq).fetch_one(&mut **tx).await?;
        ensure!(
            row.get::<String, _>("state") == "reserved",
            "cannot settle a closed reservation"
        );
        let remaining: Decimal = row.get("remaining");
        ensure!(
            spent <= remaining,
            "execution exceeds reservation; reconcile instead of overdrawing"
        );
        let rows = sqlx::query(
            "UPDATE wlt_balance SET available=available+$3,locked=locked-$3,updated_at=NOW()
            WHERE wallet_id=$1 AND asset=$2 AND locked >= $3",
        )
        .bind(wallet_id)
        .bind(row.get::<String, _>("asset"))
        .bind(spent)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        ensure!(rows == 1, "reserved balance is inconsistent");
        sqlx::query(
            "UPDATE wlt_reservation SET remaining=remaining-$5,
            state=CASE WHEN remaining=$5 THEN 'settled' ELSE 'reserved' END,updated_at=NOW()
            WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4",
        )
        .bind(wallet_id)
        .bind(producer_node_id)
        .bind(run_id)
        .bind(seq)
        .bind(spent)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn lock_wallet(
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: Uuid,
        tenant_id: Uuid,
        env: &str,
    ) -> Result<()> {
        let row = sqlx::query("SELECT id FROM wlt_wallet WHERE id=$1 AND tenant_id=$2 AND env=$3 FOR UPDATE")
            .bind(wallet_id)
            .bind(tenant_id)
            .bind(env)
            .fetch_optional(&mut **tx)
            .await?;
        ensure!(
            row.is_some(),
            "wallet not found in the authenticated tenant/environment"
        );
        Ok(())
    }
}
