// SPDX-License-Identifier: GPL-3.0-only
//! Wallet-owned reservation transactions. Distributed order workers send reservation
//! requests to wallet; only the embedded compatibility mode calls this repository.

use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use sqlx::{PgPool, Postgres, Row, Transaction};

#[derive(Clone)]
pub struct PostgresWalletReservation {
    pool: PgPool,
}

impl PostgresWalletReservation {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn reserve(
        &self,
        wallet_id: i64,
        tenant_id: &str,
        env: &str,
        space_id: i64,
        producer_node_id: i64,
        run_id: &str,
        seq: i64,
        asset: &str,
        amount: Decimal,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let changed = Self::reserve_in(
            &mut tx,
            wallet_id,
            tenant_id,
            env,
            space_id,
            producer_node_id,
            run_id,
            seq,
            asset,
            amount,
        )
        .await?;
        tx.commit().await?;
        Ok(changed)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn reserve_in(
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: i64,
        tenant_id: &str,
        env: &str,
        space_id: i64,
        producer_node_id: i64,
        run_id: &str,
        seq: i64,
        asset: &str,
        amount: Decimal,
    ) -> Result<bool> {
        ensure!(
            !run_id.is_empty() && seq >= 0 && producer_node_id > 0 && !asset.is_empty() && amount > Decimal::ZERO,
            "invalid reservation"
        );
        Self::lock_wallet(tx, wallet_id, tenant_id, env, space_id).await?;
        let existing = sqlx::query("SELECT asset,amount,state FROM wallet_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4")
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
            "UPDATE s_balances SET available=available-$3,locked=locked+$3,updated_at=NOW()
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
            "INSERT INTO wallet_reservation(wallet_id,producer_node_id,run_id,seq,asset,amount,remaining,state)
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

    #[allow(clippy::too_many_arguments)]
    pub async fn release(
        &self,
        wallet_id: i64,
        tenant_id: &str,
        env: &str,
        space_id: i64,
        producer_node_id: i64,
        run_id: &str,
        seq: i64,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        Self::lock_wallet(&mut tx, wallet_id, tenant_id, env, space_id).await?;
        let changed = Self::release_in(&mut tx, wallet_id, producer_node_id, run_id, seq).await?;
        tx.commit().await?;
        Ok(changed)
    }

    pub async fn release_in(
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: i64,
        producer_node_id: i64,
        run_id: &str,
        seq: i64,
    ) -> Result<bool> {
        let row = sqlx::query("SELECT asset,remaining,state FROM wallet_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4 FOR UPDATE")
            .bind(wallet_id).bind(producer_node_id).bind(run_id).bind(seq).fetch_one(&mut **tx).await.context("reservation missing")?;
        if row.get::<String, _>("state") != "reserved" {
            return Ok(false);
        }
        let remaining: Decimal = row.get("remaining");
        let asset: String = row.get("asset");
        let rows = sqlx::query(
            "UPDATE s_balances SET available=available+$3,locked=locked-$3,updated_at=NOW()
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
        sqlx::query("UPDATE wallet_reservation SET state='released',remaining=0,updated_at=NOW() WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4")
            .bind(wallet_id).bind(producer_node_id).bind(run_id).bind(seq).execute(&mut **tx).await?;
        Ok(true)
    }

    /// Move consumed reserve back to available immediately before applying the real
    /// execution debit in the same transaction. An order ACK must never call this.
    pub async fn settle_in(
        tx: &mut Transaction<'_, Postgres>,
        wallet_id: i64,
        producer_node_id: i64,
        run_id: &str,
        seq: i64,
        spent: Decimal,
    ) -> Result<()> {
        ensure!(spent > Decimal::ZERO, "settlement amount must be positive");
        let row = sqlx::query("SELECT asset,remaining,state FROM wallet_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4 FOR UPDATE")
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
            "UPDATE s_balances SET available=available+$3,locked=locked-$3,updated_at=NOW()
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
            "UPDATE wallet_reservation SET remaining=remaining-$5,
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
        wallet_id: i64,
        tenant_id: &str,
        env: &str,
        space_id: i64,
    ) -> Result<()> {
        let row =
            sqlx::query("SELECT id FROM s_wallets WHERE id=$1 AND tenant_id=$2 AND env=$3 AND space_id=$4 FOR UPDATE")
                .bind(wallet_id)
                .bind(tenant_id)
                .bind(env)
                .bind(space_id)
                .fetch_optional(&mut **tx)
                .await?;
        ensure!(
            row.is_some(),
            "wallet not found in the authenticated tenant/environment/space"
        );
        Ok(())
    }
}
