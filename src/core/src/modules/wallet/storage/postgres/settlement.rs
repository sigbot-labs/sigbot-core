// SPDX-License-Identifier: GPL-3.0-only

use super::reservation::WalletReservationPostgresRepository;
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::wallet::projection::{PositionProjection, TradeProjection};
use sqlx::{Postgres, Row, Transaction};

pub struct WalletSettlementPostgresRepository;

impl WalletSettlementPostgresRepository {
    pub(crate) async fn settle(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<ExecutionFact>,
        projection: &TradeProjection,
    ) -> Result<()> {
        let fact = &input.payload;
        let asset: String = sqlx::query_scalar(
            "SELECT asset FROM wlt_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4",
        )
        .bind(fact.wallet_id)
        .bind(fact.reservation_node_id)
        .bind(&input.run_id)
        .bind(fact.reservation_seq)
        .fetch_one(&mut **tx)
        .await
        .context("execution requires its original wallet reservation; reconcile missing facts")?;
        let (expected, mut spent) = if projection.buy {
            (&projection.quote_asset, projection.quote_quantity)
        } else {
            (&projection.base_asset, fact.quantity)
        };
        ensure!(&asset == expected, "reservation asset differs from execution debit");
        if fact.fee_asset == asset {
            spent = spent.checked_add(fact.fee).context("fee overflow")?;
        }
        WalletReservationPostgresRepository::settle_in(
            tx,
            fact.wallet_id,
            fact.reservation_node_id,
            &input.run_id,
            fact.reservation_seq,
            spent,
        )
        .await
    }

    pub(crate) async fn release_completed(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<ExecutionFact>,
    ) -> Result<()> {
        let fact = &input.payload;
        let state = sqlx::query(
            "SELECT COALESCE(BOOL_OR((payload->>'order_complete')::boolean),false) complete,
            COALESCE(SUM((payload->>'quantity')::numeric),0) quantity FROM wlt_journal
            WHERE wallet_id=$1 AND source='execution' AND payload->>'exchange_order_id'=$2",
        )
        .bind(fact.wallet_id)
        .bind(&fact.exchange_order_id)
        .fetch_one(&mut **tx)
        .await?;
        if state.get::<bool, _>("complete") {
            let expected: Decimal = sqlx::query_scalar(
                "SELECT (intent->>'quantity')::numeric FROM ord_order
                WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4",
            )
            .bind(fact.wallet_id)
            .bind(fact.reservation_node_id)
            .bind(&input.run_id)
            .bind(fact.reservation_seq)
            .fetch_one(&mut **tx)
            .await
            .context("terminal execution requires its persisted intent")?;
            let filled: Decimal = state.get("quantity");
            ensure!(filled <= expected, "exchange overfill requires reconciliation");
            // A terminal fill may arrive before earlier partials. Release only once
            // every observed fill accounts for the intended quantity.
            if filled == expected {
                WalletReservationPostgresRepository::release_in(
                    tx,
                    fact.wallet_id,
                    fact.reservation_node_id,
                    &input.run_id,
                    fact.reservation_seq,
                )
                .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn post(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<ExecutionFact>,
        projection: &TradeProjection,
    ) -> Result<()> {
        for (index, (asset, amount, counterparty)) in projection.movements()?.into_iter().enumerate() {
            for (offset, account, amount) in [(0, "available", amount), (1, counterparty, -amount)] {
                sqlx::query("INSERT INTO wlt_posting(wallet_id,run_id,seq,line_no,account,asset,amount,producer_node_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(input.payload.wallet_id).bind(&input.run_id).bind(input.seq).bind((index * 2 + offset) as i32)
                    .bind(account).bind(&asset).bind(amount).bind(input.node_id).execute(&mut **tx).await?;
            }
        }
        for (asset, delta) in projection.balance_deltas()? {
            if delta.is_zero() {
                continue;
            }
            if delta.is_sign_positive() {
                sqlx::query(
                    "INSERT INTO wlt_balance(wallet_id,asset,available,locked) VALUES($1,$2,$3,0)
                    ON CONFLICT(wallet_id,asset) DO UPDATE
                    SET available=wlt_balance.available+EXCLUDED.available,updated_at=NOW()",
                )
                .bind(input.payload.wallet_id)
                .bind(asset)
                .bind(delta)
                .execute(&mut **tx)
                .await?;
            } else {
                let changed = sqlx::query(
                    "UPDATE wlt_balance SET available=available+$3,updated_at=NOW()
                    WHERE wallet_id=$1 AND asset=$2 AND available >= -$3",
                )
                .bind(input.payload.wallet_id)
                .bind(asset)
                .bind(delta)
                .execute(&mut **tx)
                .await?
                .rows_affected();
                ensure!(changed == 1, "execution debit exceeds available balance");
            }
        }
        Ok(())
    }

    pub(crate) async fn rebuild_position(tx: &mut Transaction<'_, Postgres>, fact: &ExecutionFact) -> Result<()> {
        let history = sqlx::query(
            "SELECT payload FROM wlt_journal WHERE wallet_id=$1
            AND source='execution' AND payload->>'symbol'=$2 ORDER BY occurred_at,fact_ref",
        )
        .bind(fact.wallet_id)
        .bind(&fact.symbol)
        .fetch_all(&mut **tx)
        .await?;
        let mut position = PositionProjection::default();
        for row in history {
            let execution: ExecutionFact = serde_json::from_value(row.get("payload"))?;
            execution.validate()?;
            let fill = TradeProjection::from_execution(&execution)?;
            position.apply_quote(fill.buy, fill.price, fill.quantity, fill.quote_quantity)?;
        }
        sqlx::query("DELETE FROM wlt_position WHERE wallet_id=$1 AND symbol=$2")
            .bind(fact.wallet_id)
            .bind(&fact.symbol)
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "INSERT INTO wlt_position(wallet_id,symbol,side,size,entry_price,realized_pnl)
            VALUES($1,$2,$3,$4,$5,$6)",
        )
        .bind(fact.wallet_id)
        .bind(&fact.symbol)
        .bind(if position.size.is_sign_negative() {
            "SHORT"
        } else {
            "LONG"
        })
        .bind(position.size.abs())
        .bind(position.entry_price)
        .bind(position.realized_pnl)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
