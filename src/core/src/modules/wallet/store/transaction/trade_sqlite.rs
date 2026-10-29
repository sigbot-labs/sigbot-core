// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use super::{
    projection::{PositionProjection, TradeProjection},
    IWalletUpdater,
};
use crate::config::config::SqliteAppDBProperties;
use crate::store::sqlite::SQLiteRepository;
use anyhow::{ensure, Context, Error};
use async_trait::async_trait;
use rust_decimal::Decimal;
use sigbot_types::modules::order::events::SigbotTradeEvent;
use sqlx::{sqlite::SqlitePool, Row};
use std::str::FromStr;

pub struct SqliteWalletUpdater {
    db_pool: SqlitePool,
}

impl SqliteWalletUpdater {
    pub async fn new(config: &SqliteAppDBProperties) -> Result<Self, Error> {
        let repo = SQLiteRepository::<()>::get_or_init(config).await?;
        Ok(Self {
            db_pool: repo.get_pool().clone(),
        })
    }

    pub fn from_pool(db_pool: SqlitePool) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl IWalletUpdater for SqliteWalletUpdater {
    async fn upsert(&self, event: &SigbotTradeEvent) -> Result<(), Error> {
        let trade = TradeProjection::from_event(event)?;
        let mut tx = self.db_pool.begin().await?;
        let wallet =
            sqlx::query("UPDATE s_wallets SET updated_at=updated_at WHERE id=?1 AND tenant_id=?2 AND exchange=?3")
                .bind(event.wallet_id)
                .bind(&event.tenant_id)
                .bind(&event.exchange)
                .execute(&mut *tx)
                .await?;
        ensure!(
            wallet.rows_affected() == 1,
            "execution does not belong to this tenant wallet/exchange"
        );
        let inserted = sqlx::query("INSERT INTO s_ledger(wallet_id,order_id,trade_id,symbol,side,price,qty,fee,fee_asset,exchange_order_id,exchange_trade_id,ts)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,strftime('%Y-%m-%d %H:%M:%f',?12/1000.0,'unixepoch')) ON CONFLICT DO NOTHING")
            .bind(event.wallet_id).bind(event.order_id).bind(event.trade_id).bind(&event.symbol).bind(&event.side)
            .bind(trade.price.to_string()).bind(trade.quantity.to_string()).bind(trade.fee.to_string())
            .bind(&trade.fee_asset).bind(&event.exchange_order_id).bind(&event.exchange_trade_id).bind(event.ts)
            .execute(&mut *tx).await?.rows_affected();
        if inserted == 0 {
            tx.commit().await?;
            return Ok(());
        }
        // Immutable fact and every projection commit together. Duplicate facts never
        // apply deltas again; a failed projection rolls the new fact back as well.
        for (asset, delta) in trade.balance_deltas()? {
            sqlx::query(
                "INSERT INTO s_balances(wallet_id,asset,available,locked,updated_at)
                VALUES(?1,?2,?3,0,CURRENT_TIMESTAMP) ON CONFLICT(wallet_id,asset) DO UPDATE SET
                available=s_balances.available+EXCLUDED.available,updated_at=CURRENT_TIMESTAMP",
            )
            .bind(event.wallet_id)
            .bind(asset)
            .bind(delta.to_string())
            .execute(&mut *tx)
            .await?;
        }
        let rows = sqlx::query("SELECT side,CAST(price AS TEXT) AS price,CAST(qty AS TEXT) AS qty FROM s_ledger WHERE wallet_id=?1 AND symbol=?2
            ORDER BY ts,COALESCE(exchange_trade_id,''),order_id,trade_id")
            .bind(event.wallet_id).bind(&event.symbol).fetch_all(&mut *tx).await?;
        let mut position = PositionProjection::default();
        for row in rows {
            position.apply(
                row.get::<String, _>("side") == "BUY",
                Decimal::from_str(&row.get::<String, _>("price"))?,
                Decimal::from_str(&row.get::<String, _>("qty"))?,
            )?;
        }
        // Net exposure has a single active side; reset the opposite projection.
        sqlx::query("DELETE FROM s_positions WHERE wallet_id=?1 AND symbol=?2")
            .bind(event.wallet_id)
            .bind(&event.symbol)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO s_positions(wallet_id,symbol,side,size,entry_price,realized_pnl,updated_at) VALUES(?1,?2,?3,?4,?5,?6,CURRENT_TIMESTAMP)")
            .bind(event.wallet_id).bind(&event.symbol).bind(if position.size.is_sign_negative() { "SHORT" } else { "LONG" })
            .bind(position.size.abs().to_string()).bind(position.entry_price.to_string()).bind(position.realized_pnl.to_string())
            .execute(&mut *tx).await?;
        // Equity requires valuation prices in one numeraire. Never sum BTC and USDT.
        tx.commit()
            .await
            .context("failed to commit execution and wallet projections")?;
        Ok(())
    }
}
