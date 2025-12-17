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

use crate::modules::exchange::models::trade_position::EntryTradePosition;
use serde::{Deserialize, Serialize};

/// Message (from strategy-runner to order-manager)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SigbotTradeSignal {
    /// Signal ID, for idempotency check
    pub signal_id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Wallet ID
    pub wallet_id: i64,
    /// Exchange Name
    pub exchange: String,
    /// Trading Signal
    pub signal: EntryTradePosition,
}

impl SigbotTradeSignal {
    /// Generate Idempotency Key
    pub fn idempotency_key(&self) -> String {
        format!("signal:{}:{}", self.tenant_id, self.signal_id)
    }
}

/// Message (from order-manager to wallet-manager)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SigbotTradeEvent {
    /// Trade ID, for idempotency check
    pub trade_id: i64,
    /// Order ID
    pub order_id: i64,
    /// Signal ID (original signal)
    pub signal_id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Wallet ID
    pub wallet_id: i64,
    /// Exchange Name
    pub exchange: String,
    /// Symbol
    pub symbol: String,
    /// Trade Side (BUY/SELL)
    pub side: String,
    /// Trade Price
    pub price: f64,
    /// Trade Quantity
    pub qty: f64,
    /// Fee
    pub fee: f64,
    /// Fee Asset
    pub fee_asset: Option<String>,
    /// Exchange Order ID
    pub exchange_order_id: Option<String>,
    /// Exchange Trade ID
    pub exchange_trade_id: Option<String>,
    /// Timestamp (milliseconds)
    pub ts: i64,
}

impl SigbotTradeEvent {
    /// Generate Idempotency Key
    pub fn idempotency_key(&self) -> String {
        format!(
            "trade:{}:{}:{}:{}",
            self.tenant_id, self.wallet_id, self.order_id, self.trade_id
        )
    }

    /// Generate Idempotency Key from exchange trade_id (if exists)
    pub fn exchange_idempotency_key(&self) -> Option<String> {
        self.exchange_trade_id
            .as_ref()
            .map(|id| format!("trade:{}:{}:{}", self.exchange, self.wallet_id, id))
    }
}
