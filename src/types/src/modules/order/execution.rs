// SPDX-License-Identifier: GPL-3.0-only
use super::intent::OrderSide;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderState {
    Pending,
    Reserved,
    Submitting,
    Acknowledged,
    Unknown,
    Rejected,
    PartiallyFilled,
    Filled,
    Cancelled,
}

impl OrderState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Reserved => "reserved",
            Self::Submitting => "submitting",
            Self::Acknowledged => "acknowledged",
            Self::Unknown => "unknown",
            Self::Rejected => "rejected",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::Cancelled => "cancelled",
        }
    }
}

/// A receipt confirms acceptance only. Wallet must never consume this as an execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderReceipt {
    pub exchange_order_id: String,
    pub state: OrderState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionFact {
    pub reservation_node_id: i64,
    /// Sequence of the originating intent/reservation within the same run.
    pub reservation_seq: i64,
    pub wallet_id: i64,
    pub account_id: i64,
    pub exchange: String,
    pub exchange_order_id: String,
    pub exchange_trade_id: String,
    pub symbol: String,
    /// Empty only while decoding an unmigrated legacy payload; validate fails closed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub base_asset: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub quote_asset: String,
    pub side: OrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    /// Actual gross quote transfer before fees; preserves venue settlement rounding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_quantity: Option<Decimal>,
    pub fee: Decimal,
    pub fee_asset: String,
    #[serde(default)]
    pub order_complete: bool,
    pub executed_at: DateTime<Utc>,
}

impl ExecutionFact {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.base_asset.is_empty() && !self.quote_asset.is_empty() && self.base_asset != self.quote_asset,
            "execution requires explicit instrument assets"
        );
        ensure!(
            self.reservation_node_id > 0 && self.reservation_seq >= 0 && self.wallet_id > 0 && self.account_id > 0,
            "invalid execution owner"
        );
        ensure!(
            !self.exchange.is_empty()
                && !self.symbol.is_empty()
                && !self.exchange_order_id.is_empty()
                && !self.exchange_trade_id.is_empty(),
            "execution identity required"
        );
        ensure!(
            self.price > Decimal::ZERO && self.quantity > Decimal::ZERO,
            "nonpositive execution"
        );
        ensure!(
            self.quote_quantity.is_none_or(|amount| amount > Decimal::ZERO),
            "nonpositive execution quote quantity"
        );
        ensure!(
            self.fee >= Decimal::ZERO && !self.fee_asset.is_empty(),
            "invalid execution fee"
        );
        Ok(())
    }
}

/// An authoritative venue status, including cumulative executed quantity. A terminal
/// status may arrive before individual fills; wallet releases only after reconciling them.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderStatusFact {
    pub reservation_node_id: i64,
    pub reservation_seq: i64,
    pub wallet_id: i64,
    pub account_id: i64,
    pub exchange: String,
    pub receipt: OrderReceipt,
    pub executed_quantity: Decimal,
    pub occurred_at: DateTime<Utc>,
}

impl OrderStatusFact {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.reservation_node_id > 0 && self.reservation_seq >= 0 && self.wallet_id > 0 && self.account_id > 0,
            "invalid order status owner"
        );
        ensure!(
            !self.exchange.is_empty()
                && !self.receipt.exchange_order_id.is_empty()
                && self.executed_quantity >= Decimal::ZERO,
            "venue status requires an order ID and nonnegative cumulative quantity"
        );
        ensure!(
            matches!(
                self.receipt.state,
                OrderState::Acknowledged
                    | OrderState::PartiallyFilled
                    | OrderState::Filled
                    | OrderState::Cancelled
                    | OrderState::Rejected
            ),
            "invalid venue status"
        );
        Ok(())
    }
}
