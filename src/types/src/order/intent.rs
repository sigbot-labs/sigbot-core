// SPDX-License-Identifier: GPL-3.0-only
//! A strategy expresses intent; only order may perform risk checks and execution.

use anyhow::ensure;

use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum IntentOrderType {
    Market,
    Limit,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketEvidence {
    pub source: String,
    pub event_time: DateTime<Utc>,
    pub available_time: DateTime<Utc>,
    pub price: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeIntent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_group: Option<super::execution_group::ExecutionGroupLeg>,
    pub account_id: Uuid,
    pub wallet_id: Uuid,
    pub exchange: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: Decimal,
    pub order_type: IntentOrderType,
    pub limit_price: Option<Decimal>,
    pub reduce_only: bool,
    pub market: MarketEvidence,
}

impl TradeIntent {
    pub fn validate(&self, now: DateTime<Utc>, max_age_ms: i64) -> Result<()> {
        if let Some(group) = &self.execution_group {
            group.validate(self.market.available_time)?;
        }
        ensure!(
            self.account_id.get_version_num() == 7 && self.wallet_id.get_version_num() == 7,
            "invalid account/wallet"
        );
        ensure!(
            !self.exchange.is_empty() && !self.symbol.is_empty() && !self.market.source.is_empty(),
            "instrument/source required"
        );
        ensure!(
            self.quantity > Decimal::ZERO && self.market.price > Decimal::ZERO,
            "invalid quantity/price"
        );
        ensure!(
            self.market.event_time <= self.market.available_time && self.market.available_time <= now,
            "market evidence includes unavailable/future data"
        );
        ensure!(
            max_age_ms >= 0 && (now - self.market.event_time).num_milliseconds() <= max_age_ms,
            "market evidence is stale"
        );
        match self.order_type {
            IntentOrderType::Market => ensure!(self.limit_price.is_none(), "market order cannot carry a limit"),
            IntentOrderType::Limit => ensure!(
                self.limit_price.is_some_and(|value| value > Decimal::ZERO),
                "limit order needs positive price"
            ),
        }
        Ok(())
    }
}
