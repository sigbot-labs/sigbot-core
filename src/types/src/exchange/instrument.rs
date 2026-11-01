// SPDX-License-Identifier: GPL-3.0-only
//! Authoritative instrument assets and precision, shared by risk and venue adapters.
use anyhow::ensure;
use anyhow::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentSpec {
    pub exchange: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub quantity_step: Decimal,
    pub price_tick: Decimal,
    pub min_notional: Decimal,
}

impl InstrumentSpec {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.exchange.is_empty() && !self.symbol.is_empty(),
            "instrument identity required"
        );
        ensure!(
            !self.base_asset.is_empty() && !self.quote_asset.is_empty() && self.base_asset != self.quote_asset,
            "explicit, different instrument assets required"
        );
        ensure!(
            self.quantity_step > Decimal::ZERO && self.price_tick > Decimal::ZERO && self.min_notional >= Decimal::ZERO,
            "invalid instrument precision/notional"
        );
        Ok(())
    }
}
