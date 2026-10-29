// SPDX-License-Identifier: GPL-3.0-only
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// An observed deposit, withdrawal or funding transfer. Amount is signed; its
/// counter-entry is clearing. Corrections append another external source fact.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingFact {
    pub wallet_id: i64,
    pub account_id: i64,
    pub exchange: String,
    pub source: String,
    pub source_event_id: String,
    pub asset: String,
    pub amount: Decimal,
    pub occurred_at: DateTime<Utc>,
}

impl FundingFact {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.wallet_id > 0 && self.account_id > 0 && !self.amount.is_zero(),
            "invalid funding account/amount"
        );
        ensure!(
            !self.exchange.is_empty()
                && !self.source.is_empty()
                && !self.source_event_id.is_empty()
                && !self.asset.is_empty(),
            "funding requires source provenance and asset"
        );
        Ok(())
    }
}
