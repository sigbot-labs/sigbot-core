// SPDX-License-Identifier: GPL-3.0-only
use anyhow::ensure;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// An observed deposit, withdrawal or funding transfer. Amount is signed; its
/// counter-entry is clearing. Corrections append another external source fact.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingFact {
    pub wallet_id: Uuid,
    pub account_id: Uuid,
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
            self.wallet_id.get_version_num() == 7 && self.account_id.get_version_num() == 7 && !self.amount.is_zero(),
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
