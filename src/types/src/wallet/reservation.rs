// SPDX-License-Identifier: GPL-3.0-only
//! Original intent identity is the reservation identity; transport span pairing is ephemeral.
use anyhow::ensure;
use anyhow::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationRequest {
    pub wallet_id: Uuid,
    pub account_id: Uuid,
    pub reservation_node_id: i64,
    pub reservation_seq: i64,
    pub action: ReservationAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReservationAction {
    Reserve { asset: String, amount: Decimal },
    Release,
}

impl ReservationRequest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.wallet_id.get_version_num() == 7
                && self.account_id.get_version_num() == 7
                && self.reservation_node_id > 0
                && self.reservation_seq >= 0,
            "invalid reservation identity"
        );
        if let ReservationAction::Reserve { asset, amount } = &self.action {
            ensure!(
                !asset.is_empty() && asset.len() <= 32 && *amount > Decimal::ZERO,
                "invalid reservation terms"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationState {
    Reserved,
    Released,
    Rejected,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationResult {
    pub request: ReservationRequest,
    pub state: ReservationState,
    pub reason: Option<String>,
}
