// SPDX-License-Identifier: GPL-3.0-only
use anyhow::ensure;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupFailurePolicy {
    Hold,
    CompensateFilled,
}

/// Group identity is the originating strategy node/run and its business sequence.
/// Each leg remains an ordinary risk-checked, reserved and reconciled order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionGroupLeg {
    pub group_seq: i64,
    pub coordinator_node_id: i64,
    pub leg_index: u16,
    pub leg_count: u16,
    pub deadline: DateTime<Utc>,
    pub failure_policy: GroupFailurePolicy,
}
impl ExecutionGroupLeg {
    pub fn validate(&self, available_time: DateTime<Utc>) -> Result<()> {
        ensure!(
            self.group_seq >= 0
                && self.coordinator_node_id > 0
                && (2..=8).contains(&self.leg_count)
                && self.leg_index < self.leg_count,
            "invalid execution group identity/leg bounds"
        );
        ensure!(
            available_time <= self.deadline && (self.deadline - available_time).num_hours() <= 24,
            "execution group deadline is past or unbounded"
        );
        Ok(())
    }
}
