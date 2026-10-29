// SPDX-License-Identifier: GPL-3.0-only
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationRequest {
    pub channel: String,
    pub recipient: String,
    pub template: String,
    pub variables: BTreeMap<String, Value>,
}
impl NotificationRequest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            ["in_app", "email", "telegram"].contains(&self.channel.as_str()),
            "unknown notification channel"
        );
        ensure!(
            !self.recipient.is_empty()
                && self.recipient.len() <= 512
                && !self.template.is_empty()
                && self.template.len() <= 128
                && self.variables.len() <= 128,
            "notification request is invalid or unbounded"
        );
        ensure!(
            serde_json::to_vec(self)?.len() <= 65_536,
            "notification request exceeds 64 KiB"
        );
        Ok(())
    }
}
