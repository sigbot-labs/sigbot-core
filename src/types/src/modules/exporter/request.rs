// SPDX-License-Identifier: GPL-3.0-only
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportBatch {
    /// Exporter derives this from the existing scoped producer/run/sequence.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,
    /// Fixed destination range; retries must never append to a new location.
    pub range: String,
    pub rows: Vec<Vec<Value>>,
}
impl ExportBatch {
    pub fn validate_request(&self) -> Result<()> {
        ensure!(
            self.range.len() <= 256
                && !self.rows.is_empty()
                && self.rows.len() <= 1000
                && self.rows.iter().all(|row| !row.is_empty() && row.len() <= 128),
            "export batch is unbounded"
        );
        ensure!(
            serde_json::to_vec(self)?.len() <= 1_048_576,
            "export batch exceeds 1 MiB"
        );
        Ok(())
    }
    pub fn validate(&self) -> Result<()> {
        self.validate_request()?;
        ensure!(
            !self.key.is_empty() && self.key.len() <= 512,
            "export business key required"
        );
        Ok(())
    }
}
