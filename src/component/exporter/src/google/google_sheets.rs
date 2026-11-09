// SPDX-License-Identifier: GPL-3.0-only

use crate::exporter::ExportBatch;
use crate::exporter::IExporter;
use anyhow::ensure;
use anyhow::Result;
use async_trait::async_trait;
use common_context::IProvider;
use serde_json::json;
use serde_json::Value;
use sigbot_core::config::config::get_config;
use sigbot_types::exporter::ExporterProvider;
use std::sync::Arc;
use std::time::Duration;

pub struct GoogleSheetsExporter {
    spreadsheet_id: String,
    token: String,
    client: reqwest::Client,
}
impl GoogleSheetsExporter {
    pub fn new(spreadsheet_id: &str, token_secret: &str, proxy_url: Option<&str>) -> Result<Arc<Self>> {
        ensure!(
            !spreadsheet_id.is_empty()
                && spreadsheet_id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-'),
            "invalid spreadsheet ID"
        );
        let token = get_config().secrets.require(token_secret)?.to_owned();
        let mut client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(proxy_url) = proxy_url.filter(|value| !value.trim().is_empty()) {
            client = client.proxy(reqwest::Proxy::all(proxy_url)?);
        }
        Ok(Arc::new(Self {
            spreadsheet_id: spreadsheet_id.to_owned(),
            token,
            client: client.build()?,
        }))
    }
}
#[async_trait]
impl IExporter for GoogleSheetsExporter {
    fn provider(&self) -> ExporterProvider {
        ExporterProvider::GoogleSheets
    }
    async fn write(&self, batch: &ExportBatch) -> Result<()> {
        batch.validate()?;
        ensure!(
            !batch.range.is_empty() && batch.range.len() <= 256,
            "deterministic Google Sheets range required"
        );
        let body = json!({"range": batch.range, "majorDimension": "ROWS", "values": batch.rows});
        let mut endpoint = url::Url::parse("https://sheets.googleapis.com/v4/spreadsheets/")?;
        endpoint
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid Sheets base URL"))?
            .pop_if_empty()
            .push(&self.spreadsheet_id)
            .push("values")
            .push(&batch.range);
        let response = self
            .client
            .put(endpoint)
            .bearer_auth(&self.token)
            .query(&[("valueInputOption", "RAW")])
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        let receipt: Value = response.json().await?;
        ensure!(
            receipt.get("updatedRows").and_then(Value::as_u64) == Some(batch.rows.len() as u64),
            "Sheets returned an incomplete write"
        );
        Ok(())
    }
}
#[async_trait]
impl IProvider for GoogleSheetsExporter {
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
