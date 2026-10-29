// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::manager::exporter_factory::ISigbotExporterManager;
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use sigbot_messager::client::messager_factory::ISigbotMessagerClient;
use sigbot_types::modules::{
    exporter::{ExporterMgrProvider, SigbotExporterManagerArgument},
    messager::TOPIC_WF_TRADING_PLACED,
    order::events::SigbotTradeEvent,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Google Sheets writer configuration
#[derive(Clone, Debug)]
pub struct GoogleSheetsWriterConfig {
    pub spreadsheet_id: Option<String>,
    pub secrets: Option<HashMap<String, String>>,
    pub properties: Option<HashMap<String, String>>,
}

/// Google Sheets writer using deterministic range replacement for retry safety.
pub struct GoogleSheetsWriter {
    sink: Arc<dyn super::sink::IExportSink>,
    range: String,
}

impl GoogleSheetsWriter {
    pub fn new(config: GoogleSheetsWriterConfig) -> Result<Self, Error> {
        let properties = config.properties.unwrap_or_default();
        let range = properties
            .get("range")
            .cloned()
            .ok_or_else(|| Error::msg("deterministic Sheets range required"))?;
        let token_env = properties
            .get("token_env")
            .cloned()
            .ok_or_else(|| Error::msg("Sheets token_env required"))?;
        let spreadsheet_id = config
            .spreadsheet_id
            .ok_or_else(|| Error::msg("spreadsheet_id required"))?;
        Ok(Self {
            sink: super::sink::ExportSinkFactory::create(&super::sink::ExportSinkConfig::GoogleSheets {
                spreadsheet_id,
                token_env,
            })?,
            range,
        })
    }

    async fn write_to_sheets(&self, data: Vec<u8>) -> Result<(), Error> {
        let event: SigbotTradeEvent = serde_json::from_slice(&data)?;
        self.sink
            .write(&super::sink::ExportBatch {
                key: event.idempotency_key(),
                range: self.range.clone(),
                rows: vec![vec![serde_json::Value::String(serde_json::to_string(&event)?)]],
            })
            .await
    }
}

#[derive(Clone)]
pub struct SigbotGoogleSheetsExporterManager {
    writer: Arc<Mutex<Option<GoogleSheetsWriter>>>,
    messager_client: Arc<Mutex<Option<Arc<dyn ISigbotMessagerClient>>>>,
}

impl SigbotGoogleSheetsExporterManager {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            writer: Arc::new(Mutex::new(None)),
            messager_client: Arc::new(Mutex::new(None)),
        })
    }

    async fn export_data_internal(&self, data: Vec<u8>) -> Result<(), Error> {
        // Parse and validate SigbotTradeEvent
        let trade_event: SigbotTradeEvent = serde_json::from_slice(&data)
            .map_err(|e| Error::msg(format!("Failed to deserialize SigbotTradeEvent: {}", e)))?;

        debug!(
            "GoogleSheets exporter processing trade_event: trade_id={}, tenant_id={}",
            trade_event.trade_id, trade_event.tenant_id
        );

        // Write to Google Sheets (batch mode for now)
        let guard = self.writer.lock().await;
        if let Some(ref writer) = *guard {
            writer.write_to_sheets(data).await?;
        } else {
            return Err(Error::msg("Google Sheets writer not configured"));
        }

        Ok(())
    }
}

#[async_trait]
impl ISigbotExporterManager for SigbotGoogleSheetsExporterManager {
    fn provider(&self) -> ExporterMgrProvider {
        ExporterMgrProvider::GOOGLESHEETS
    }

    async fn init(&self, argument: Arc<SigbotExporterManagerArgument>) {
        debug!("Initializing GoogleSheets Exporter manager");

        // Parse Google Sheets configuration from argument
        let config = GoogleSheetsWriterConfig {
            spreadsheet_id: argument
                .properties
                .as_ref()
                .and_then(|p| p.get("spreadsheet_id").cloned()),
            secrets: argument.secrets.clone(),
            properties: argument.properties.clone(),
        };

        let writer = GoogleSheetsWriter::new(config).expect("invalid Sheets exporter configuration");
        *self.writer.lock().await = Some(writer);

        info!("GoogleSheets Exporter manager initialized");
    }

    async fn close(&self) {
        info!("Shutting down GoogleSheets Exporter manager");
    }

    async fn subscribe(&self, messager: Arc<dyn ISigbotMessagerClient>) -> Result<(), Error> {
        // Store messager client
        *self.messager_client.lock().await = Some(messager.clone());

        // Subscribe to trading placed events (orders) - this exporter handles order data
        let topic = TOPIC_WF_TRADING_PLACED
            .replace("{TENANT_ID}", "+")
            .replace("{WORKFLOW_ID}", "+");

        let manager = self.clone();
        let handler: Arc<
            dyn Fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, Error>> + Send>>
                + Send
                + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let manager_clone = manager.clone();
            Box::pin(async move {
                // Export to Google Sheets
                manager_clone.export_data_internal(data.clone()).await?;
                Ok(String::from_utf8_lossy(&data).to_string())
            })
        });

        messager.subscribe(&topic, handler).await?;
        info!("GoogleSheets Exporter subscribed to topic: {}", topic);

        Ok(())
    }

    async fn export_batch(&self, data: Vec<u8>) -> Result<(), Error> {
        self.export_data_internal(data).await
    }

    async fn export_stream(&self, data: Vec<u8>) -> Result<(), Error> {
        self.export_data_internal(data).await
    }
}

#[cfg(test)]
mod tests {}
