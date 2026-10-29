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

use crate::client::notification_factory::{ISigbotNotificationClient, NotificationDeliveryError};
use anyhow::{ensure, Context, Error};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use sigbot_types::modules::notification::notification::{NotificationInfo, NotificationProvider};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SigbotTelegramConfig {
    pub id: i64,
    pub telegram_bot_token: String,
    pub default_telegram_chat_id: String,
}
impl SigbotTelegramConfig {
    pub fn from_config(notification: Arc<NotificationInfo>) -> Arc<Self> {
        let properties = notification.properties.as_ref().expect("Telegram properties required");
        let secrets = notification.secrets.as_ref().expect("Telegram secrets required");
        Arc::new(Self {
            id: notification.base.id.expect("notification node ID required"),
            telegram_bot_token: secrets
                .get("telegram_bot_token")
                .expect("Telegram token must come from Secret")
                .clone(),
            default_telegram_chat_id: properties.get("default_telegram_chat_id").cloned().unwrap_or_default(),
        })
    }
}
pub struct SigbotTelegramClient {
    config: Arc<SigbotTelegramConfig>,
    http_client: Mutex<Option<Client>>,
}
impl SigbotTelegramClient {
    pub async fn new(config: Option<Arc<SigbotTelegramConfig>>) -> Arc<Self> {
        Arc::new(Self {
            config: config.expect("Telegram configuration required"),
            http_client: Mutex::new(None),
        })
    }
}
#[async_trait]
impl ISigbotNotificationClient for SigbotTelegramClient {
    fn provider(&self) -> NotificationProvider {
        NotificationProvider::TELEGRAM
    }
    async fn init(&self) {
        *self.http_client.lock().await = Some(
            Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("Telegram HTTP client configuration failed"),
        );
    }
    async fn close(&self) {
        self.http_client.lock().await.take();
    }
    async fn send_simple_message(&self, to: Option<Vec<String>>, message: &str) -> Result<String, Error> {
        let recipients = to.unwrap_or_else(|| vec![self.config.default_telegram_chat_id.clone()]);
        ensure!(
            recipients.len() == 1 && !recipients[0].is_empty(),
            "one Telegram recipient is required per durable delivery"
        );
        ensure!(message.chars().count() <= 4096, "Telegram message is too long");
        let client = self
            .http_client
            .lock()
            .await
            .as_ref()
            .cloned()
            .context("Telegram client is not initialized")?;
        let response = client
            .post(format!(
                "https://api.telegram.org/bot{}/sendMessage",
                self.config.telegram_bot_token
            ))
            .json(&json!({"chat_id":recipients[0],"text":message}))
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() {
                    NotificationDeliveryError::Retryable
                } else {
                    NotificationDeliveryError::Unknown
                }
            })?;
        let status = response.status();
        let payload: serde_json::Value = response.json().await.map_err(|_| NotificationDeliveryError::Unknown)?;
        if status.is_success() && payload.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
            return payload
                .pointer("/result/message_id")
                .map(ToString::to_string)
                .context("Telegram accepted without a message receipt");
        }
        if payload.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
            let code = payload
                .get("error_code")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(status.as_u16().into());
            return Err(if code == 429 || code >= 500 {
                NotificationDeliveryError::Retryable
            } else {
                NotificationDeliveryError::Rejected
            }
            .into());
        }
        Err(NotificationDeliveryError::Unknown.into())
    }
}
