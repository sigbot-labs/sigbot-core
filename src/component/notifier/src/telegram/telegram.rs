// SPDX-License-Identifier: GPL-3.0-only

use crate::notifier::INotifier;
use crate::notifier::NotificationDeliveryError;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Error;
use async_trait::async_trait;
use common_context::IProvider;
use reqwest::Client;
use serde_json::json;
use sigbot_types::notifier::notification::NotificationInfo;
use sigbot_types::notifier::notification::NotificationProvider;
use sigbot_types::Uuid;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct TelegramNotifierConfig {
    pub id: Uuid,
    pub telegram_bot_token: String,
    pub default_telegram_chat_id: String,
}
impl TelegramNotifierConfig {
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
pub struct TelegramNotifier {
    config: Arc<TelegramNotifierConfig>,
    http_client: Mutex<Option<Client>>,
}
impl TelegramNotifier {
    pub async fn new(config: Option<Arc<TelegramNotifierConfig>>) -> Arc<Self> {
        Arc::new(Self {
            config: config.expect("Telegram configuration required"),
            http_client: Mutex::new(None),
        })
    }
}
#[async_trait]
impl INotifier for TelegramNotifier {
    fn provider(&self) -> NotificationProvider {
        NotificationProvider::Telegram
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

#[async_trait::async_trait]
impl IProvider for TelegramNotifier {
    async fn close(&self) -> anyhow::Result<()> {
        self.http_client.lock().await.take();

        Ok(())
    }
    async fn init(&self) -> anyhow::Result<()> {
        *self.http_client.lock().await = Some(
            Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("Telegram HTTP client configuration failed"),
        );

        Ok(())
    }
}
