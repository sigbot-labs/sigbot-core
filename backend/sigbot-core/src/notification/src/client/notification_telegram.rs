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

use crate::client::notification_factory::ISigbotNotificationClient;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{info, warn};
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

impl std::fmt::Display for SigbotTelegramConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(id={}, telegram_chat_id={:?}, telegram_bot_token_len={:?})",
            self.id,
            self.default_telegram_chat_id,
            self.telegram_bot_token.chars().count(),
        )?;
        Ok(())
    }
}

impl SigbotTelegramConfig {
    pub fn from_config(notification: Arc<NotificationInfo>) -> Arc<Self> {
        let plain_config = notification
            .as_ref()
            .to_owned()
            .properties
            .expect("Plain configuration is required")
            .to_owned();
        Arc::new(Self {
            id: notification
                .as_ref()
                .to_owned()
                .base
                .id
                .expect("Notification ID is required"),
            telegram_bot_token: plain_config
                .get("telegram_bot_token")
                .expect("Telegram bot token is required")
                .to_string(),
            default_telegram_chat_id: plain_config
                .get("default_telegram_chat_id")
                .expect("Telegram chat ID is required")
                .to_string(),
        })
    }
}

pub struct SigbotTelegramClient {
    config: Arc<SigbotTelegramConfig>,
    http_client: Arc<Mutex<Option<Client>>>,
}

impl SigbotTelegramClient {
    pub async fn new(config: Option<Arc<SigbotTelegramConfig>>) -> Arc<Self> {
        Arc::new(Self {
            config: config.expect("Config is required"),
            http_client: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl ISigbotNotificationClient for SigbotTelegramClient {
    fn provider(&self) -> NotificationProvider {
        NotificationProvider::TELEGRAM
    }

    async fn init(&self) {
        info!("Starting Telegram notification with config={}", self.config);

        match Client::builder()
            .connect_timeout(Duration::from_secs(10))
            // .read_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(20))
            .build()
        {
            Ok(client) => {
                *self.http_client.lock().await = Some(client);
                info!(
                    "Initialized Telegram notification with bot_token_len={}, chat_id={}",
                    self.config.telegram_bot_token.chars().count(),
                    self.config.default_telegram_chat_id
                );
            }
            Err(e) => {
                warn!("Failed to create HTTP client for Telegram: {}", e);
            }
        }
    }

    async fn close(&self) {
        info!("Closing Telegram notification with {}", self.config);
        let mut guard = self.http_client.lock().await;
        *guard = None;
        info!("Closed Telegram notification with {}", self.config);
    }

    async fn send_simple_message(&self, to: Option<Vec<String>>, message: &str) -> Result<String, Error> {
        info!("Sending Telegram to {:?} with message={}", to, message);

        let guard = self.http_client.lock().await;
        let client = guard
            .as_ref()
            .context("HTTP client not initialized. Please call init() first.")?;

        // Collect all chat_ids to send to
        let chat_ids: Vec<String> = if let Some(to_list) = to {
            if to_list.is_empty() {
                vec![self.config.default_telegram_chat_id.clone()]
            } else {
                to_list
            }
        } else {
            vec![self.config.default_telegram_chat_id.clone()]
        };

        if chat_ids.is_empty() {
            return Err(Error::msg("No Telegram chat IDs provided"));
        }

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.config.telegram_bot_token
        );

        let mut success_count = 0;
        let mut failed_chats = Vec::new();

        // Send message to each chat_id
        for chat_id in &chat_ids {
            let payload = json!({
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            });

            match client.post(&url).json(&payload).send().await {
                Ok(response) => {
                    if !response.status().is_success() {
                        let status = response.status();
                        let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                        warn!(
                            "Failed to send Telegram message to chat_id={}: status={}, error={}",
                            chat_id, status, error_text
                        );
                        failed_chats.push((chat_id.clone(), format!("HTTP {}: {}", status, error_text)));
                        continue;
                    }

                    match response.json::<serde_json::Value>().await {
                        Ok(response_json) => {
                            if let Some(ok) = response_json.get("ok") {
                                if ok.as_bool().unwrap_or(false) {
                                    info!("Successfully sent Telegram message to chat_id={}", chat_id);
                                    success_count += 1;
                                } else {
                                    let description = response_json
                                        .get("description")
                                        .and_then(|d| d.as_str())
                                        .unwrap_or("Unknown error");
                                    warn!("Telegram API returned error for chat_id={}: {}", chat_id, description);
                                    failed_chats.push((chat_id.clone(), description.to_string()));
                                }
                            } else {
                                warn!("Invalid Telegram API response format for chat_id={}", chat_id);
                                failed_chats.push((chat_id.clone(), "Invalid response format".to_string()));
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse Telegram API response for chat_id={}: {}", chat_id, e);
                            failed_chats.push((chat_id.clone(), format!("Parse error: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to send Telegram message to chat_id={}: {}", chat_id, e);
                    failed_chats.push((chat_id.clone(), format!("Request error: {}", e)));
                }
            }
        }

        // Return summary result
        if success_count == 0 {
            Err(Error::msg(format!(
                "Failed to send Telegram message to all {} chat(s). Errors: {:?}",
                chat_ids.len(),
                failed_chats
            )))
        } else if failed_chats.is_empty() {
            Ok(format!(
                "Successfully sent Telegram message to {} chat(s): {}",
                success_count,
                chat_ids.join(", ")
            ))
        } else {
            let success_chats: Vec<String> = chat_ids
                .iter()
                .filter(|id| !failed_chats.iter().any(|(failed_id, _)| failed_id == *id))
                .cloned()
                .collect();
            Ok(format!(
                "Sent Telegram message to {}/{} chat(s). Success: [{}], Failed: {:?}",
                success_count,
                chat_ids.len(),
                success_chats.join(", "),
                failed_chats
            ))
        }
    }
}
