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
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use sigbot_types::modules::notification::notification::{NotificationInfo, NotificationProvider};
use std::sync::Arc;

#[derive(Clone)]
pub struct SigbotTelegramConfig {
    pub id: i64,
    pub telegram_bot_token: String,
    pub telegram_chat_id: String,
}

impl std::fmt::Display for SigbotTelegramConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SigbotTelegramConfig=(
                id={}, telegram_bot_token={:?}, telegram_chat_id={:?},
            )",
            self.id, self.telegram_bot_token, self.telegram_chat_id,
        )?;
        Ok(())
    }
}

impl SigbotTelegramConfig {
    pub fn from_notification(notification: NotificationInfo) -> Self {
        let plain_config = notification.configuration.expect("Plain configuration is required");
        Self {
            id: notification.base.id.expect("Notification ID is required"),
            telegram_bot_token: plain_config
                .get("telegram_bot_token")
                .expect("Telegram bot token is required")
                .to_string(),
            telegram_chat_id: plain_config
                .get("telegram_chat_id")
                .expect("Telegram chat ID is required")
                .to_string(),
        }
    }
}

pub struct SigbotTelegramClient {
    config: Arc<SigbotTelegramConfig>,
    // telegram_client: Arc<Mutex<Option<TelegramClient>>>,
}

impl SigbotTelegramClient {
    pub async fn new(config: Option<Arc<SigbotTelegramConfig>>) -> Arc<Self> {
        Arc::new(Self {
            config: config.expect("Config is required"),
            // telegram_client: Arc::new(Mutex::new(None)),
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
        unimplemented!();
    }

    async fn close(&self) {
        info!("Closing Telegram notification with {}", self.config);
        unimplemented!();
    }

    async fn send_message(&self, to: &str, message: &str) -> Result<String, Error> {
        info!("Sending Telegram to {} with message={}", to, message);
        unimplemented!();
    }
}
