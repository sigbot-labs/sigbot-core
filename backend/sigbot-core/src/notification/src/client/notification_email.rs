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
use sigbot_types::modules::notification::notification::NotificationInfo;
use std::sync::Arc;

#[derive(Clone)]
pub struct SigbotEmailClientConfig {
    pub id: i64,
    pub smtp_server: String,
    pub smtp_port: u16,
    pub smtp_username: String,
    pub smtp_password: String,
}

impl std::fmt::Display for SigbotEmailClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SigbotEmailConfig=(
                id={}, smtp_server={:?}, smtp_port={:?}, smtp_username={:?}, smtp_password={:?}),
            )
            ",
            self.id, self.smtp_server, self.smtp_port, self.smtp_username, self.smtp_password,
        )
    }
}

impl SigbotEmailClientConfig {
    pub fn from_notification(notification: NotificationInfo) -> Self {
        let plain_config = notification.configuration.expect("Plain configuration is required");
        let secret_config = notification.secrets.expect("Secret configuration is required");
        Self {
            id: notification.base.id.expect("Notification ID is required"),
            smtp_server: plain_config
                .get("smtp_server")
                .expect("SMTP server is required")
                .to_string(),
            smtp_port: plain_config
                .get("smtp_port")
                .expect("SMTP port is required")
                .to_string()
                .parse::<u16>()
                .expect("SMTP port must be a valid number"),
            smtp_username: plain_config
                .get("smtp_username")
                .expect("SMTP username is required")
                .to_string(),
            smtp_password: secret_config
                .get("smtp_password")
                .expect("SMTP password is required")
                .to_string(),
        }
    }
}

pub struct SigbotEmailClient {
    config: SigbotEmailClientConfig,
    // email_client: Arc<Mutex<Option<EmailClient>>>,
}

impl SigbotEmailClient {
    pub const KIND: &'static str = "EMAIL"; // NotificationKind::EMAIL

    pub async fn new(config: &SigbotEmailClientConfig) -> Arc<Self> {
        Arc::new(Self {
            config: config.to_owned(),
            // email_client: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl ISigbotNotificationClient for SigbotEmailClient {
    async fn init(&self) {
        info!("Starting Email notification with config={}", self.config);
        unimplemented!();
    }

    async fn close(&self) {
        info!("Closing Email notification with {}", self.config);
        unimplemented!();
    }

    async fn send_message(&self, to: &str, message: &str) -> Result<String, Error> {
        info!("Sending email to {} with message={}", to, message);
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {}
