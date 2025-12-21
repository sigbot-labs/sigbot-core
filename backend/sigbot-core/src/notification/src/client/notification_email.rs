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
use lettre::{
    message::{header::ContentType, Mailbox, MessageBuilder},
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Tokio1Executor,
};
use sigbot_types::modules::notification::notification::{NotificationInfo, NotificationProvider};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SigbotEmailClientConfig {
    pub id: i64,
    pub smtp_server: String,
    pub smtp_port: u16,
    pub smtp_username: String,
    pub smtp_password: String,
    pub from_address: String,
    pub default_to_addresses: Option<Vec<String>>,
    // pub default_cc_addresses: Option<Vec<String>>,
    // pub default_bcc_addresses: Option<Vec<String>>,
}

impl std::fmt::Display for SigbotEmailClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(id={}, smtp_server={:?}, smtp_port={:?}, smtp_username={:?}, smtp_password_len={:?})",
            self.id,
            self.smtp_server,
            self.smtp_port,
            self.smtp_username,
            self.smtp_password.chars().count(),
        )?;
        Ok(())
    }
}

impl SigbotEmailClientConfig {
    pub fn from_config(notification: Arc<NotificationInfo>) -> Arc<Self> {
        let plain_config = notification
            .as_ref()
            .to_owned()
            .configuration
            .expect("Plain configuration is required")
            .to_owned();
        let secret_config = notification
            .as_ref()
            .to_owned()
            .secrets
            .expect("Secret configuration is required")
            .to_owned();

        Arc::new(Self {
            id: notification
                .as_ref()
                .to_owned()
                .base
                .id
                .expect("Notification ID is required"),
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
            from_address: plain_config
                .get("from_address")
                .expect("From email address is required")
                .to_string(),
            default_to_addresses: plain_config
                .get("default_to_addresses")
                .map(|s| {
                    s.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<String>>()
                })
                .filter(|v| !v.is_empty()),
        })
    }
}

pub struct SigbotEmailClient {
    config: Arc<SigbotEmailClientConfig>,
    smtp_transport: Arc<Mutex<Option<AsyncSmtpTransport<Tokio1Executor>>>>,
}

impl SigbotEmailClient {
    pub async fn new(config: Option<Arc<SigbotEmailClientConfig>>) -> Arc<Self> {
        Arc::new(Self {
            config: config.expect("Config is required"),
            smtp_transport: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl ISigbotNotificationClient for SigbotEmailClient {
    fn provider(&self) -> NotificationProvider {
        NotificationProvider::EMAIL
    }

    async fn init(&self) {
        info!("Starting Email notification with config={}", self.config);

        let creds = Credentials::new(self.config.smtp_username.clone(), self.config.smtp_password.clone());

        match AsyncSmtpTransport::<Tokio1Executor>::relay(&self.config.smtp_server) {
            Ok(builder) => {
                let mailer = builder.port(self.config.smtp_port).credentials(creds).build();

                *self.smtp_transport.lock().await = Some(mailer);
                info!(
                    "Initialized Email notification with smtp_server={}, smtp_port={}",
                    self.config.smtp_server, self.config.smtp_port
                );
            }
            Err(e) => {
                warn!(
                    "Failed to create SMTP relay for server {}: {}",
                    self.config.smtp_server, e
                );
            }
        }
    }

    async fn close(&self) {
        info!("Closing Email notification with {}", self.config);
        let mut guard = self.smtp_transport.lock().await;
        *guard = None;
        info!("Closed Email notification with {}", self.config);
    }

    async fn send_simple_message(&self, to: Option<Vec<String>>, message: &str) -> Result<String, Error> {
        info!("Sending email to {:?} with message={}", to, message);

        let guard = self.smtp_transport.lock().await;
        let mailer = guard
            .as_ref()
            .context("SMTP transport not initialized. Please call init() first.")?;

        let from_mailbox: Mailbox = self.config.from_address.parse().context(format!(
            "Failed to parse from email address: {}",
            self.config.from_address
        ))?;

        // Use provided addresses or fall back to configured addresses
        let to_addresses = to
            .or_else(|| self.config.default_to_addresses.clone())
            .context("To email address is required")?;

        if to_addresses.is_empty() {
            return Err(Error::msg("At least one recipient email address is required"));
        }

        // Parse all recipient addresses
        let mut to_mailboxes = Vec::new();
        for addr in &to_addresses {
            let mailbox: Mailbox = addr
                .parse()
                .context(format!("Failed to parse to email address: {}", addr))?;
            to_mailboxes.push(mailbox);
        }

        // Build email with multiple recipients
        let mut email_builder = MessageBuilder::new()
            .from(from_mailbox)
            .subject("Sigbot Notification")
            .header(ContentType::TEXT_PLAIN);

        // Add all recipients before building the message
        for mailbox in to_mailboxes {
            email_builder = email_builder.to(mailbox);
        }

        let email = email_builder
            .body(message.to_string())
            .context("Failed to build email message")?;

        mailer
            .send(email)
            .await
            .context(format!("Failed to send email to {:?}", to_addresses))?;

        info!("Successfully sent email to {:?}", to_addresses);
        Ok(format!("Email sent to {:?}", to_addresses))
    }
}

#[cfg(test)]
mod tests {}
