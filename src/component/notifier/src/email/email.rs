// SPDX-License-Identifier: GPL-3.0-only

use crate::notifier::INotifier;
use crate::notifier::NotificationDeliveryError;
use anyhow::Context;
use anyhow::Error;
use async_trait::async_trait;
use common_context::IProvider;
use common_telemetry::info;
use common_telemetry::warn;
use lettre::message::header::ContentType;
use lettre::message::Mailbox;
use lettre::message::MessageBuilder;
use lettre::transport::smtp::authentication::Credentials;
use lettre::AsyncSmtpTransport;
use lettre::AsyncTransport;
use lettre::Tokio1Executor;
use sigbot_types::notifier::notification::NotificationInfo;
use sigbot_types::notifier::notification::NotificationProvider;
use sigbot_types::Uuid;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct EmailNotifierConfig {
    pub id: Uuid,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_username: String,
    pub smtp_password: String,
    pub from_address: String,
    pub default_to_addresses: Option<Vec<String>>,
    // pub default_cc_addresses: Option<Vec<String>>,
    // pub default_bcc_addresses: Option<Vec<String>>,
}

impl EmailNotifierConfig {
    pub fn from_config(notification: Arc<NotificationInfo>) -> Arc<Self> {
        let plain_config = notification
            .as_ref()
            .to_owned()
            .properties
            .expect("Plain configuration is required")
            .clone();
        let secret_config = notification
            .as_ref()
            .to_owned()
            .secrets
            .expect("Secret configuration is required")
            .clone();

        Arc::new(Self {
            id: notification
                .as_ref()
                .to_owned()
                .base
                .id
                .expect("Notification ID is required"),
            smtp_host: plain_config.get("smtp_host").expect("smtp_host is required").clone(),
            smtp_port: plain_config
                .get("smtp_port")
                .expect("smtp_port is required")
                .clone()
                .parse::<u16>()
                .expect("smtp_port must be a valid number"),
            smtp_username: plain_config
                .get("smtp_username")
                .expect("smtp_username is required")
                .clone(),
            smtp_password: secret_config
                .get("smtp_password")
                .expect("smtp_password is required")
                .clone(),
            from_address: plain_config
                .get("from_address")
                .expect("from_address is required")
                .clone(),
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

pub struct EmailNotifier {
    config: Arc<EmailNotifierConfig>,
    smtp_transport: Arc<Mutex<Option<AsyncSmtpTransport<Tokio1Executor>>>>,
}

impl EmailNotifier {
    pub async fn new(config: Option<Arc<EmailNotifierConfig>>) -> Arc<Self> {
        Arc::new(Self {
            config: config.expect("Config is required"),
            smtp_transport: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl INotifier for EmailNotifier {
    fn provider(&self) -> NotificationProvider {
        NotificationProvider::Email
    }

    async fn send_simple_message(&self, to: Option<Vec<String>>, message: &str) -> Result<String, Error> {
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

        if to_addresses.len() != 1 {
            return Err(Error::msg("One email recipient is required per durable delivery"));
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

        let response = mailer.send(email).await.map_err(|error| {
            if error.is_transient() {
                NotificationDeliveryError::Retryable
            } else if error.is_permanent() {
                NotificationDeliveryError::Rejected
            } else {
                NotificationDeliveryError::Unknown
            }
        })?;
        Ok(response.code().to_string())
    }
}

#[async_trait::async_trait]
impl IProvider for EmailNotifier {
    async fn close(&self) -> anyhow::Result<()> {
        info!("Closing Email notification provider");
        let mut guard = self.smtp_transport.lock().await;
        *guard = None;
        info!("Closed Email notification provider");

        Ok(())
    }
    async fn init(&self) -> anyhow::Result<()> {
        info!("Starting Email notification provider");

        let creds = Credentials::new(self.config.smtp_username.clone(), self.config.smtp_password.clone());

        match AsyncSmtpTransport::<Tokio1Executor>::relay(&self.config.smtp_host) {
            Ok(builder) => {
                let mailer = builder
                    .port(self.config.smtp_port)
                    .credentials(creds)
                    .timeout(Some(std::time::Duration::from_secs(20)))
                    .build();

                *self.smtp_transport.lock().await = Some(mailer);
                info!(
                    "Initialized Email notification with smtp_host={}, smtp_port={}",
                    self.config.smtp_host, self.config.smtp_port
                );
            }
            Err(e) => {
                warn!("Failed to create SMTP relay for host {}: {}", self.config.smtp_host, e);
            }
        }

        Ok(())
    }
}
