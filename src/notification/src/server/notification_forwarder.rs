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

use crate::client::notification_factory::SigbotNotificationClientFactory;
use anyhow::{Context, Error};
use common_telemetry::{debug, info};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use sigbot_types::modules::messager::TOPIC_NOTIFICATION_MESSAGES;
use std::{future::Future, pin::Pin, sync::Arc};

pub struct SigbotNotificationForwarder {
    // TODO: email client.
    // TODO: telegram client.
}

impl SigbotNotificationForwarder {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        debug!("Initializing Notification clients.");
        let (notifications, argument) = SigbotNotificationClientFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Notification clients.");
        info!("Initialized Notification clients.");

        debug!("Initializing Messager client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");

        let handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<String, Error>> + Send>> + Send + Sync> =
            Arc::new(move |data: Vec<u8>| {
                info!("Received message: {:?}", data);
                let notifications0 = notifications.to_owned();
                Box::pin(async move {
                    // TODO: improvement to the content message parsing mechanism.
                    let message = String::from_utf8_lossy(&data);
                    for notification in notifications0.iter() {
                        info!("Sending message: {:?} : {}", notification.provider(), &message);
                        // TODO: improvement to the sent result confirmation mechanism.
                        let res = notification
                            .send_simple_message(None, &message)
                            .await
                            .context("Failed to send message.")?;
                        info!(
                            "Sent message to provider: {:?}, result: {:?}.",
                            notification.provider(),
                            res
                        );
                    }
                    Ok("OK".to_string())
                })
            });

        let topic = TOPIC_NOTIFICATION_MESSAGES.replace("{tenant_id}", "+"); // MQTT single level wildcard
        let _ = messager
            .subscribe(&topic, handler) // TODO: configuable
            .await
            .context("Failed to subscribe to the messager topic.");
        info!("Initialized Messager client with provider: {:?}.", messager.provider());
    }

    pub async fn shutdown() {
        info!("Shutting down Notification clients.");
        SigbotNotificationClientFactory::close().await;
        info!("Shutting down Notification clients.");

        info!("Shutting down Messager client.");
        SigbotMessagerClientFactory::close().await;
        info!("Shutting down Messager client.");
    }
}

#[cfg(test)]
mod tests {}
