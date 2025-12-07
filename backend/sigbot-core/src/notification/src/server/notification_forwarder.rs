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
use common_telemetry::info;
use sigbot_messaging::client::messaging_factory::SigbotMessagingClientFactory;
use std::{future::Future, pin::Pin, sync::Arc};

pub struct SigbotNotificationForwarderServer {
    // TODO: email client.
    // TODO: telegram client.
}

impl SigbotNotificationForwarderServer {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        info!("Initializing Notification clients.");
        let notifications = SigbotNotificationClientFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Notification clients.");
        info!("Initialized Notification clients.");

        info!("Initializing Messaging client.");
        let messaging = SigbotMessagingClientFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Messaging client.");

        // TODO: Subscribe to the messaging from topics.
        let handler: Arc<
            dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let notifications0 = notifications.to_owned();
            Box::pin(async move {
                info!("Received message: {:?}", data);
                for notification in notifications0.iter() {
                    // TODO: Forwarding message via notification client.
                    let res = notification
                        .send_message("test@example.com", "Hello, world!")
                        .await
                        .context("Failed to send message.")?;
                    info!("Sent notification message: {:?}.", res);
                }
                Ok(data)
            })
        });
        let _ = messaging
            .subscribe("sigbot/notification/alarm", handler) // TODO: configuable
            .await
            .context("Failed to subscribe to the messaging topic.");
        info!("Initialized Messaging client with provider: {:?}.", messaging.name());
    }

    pub async fn shutdown() {
        info!("Shutting down Notification clients.");
        SigbotNotificationClientFactory::close().await;
        info!("Shutting down Notification clients.");

        info!("Shutting down Messaging client.");
        SigbotMessagingClientFactory::close().await;
        info!("Shutting down Messaging client.");
    }
}

#[cfg(test)]
mod tests {}
