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

use crate::client::datafeed_factory::SigbotDatafeedClientFactory;
use anyhow::{Context, Error};
use common_telemetry::info;
use sigbot_messaging::client::messaging_factory::SigbotMessagingClientFactory;
use sigbot_types::modules::messaging::TOPIC_MARKET_DATA;
use std::{future::Future, pin::Pin, sync::Arc};

pub struct SigbotDatafeedIngestor {}

impl SigbotDatafeedIngestor {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        info!("Initializing Datafeed clients.");
        let (datafeeds, argument) = SigbotDatafeedClientFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Datafeed clients.");
        info!("Initialized Datafeed clients. {:?}", datafeeds.len());

        info!("Initializing Messaging client.");
        let messaging = SigbotMessagingClientFactory::init(matches, argument.messaging_config.to_owned())
            .await
            .expect("Failed to initialize Messaging client.");
        info!("Initialized Messaging client. {:?}", messaging.name());

        // TODO: Publish the datafeed data to messaging topics.
        let handler: Arc<
            dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync,
        > = Arc::new(move |data: Vec<u8>| {
            let messaging0 = messaging.to_owned();
            Box::pin(async move {
                let data0 = String::from_utf8(data.clone())
                    .context("Failed to convert data to string.")
                    .unwrap_or_default();
                info!("Received data: {:?}", data0);
                messaging0
                    .publish(TOPIC_MARKET_DATA, &data0)
                    .await
                    .context("Failed to publish data to messaging topic.")?;
                Ok(data)
            })
        });
        for datafeed in datafeeds.iter() {
            datafeed.subscribe(handler.to_owned()).await;
        }
        info!("Subscribed to Datafeed clients.");
    }

    pub async fn shutdown() {
        info!("Shutting down Datafeed clients.");
        SigbotDatafeedClientFactory::close().await;
        info!("Shutting down Datafeed clients.");

        info!("Shutting down Messaging client.");
        SigbotMessagingClientFactory::close().await;
        info!("Shutting down Messaging client.");
    }
}

#[cfg(test)]
mod tests {}
