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
use common_telemetry::{debug, info};
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use std::sync::{Arc, OnceLock};

static QUERY_RUNTIME: OnceLock<tokio::sync::Mutex<Option<Arc<super::market_query::MarketQueryRuntime>>>> =
    OnceLock::new();

pub struct SigbotDatafeedIngestor {}

impl SigbotDatafeedIngestor {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        common_telemetry::readiness::RuntimeReadiness::business("datafeed", false);
        debug!("Initializing Datafeed clients.");
        let (datafeeds, argument) = SigbotDatafeedClientFactory::init(matches, verbose)
            .await
            .expect("Failed to initialize Datafeed clients.");
        info!("Initialized Datafeed clients. {}", datafeeds.len());

        debug!("Initializing Messager client.");
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config.to_owned())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {}", messager.provider().as_str());

        let properties = argument
            .datafeed_config
            .properties
            .as_ref()
            .expect("datafeed properties required");
        if properties.contains_key("snapshot_input_node_ids") {
            let query = super::market_query::MarketQueryRuntime::from_properties(properties)
                .await
                .expect("failed to initialize bounded market query runtime");
            query
                .start(messager.clone())
                .await
                .expect("failed to subscribe market snapshot requests");
            *QUERY_RUNTIME.get_or_init(Default::default).lock().await = Some(query);
        }
        for datafeed in datafeeds.iter() {
            let provider = datafeed.provider().as_str().to_lowercase();
            let ingestor = Arc::new(
                super::persisted_ingestor::PersistedDatafeedIngestor::from_properties(properties, &provider)
                    .await
                    .expect("failed to initialize durable datafeed storage"),
            );
            ingestor
                .start_relay(messager.clone())
                .await
                .expect("failed to start durable feed outbox relay");
            let handler: crate::client::news::social_feed::FeedHandler = Arc::new(move |data| {
                let ingestor = ingestor.clone();
                Box::pin(async move {
                    ingestor.ingest(&data).await?;
                    Ok(data)
                })
            });
            datafeed
                .subscribe(handler)
                .await
                .expect("failed to subscribe datafeed source");
        }
        info!("Subscribed to Datafeed clients.");
        common_telemetry::readiness::RuntimeReadiness::business("datafeed", true);
    }

    pub async fn shutdown() {
        common_telemetry::readiness::RuntimeReadiness::business("datafeed", false);
        if let Some(query) = QUERY_RUNTIME.get_or_init(Default::default).lock().await.take() {
            query.close().await;
        }
        info!("Shutting down Datafeed clients.");
        SigbotDatafeedClientFactory::shutdown().await;
        info!("Shutdown Datafeed clients.");

        info!("Shutting down Messager client.");
        SigbotMessagerClientFactory::shutdown().await;
        info!("Shutdown Messager client.");
    }
}

#[cfg(test)]
mod tests {}
