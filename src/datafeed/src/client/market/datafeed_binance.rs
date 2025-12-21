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

use crate::client::datafeed_factory::ISigbotDatafeedClient;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{debug, info};
use sigbot_types::modules::datafeed::datafeed::DatafeedProvider;
use sigbot_types::modules::{datafeed::SigbotDatefeedArgument, exchange::exchange::ExchangeInfo};
use std::{future::Future, pin::Pin, sync::Arc};

#[derive(Clone)]
pub struct SigbotBinanceDatafeedClient {
    // config: Arc<DatafeedInfo>,
}

impl SigbotBinanceDatafeedClient {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ISigbotDatafeedClient for SigbotBinanceDatafeedClient {
    fn provider(&self) -> DatafeedProvider {
        DatafeedProvider::BINANCE
    }

    async fn init(&self, argument: Arc<SigbotDatefeedArgument>) {
        debug!("Initializing Binance Datafeed ...");
        // TODO: Implement the logic to execute Binance market gateway.
        // TODO: 1. Start the Binance market websocket subscription and pushing to EMQx(hot data cache).
        // TODO: 2. Start the consumer to market data to database(cold data persist) from EMQx.

        // TODO: create from datafeed configuration.
        let _ = Arc::new(ExchangeInfo::default());

        let kline_params = argument
            .datafeed_config
            .properties
            .as_ref()
            .context("Datafeed config is required")
            .map(|config| {
                (
                    config
                        .get("symbol")
                        .context("Symbol is required")
                        .map(|s| s.clone())
                        .unwrap_or(String::from("BTCUSDC")),
                    config
                        .get("interval")
                        .context("Interval is required")
                        .map(|s| s.clone())
                        .unwrap_or(String::from("1m")),
                )
            })
            .expect("Failed to get kline params from datafeed config");

        // let exchange_client = SigbotExchangeClientFactory::init(exchange)
        //     .await
        //     .expect("Failed to initialize Exchange client.");
        // let klines = exchange_client
        //     .get_klines(&kline_params.0, &kline_params.1, None, None, 100)
        //     .await
        //     .expect("Failed to get klines");

        // info!("Klines: {:?}", klines);

        info!("Initialized Binance Datafeed.");
    }

    async fn close(&self) {
        info!("Shut down Binance Datafeed.");
    }

    async fn subscribe(
        &self,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>> + Send + Sync>,
    ) {
        info!("Subscribed to Binance Datafeed with handler.");
    }
}
