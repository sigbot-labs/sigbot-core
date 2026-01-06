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

use crate::client::dex::ISigbotDEXExchangeClient;
use crate::client::exchange_factory::ISigbotExchangeClient;
use crate::client::{ISigbotOrderBookExchangeClient, KlineModelCacheWrapper};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::info;
use serde_json;
use sigbot_core::cache::{CacheContainer, ICache};
use sigbot_types::modules::exchange::exchange::ExchangeProvider;
use sigbot_types::{
    modules::exchange::exchange::ExchangeInfo,
    modules::exchange::models::{
        trade_market::{KlineModel, OrderInfo, PriceModel, SymbolInfo},
        trade_position::{PlaceTradeSignal, ExitTradePosition, TradeResult},
    },
};
use std::collections::HashMap;
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub struct SigbotHyperliquidConfig {
    pub id: i64,
    pub name: String,
    // Spot endpoints
    pub spot_api_mainnet_endpoint: String,
    pub spot_api_testnet_endpoint: String,
    pub spot_ws_mainnet_endpoint: String,
    pub spot_ws_testnet_endpoint: String,
    // Spot-market endpoints
    pub spot_market_api_mainnet_endpoint: String,
    pub spot_market_ws_mainnet_endpoint: String,
    // USDS-derivatives endpoints
    pub usds_api_mainnet_endpoint: String,
    pub usds_api_testnet_endpoint: String,
    pub usds_ws_mainnet_endpoint: String,
    pub usds_ws_testnet_endpoint: String,
    // USDS-derivatives-market endpoints
    pub usds_market_api_mainnet_endpoint: String,
    pub usds_market_ws_mainnet_endpoint: String,
    // Secrets configuration.
    pub api_key: String,
    pub api_secret: String,
    // Generic configuration.
    pub api_timeout: Duration,
    pub api_retries: u32,
    pub api_backoff: u32,
    pub use_websocket: bool,
    pub ws_timeout: Duration,
    pub ws_reconnect_delay: u64,
    pub ws_streams_max_concurrent: usize,
    pub description: Option<String>,
}

impl std::fmt::Display for SigbotHyperliquidConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SigbotHyperliquidConfig=(
                id={}, name={},
                spot_endpoints=(api_mainnet={}, api_testnet={}, ws_mainnet={}, ws_testnet={})),
                usds_endpoints=(api_mainnet={}, api_testnet={}, ws_mainnet={}, ws_testnet={})),
            )
            ",
            self.id,
            self.name,
            self.spot_api_mainnet_endpoint,
            self.spot_api_testnet_endpoint,
            self.spot_ws_mainnet_endpoint,
            self.spot_ws_testnet_endpoint,
            self.usds_api_mainnet_endpoint,
            self.usds_api_testnet_endpoint,
            self.usds_ws_mainnet_endpoint,
            self.usds_ws_testnet_endpoint,
        )
    }
}

impl SigbotHyperliquidConfig {
    pub fn from_exchange(exchange: Arc<ExchangeInfo>) -> Self {
        let plain_config = exchange.properties.as_ref().expect("Plain configuration is required");
        let secret_config = exchange.secrets.as_ref().expect("Secret configuration is required");
        Self {
            id: exchange.base.id.expect("Exchange ID is required"),
            name: exchange.name.as_ref().expect("Exchange name is required").to_string(),
            spot_api_mainnet_endpoint: plain_config
                .get("spot_api_mainnet_endpoint")
                .expect("Spot API mainnet endpoint is required")
                .to_string(),
            spot_api_testnet_endpoint: plain_config
                .get("spot_api_testnet_endpoint")
                .expect("Spot API testnet endpoint is required")
                .to_string(),
            spot_ws_mainnet_endpoint: plain_config
                .get("spot_ws_mainnet_endpoint")
                .expect("Spot WebSocket mainnet endpoint is required")
                .to_string(),
            spot_ws_testnet_endpoint: plain_config
                .get("spot_ws_testnet_endpoint")
                .expect("Spot WebSocket testnet endpoint is required")
                .to_string(),
            spot_market_api_mainnet_endpoint: plain_config
                .get("spot_market_api_mainnet_endpoint")
                .expect("Spot market API mainnet endpoint is required")
                .to_string(),
            spot_market_ws_mainnet_endpoint: plain_config
                .get("spot_market_ws_mainnet_endpoint")
                .expect("Spot market WebSocket mainnet endpoint is required")
                .to_string(),
            usds_api_mainnet_endpoint: plain_config
                .get("usds_api_mainnet_endpoint")
                .expect("USDS API mainnet endpoint is required")
                .to_string(),
            usds_api_testnet_endpoint: plain_config
                .get("usds_api_testnet_endpoint")
                .expect("USDS API testnet endpoint is required")
                .to_string(),
            usds_ws_mainnet_endpoint: plain_config
                .get("usds_ws_mainnet_endpoint")
                .expect("USDS WebSocket mainnet endpoint is required")
                .to_string(),
            usds_ws_testnet_endpoint: plain_config
                .get("usds_ws_testnet_endpoint")
                .expect("USDS WebSocket testnet endpoint is required")
                .to_string(),
            usds_market_api_mainnet_endpoint: plain_config
                .get("usds_market_api_mainnet_endpoint")
                .expect("USDS market API mainnet endpoint is required")
                .to_string(),
            usds_market_ws_mainnet_endpoint: plain_config
                .get("usds_market_ws_mainnet_endpoint")
                .expect("USDS market WebSocket mainnet endpoint is required")
                .to_string(),
            api_key: secret_config.get("api_key").expect("API key is required").to_string(),
            api_secret: secret_config
                .get("api_secret")
                .expect("API secret is required")
                .to_string(),
            api_timeout: Duration::from_secs(
                plain_config
                    .get("api_timeout")
                    .expect("API timeout is required")
                    .parse::<u64>()
                    .expect("API timeout must be a valid number"),
            ),
            api_retries: plain_config
                .get("api_retries")
                .expect("API retries is required")
                .parse::<u32>()
                .expect("API retries must be a valid number"),
            api_backoff: plain_config
                .get("api_backoff")
                .expect("API backoff is required")
                .parse::<u32>()
                .expect("API backoff must be a valid number"),
            use_websocket: plain_config
                .get("enable_websocket")
                .expect("Enable WebSocket is required")
                .parse::<bool>()
                .expect("Enable WebSocket must be a valid boolean"),
            ws_timeout: Duration::from_secs(
                plain_config
                    .get("ws_timeout")
                    .expect("WebSocket timeout is required")
                    .parse::<u64>()
                    .expect("WebSocket timeout must be a valid number"),
            ),
            ws_reconnect_delay: plain_config
                .get("ws_reconnect_delay")
                .expect("WebSocket reconnect delay is required")
                .parse::<u64>()
                .expect("WebSocket reconnect delay must be a valid number"),
            ws_streams_max_concurrent: plain_config
                .get("ws_streams_max_concurrent")
                .expect("WebSocket streams max concurrent is required")
                .parse::<usize>()
                .expect("WebSocket streams max concurrent must be a valid number"),
            description: exchange.description.clone(),
        }
    }
}

pub struct SigbotHyperliquidClient {
    config: SigbotHyperliquidConfig,
    // Kline store for websocket streams.
    kline_store: Arc<dyn ICache<Vec<KlineModel>>>,
}

impl SigbotHyperliquidClient {
    pub const KIND: &'static str = "HYPERLIQUID"; // ExchangeProvider::HYPERLIQUID

    pub async fn new(exchange: Arc<ExchangeInfo>) -> Arc<Self> {
        let config = SigbotHyperliquidConfig::from_exchange(exchange.clone());
        let string_cache = CacheContainer::<String>::new();
        let kline_store: Box<dyn ICache<Vec<KlineModel>>> = Box::new(KlineModelCacheWrapper {
            inner: Arc::from(string_cache),
        });
        Arc::new(Self {
            config: config.to_owned(),
            kline_store: Arc::from(kline_store),
        })
    }
}

// see:https://github.com/wl4g-blockchain/hyperliquid-rust-sdk/blob/master/src/bin/ws_candles.rs
#[async_trait]
impl ISigbotExchangeClient for SigbotHyperliquidClient {
    fn provider(&self) -> ExchangeProvider {
        ExchangeProvider::HYPERLIQUID
    }

    async fn init(&self) {
        info!("Starting Hyperliquid exchange manager with config={}", self.config);

        if self.config.use_websocket {
            info!("Initializing USDS API WS client with config={}", self.config);
            unimplemented!("Hyperliquid does not support WebSocket API");
        } else {
            info!("Initializing USDS API Rest client with {}", self.config);
            unimplemented!("Hyperliquid does not support REST API");
        }
    }

    async fn close(&self) {
        info!("Closing Hyperliquid operator with {}", self.config);
        unimplemented!("Hyperliquid does not support WebSocket API");
    }

    async fn enter_position(&self, signal: PlaceTradeSignal) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;
        unimplemented!("Hyperliquid does not support entry position trading");
    }

    async fn exit_loss_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;

        unimplemented!("Hyperliquid does not support exit loss position trading");
    }

    async fn exit_profit_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;

        unimplemented!("Hyperliquid does not support exit profit position trading");
    }

    async fn search_symbols(&self, _query: &str, _sec_type: Option<&str>) -> Result<Vec<SymbolInfo>, Error> {
        // TODO: Implement symbol search for Hyperliquid
        Err(Error::msg("Symbol search not yet implemented for Hyperliquid"))
    }

    async fn get_orders(&self, _account_id: Option<&str>, _filters: Option<&str>) -> Result<Vec<OrderInfo>, Error> {
        // TODO: Implement order list retrieval for Hyperliquid
        Err(Error::msg("Order list retrieval not yet implemented for Hyperliquid"))
    }
}

#[async_trait]
impl ISigbotOrderBookExchangeClient for SigbotHyperliquidClient {
    async fn get_current_price(&self, symbol: &str) -> Result<PriceModel, Error> {
        if self.config.use_websocket {
            unimplemented!("Hyperliquid does not support WebSocket API");
        } else {
            info!(
                "Getting current price for symbol={} from Hyperliquid using REST API",
                symbol
            );
            unimplemented!("Hyperliquid does not support REST API");
        }
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Result<Vec<KlineModel>, Error> {
        if self.config.use_websocket {
            info!(
                "Getting klines for symbol={} from Hyperliquid using WebSocket Streams with interval={}",
                symbol, interval
            );
            unimplemented!("Hyperliquid does not support WebSocket Streams");
        } else {
            info!("Getting klines for symbol={} from Hyperliquid using REST API", symbol);
            unimplemented!("Hyperliquid does not support REST API");
        }
    }
}

#[async_trait]
impl ISigbotDEXExchangeClient for SigbotHyperliquidClient {}
