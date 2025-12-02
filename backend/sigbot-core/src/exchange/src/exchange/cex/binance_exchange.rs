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

use crate::exchange::exchange_engine::ISigbotExchangeManager;
use anyhow::{Context, Error};
use async_trait::async_trait;
use binance_sdk::common::websocket::WebsocketStream;
use binance_sdk::config::ConfigurationWebsocketStreams;
use binance_sdk::derivatives_trading_usds_futures::websocket_streams::{
    self, KlineCandlestickStreamsResponse, WebsocketStreams,
};
use binance_sdk::derivatives_trading_usds_futures::{rest_api, websocket_api, DerivativesTradingUsdsFuturesWsStreams};
use binance_sdk::models::WebsocketMode;
use binance_sdk::{
    config::{ConfigurationRestApi, ConfigurationWebsocketApi},
    derivatives_trading_usds_futures::{
        rest_api::{
            ModifyOrderParams, ModifyOrderSideEnum, NewOrderParams, NewOrderSideEnum as RestNewOrderSideEnum, RestApi,
        },
        websocket_api::WebsocketApi,
        DerivativesTradingUsdsFuturesRestApi, DerivativesTradingUsdsFuturesWsApi,
    },
};
use common_telemetry::{debug, error, info};
use rust_decimal::Decimal;
use sigbot_core::cache::ICache;
use sigbot_types::{
    modules::exchange::exchange::ExchangeInfo,
    modules::exchange::models::{
        trade_market::{KlineResult, PriceResult},
        trade_signal::{EntryTradeSignal, ExitTradePosition, TradeResult},
    },
};
use std::collections::HashMap;
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SigbotBinanceConfig {
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
    // Coin-derivatives
    pub coin_api_mainnet_endpoint: String,
    pub coin_api_testnet_endpoint: String,
    pub coin_ws_mainnet_endpoint: String,
    pub coin_ws_testnet_endpoint: String,
    // Coin-derivatives-market endpoints
    pub coin_market_api_mainnet_endpoint: String,
    pub coin_market_ws_mainnet_endpoint: String,
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

impl std::fmt::Display for SigbotBinanceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SigbotBinanceConfig=(
                id={}, name={}, 
                spot_endpoints=(api_mainnet={}, api_testnet={}, ws_mainnet={}, ws_testnet={})),
                usds_endpoints=(api_mainnet={}, api_testnet={}, ws_mainnet={}, ws_testnet={})),
                coin_endpoints=(api_mainnet={}, api_testnet={}, ws_mainnet={}, ws_testnet={})),
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
            self.coin_api_mainnet_endpoint,
            self.coin_api_testnet_endpoint,
            self.coin_ws_mainnet_endpoint,
            self.coin_ws_testnet_endpoint,
        )
    }
}

impl SigbotBinanceConfig {
    pub fn from_exchange(exchange: ExchangeInfo) -> Self {
        let plain_config = exchange.plain_configuration.expect("Plain configuration is required");
        let secret_config = exchange.secret_configuration.expect("Secret configuration is required");
        Self {
            id: exchange.base.id.expect("Exchange ID is required"),
            name: exchange.name.expect("Exchange name is required"),
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
            coin_api_mainnet_endpoint: plain_config
                .get("coin_api_mainnet_endpoint")
                .expect("Coin API mainnet endpoint is required")
                .to_string(),
            coin_api_testnet_endpoint: plain_config
                .get("coin_api_testnet_endpoint")
                .expect("Coin API testnet endpoint is required")
                .to_string(),
            coin_ws_mainnet_endpoint: plain_config
                .get("coin_ws_mainnet_endpoint")
                .expect("Coin WebSocket mainnet endpoint is required")
                .to_string(),
            coin_ws_testnet_endpoint: plain_config
                .get("coin_ws_testnet_endpoint")
                .expect("Coin WebSocket testnet endpoint is required")
                .to_string(),
            coin_market_api_mainnet_endpoint: plain_config
                .get("coin_market_api_mainnet_endpoint")
                .expect("Coin market API mainnet endpoint is required")
                .to_string(),
            coin_market_ws_mainnet_endpoint: plain_config
                .get("coin_market_ws_mainnet_endpoint")
                .expect("Coin market WebSocket mainnet endpoint is required")
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
            description: exchange.description,
        }
    }
}

pub struct BinanceExchangeManager {
    config: SigbotBinanceConfig,
    rest_api_client: Arc<Mutex<Option<RestApi>>>,
    rest_market_client: Arc<Mutex<Option<RestApi>>>,
    ws_api_client: Arc<Mutex<Option<WebsocketApi>>>,
    ws_market_client: Arc<Mutex<Option<WebsocketApi>>>,
    ws_market_stream: Arc<Mutex<Option<WebsocketStreams>>>,
    // Kline store for websocket streams.
    kline_store: Arc<dyn ICache<Vec<KlineResult>>>,
    // Kline subscription map for websocket streams.
    kline_subscription_registrations:
        Arc<Mutex<HashMap<String, Arc<WebsocketStream<KlineCandlestickStreamsResponse>>>>>,
}

impl BinanceExchangeManager {
    pub const KIND: &'static str = "BINANCE"; // ExchangeProvider::BINANCE

    pub async fn new(config: &SigbotBinanceConfig, kline_store: Box<dyn ICache<Vec<KlineResult>>>) -> Arc<Self> {
        Arc::new(Self {
            config: config.to_owned(),
            rest_api_client: Arc::new(Mutex::new(None)),
            rest_market_client: Arc::new(Mutex::new(None)),
            ws_api_client: Arc::new(Mutex::new(None)),
            ws_market_client: Arc::new(Mutex::new(None)),
            ws_market_stream: Arc::new(Mutex::new(None)),
            kline_store: Arc::from(kline_store),
            kline_subscription_registrations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn _extract_kline_item(&self, index: usize, klines: &Vec<rest_api::KlineCandlestickDataResponseItemInner>) -> f64 {
        match klines.get(index) {
            Some(rest_api::KlineCandlestickDataResponseItemInner::Integer(i)) => *i as f64,
            Some(rest_api::KlineCandlestickDataResponseItemInner::String(s)) => s.parse::<f64>().unwrap_or(0.0),
            _ => -1.0,
        }
    }
}

// see:https://github.com/binance/binance-connector-rust/blob/main/examples/derivatives_trading_usds_futures/rest_api/trade_api/new_order.rs
// see:https://github.com/binance/binance-connector-rust/blob/main/examples/derivatives_trading_usds_futures/websocket_api/trade_api/new_order.rs
#[async_trait]
impl ISigbotExchangeManager for BinanceExchangeManager {
    async fn init(&self) {
        info!("Starting Binance exchange manager with config={}", self.config);

        if self.config.use_websocket {
            info!("Initializing USDS API WS client with config={}", self.config);
            match DerivativesTradingUsdsFuturesWsApi::from_config(
                ConfigurationWebsocketApi::builder()
                    .api_key(self.config.api_key.clone())
                    .api_secret(self.config.api_secret.clone())
                    .ws_url(self.config.usds_ws_mainnet_endpoint.clone())
                    .timeout(self.config.ws_timeout.as_millis() as u64)
                    .reconnect_delay(self.config.ws_reconnect_delay)
                    .build()
                    .expect("Failed to build WebSocket configuration"),
            )
            .connect()
            .await
            {
                Ok(ws_client) => {
                    *self.ws_api_client.lock().await = Some(ws_client);
                    info!("Initialized USDS API WS client with configId={}", self.config.id);
                }
                Err(e) => {
                    panic!("Failed to initialize USDS API WS client: {:?}", e);
                }
            }

            info!("Initializing USDS Market WS client with config={}", self.config);
            match DerivativesTradingUsdsFuturesWsApi::from_config(
                ConfigurationWebsocketApi::builder()
                    .api_key(self.config.api_key.clone())
                    .api_secret(self.config.api_secret.clone())
                    .ws_url(self.config.usds_market_ws_mainnet_endpoint.clone())
                    .reconnect_delay(self.config.ws_reconnect_delay)
                    .timeout(self.config.ws_timeout.as_millis() as u64)
                    .build()
                    .expect("Failed to build WebSocket configuration"),
            )
            .connect()
            .await
            {
                Ok(ws_client) => {
                    *self.ws_market_client.lock().await = Some(ws_client);
                    info!("Initialized USDS Market WS client with config={}", self.config);
                }
                Err(e) => {
                    panic!("Failed to initialize USDS Market WS client: {:?}", e);
                }
            }

            info!("Initializing USDS Market WS streams with config={}", self.config);
            // see:https://github.com/binance/binance-connector-rust/blob/main/examples/derivatives_trading_usds_futures/websocket_streams/kline_candlestick_streams.rs
            let connection = DerivativesTradingUsdsFuturesWsStreams::from_config(
                ConfigurationWebsocketStreams::builder()
                    .mode(WebsocketMode::Pool(self.config.ws_streams_max_concurrent))
                    .ws_url(self.config.usds_market_ws_mainnet_endpoint.clone())
                    .reconnect_delay(self.config.ws_reconnect_delay)
                    .build()
                    .expect("Failed to build websocket streams config."),
            )
            .connect()
            .await
            .expect("Failed to connect to websocket streams");
            *self.ws_market_stream.lock().await = Some(connection);
            info!("Initialized USDS Market WS streams with config={}", self.config);
        } else {
            info!("Initializing USDS API Rest client with {}", self.config);
            let rest_api = DerivativesTradingUsdsFuturesRestApi::from_config(
                ConfigurationRestApi::builder()
                    .api_key(self.config.api_key.clone())
                    .api_secret(self.config.api_secret.clone())
                    .base_path(self.config.usds_api_mainnet_endpoint.clone())
                    .timeout(self.config.api_timeout.as_secs())
                    .retries(self.config.api_retries)
                    .backoff(self.config.api_backoff as u64)
                    .build()
                    .expect("Failed to build REST API configuration"),
            );
            *self.rest_api_client.lock().await = Some(rest_api);
            info!("Initialized USDS API Rest client with {}", self.config.id);

            info!("Initializing USDS Market API Rest client with {}", self.config);
            let rest_market_api = DerivativesTradingUsdsFuturesRestApi::from_config(
                ConfigurationRestApi::builder()
                    .api_key(self.config.api_key.clone())
                    .api_secret(self.config.api_secret.clone())
                    .base_path(self.config.usds_market_api_mainnet_endpoint.clone())
                    .timeout(self.config.api_timeout.as_secs())
                    .retries(self.config.api_retries)
                    .backoff(self.config.api_backoff as u64)
                    .build()
                    .expect("Failed to build REST API configuration"),
            );
            *self.rest_market_client.lock().await = Some(rest_market_api);
            info!("Initialized USDS Market API Rest client with {}", self.config.id);
        }

        // Notice: It's will keep the program running
        // tokio::signal::ctrl_c().await.unwrap();
    }

    async fn close(&self) {
        info!("Closing Binance operator with {}", self.config);
        if self.config.use_websocket {
            if let Some(ws_api_client) = self.ws_api_client.lock().await.take() {
                if let Err(e) = ws_api_client.disconnect().await {
                    error!("Failed to disconnect USDS API WS client: {:?}", e);
                }
            }
            if let Some(ws_market_client) = self.ws_market_client.lock().await.take() {
                if let Err(e) = ws_market_client.disconnect().await {
                    error!("Failed to disconnect USDS Market WS client: {:?}", e);
                }
            }
        }
        info!("Closed Binance operator with {}", self.config);
    }

    async fn get_current_price(&self, symbol: &str) -> Result<PriceResult, Error> {
        if self.config.use_websocket {
            info!(
                "Getting current price for symbol={} from Binance using WebSocket API",
                symbol
            );
            let params = websocket_api::SymbolPriceTickerParams {
                id: None,
                symbol: Some(symbol.to_string()),
            };
            let guard = self.ws_market_client.lock().await;
            let response = guard
                .as_ref()
                .ok_or_else(|| Error::msg("WebSocket market client not initialized"))?
                .symbol_price_ticker(params)
                .await
                .context("Failed to get current price")?;

            let data = response.data().context("Failed to get price data")?;
            let (price, time) = match data {
                websocket_api::SymbolPriceTickerResponse::SymbolPriceTickerResponse1(inner) => {
                    let result_ref = inner.result.as_ref();
                    (
                        result_ref
                            .and_then(|r| r.price.as_ref())
                            .map(|p| p.parse::<f64>().expect("Failed to parse price")),
                        result_ref.and_then(|r| r.time.as_ref()).map(|t| *t as u64),
                    )
                }
                websocket_api::SymbolPriceTickerResponse::SymbolPriceTickerResponse2(items) => items
                    .result
                    .and_then(|item| item.first().cloned())
                    .map(|item| {
                        (
                            item.price.map(|p| p.parse::<f64>().expect("Failed to parse price")),
                            item.time.map(|t| t as u64),
                        )
                    })
                    .ok_or_else(|| Error::msg("No current price data available"))?,
                websocket_api::SymbolPriceTickerResponse::Other(_) => {
                    return Err(Error::msg("Unexpected response type from symbol price ticker API"))
                }
            };
            Ok(PriceResult {
                price: price.ok_or_else(|| Error::msg("No current price available"))?,
                time: time.ok_or_else(|| Error::msg("No current time available"))?,
            })
        } else {
            info!(
                "Getting current price for symbol={} from Binance using REST API",
                symbol
            );
            let params = rest_api::SymbolPriceTickerV2Params {
                symbol: Some(symbol.to_string()),
            };
            let guard = self.rest_market_client.lock().await;
            let response = guard
                .as_ref()
                .ok_or_else(|| Error::msg("REST market client not initialized"))?
                .symbol_price_ticker_v2(params)
                .await
                .context("Failed to get current price")?;

            let data = response.data().await.context("Failed to get price data")?;
            let (price, time) = match data {
                rest_api::SymbolPriceTickerV2Response::SymbolPriceTickerV2Response1(inner) => (inner.price, inner.time),
                rest_api::SymbolPriceTickerV2Response::SymbolPriceTickerV2Response2(items) => items
                    .first()
                    .ok_or_else(|| Error::msg("No current price data available"))
                    .map(|item| (item.price.to_owned(), item.time))?,
                rest_api::SymbolPriceTickerV2Response::Other(_) => {
                    return Err(Error::msg("Unexpected response type from symbol price ticker API"))
                }
            };
            Ok(PriceResult {
                price: price
                    .ok_or_else(|| Error::msg("No current price available"))?
                    .parse::<f64>()
                    .unwrap_or(-1.0),
                time: time.ok_or_else(|| Error::msg("No current price time available"))? as u64,
            })
        }
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Result<Vec<KlineResult>, Error> {
        if self.config.use_websocket {
            info!(
                "Getting klines for symbol={} from Binance using WebSocket Streams with interval={}",
                symbol, interval
            );

            let store_key = format!("{}_{}", symbol, interval);
            if !self
                .kline_subscription_registrations
                .lock()
                .await
                .contains_key(&store_key)
            {
                info!(
                    "Subscribing to kline candlestick websocket streams for symbol={} and interval={}",
                    symbol, interval
                );

                let guard = self.ws_market_stream.lock().await;
                let stream = guard
                    .as_ref()
                    .ok_or_else(|| Error::msg("WebSocket market stream not initialized"))?
                    .kline_candlestick_streams(
                        websocket_streams::KlineCandlestickStreamsParams::builder(
                            symbol.to_string(),
                            interval.to_string(),
                        )
                        .build()?,
                    )
                    .await
                    .context("Failed to subscribe to the stream")?;

                // Register callback for incoming messages.
                let store_key0 = store_key.to_string();
                let kline_store0 = self.kline_store.clone();
                stream.on_message(move |data| {
                    debug!("Received klines from WebSocket Streams: {:?}", data);

                    let kline = data.k.map(|kline| {
                        // let kline0 = Arc::new(kline);
                        return KlineResult {
                            open_time: kline.to_owned().t.map(|v| v as u64).unwrap_or(0),
                            open_price: kline
                                .to_owned()
                                .o
                                .map(|v| v.parse::<f64>().expect("Failed to parse open price"))
                                .unwrap_or(0.0),
                            high_price: kline
                                .to_owned()
                                .h
                                .map(|v| v.parse::<f64>().expect("Failed to parse high price"))
                                .unwrap_or(0.0),
                            low_price: kline
                                .to_owned()
                                .l
                                .map(|v| v.parse::<f64>().expect("Failed to parse low price"))
                                .unwrap_or(0.0),
                            close_price: kline
                                .to_owned()
                                .c
                                .map(|v| v.parse::<f64>().expect("Failed to parse close price"))
                                .unwrap_or(0.0),
                            volume: kline
                                .to_owned()
                                .v
                                .map(|v| v.parse::<f64>().expect("Failed to parse volume price"))
                                .unwrap_or(0.0),
                            close_time: kline.to_owned().t_uppercase.map(|v| v as u64).unwrap_or(0),
                        };
                    });

                    let kline_store1 = kline_store0.clone();
                    let store_key1 = store_key0.clone();
                    if let Some(kline) = kline {
                        tokio::spawn(async move {
                            if let Err(e) = kline_store1
                                // TODO: using simailar redis list to store klines, and use the last kline as the current kline.
                                .set(store_key1, vec![kline], None)
                                .await
                                .context("Failed to set klines to cache")
                            {
                                error!("Failed to set klines to cache: {:?}", e);
                            }
                        });
                    }
                });

                info!(
                    "Registering kline subscription stream for symbol={} and interval={}",
                    symbol, interval
                );
                self.kline_subscription_registrations
                    .lock()
                    .await
                    .insert(store_key.to_owned(), stream);

                info!(
                    "Subscribed to kline candlestick websocket streams for symbol={} and interval={}",
                    symbol, interval
                );
            }

            // TODO: Implement the logic to get klines from cache with start_time and end_time.
            let klines = self.kline_store.get(store_key).await?.unwrap_or_default();
            debug!("Getting klines from cache: {:?}", klines);
            if klines.is_empty() {
                error!(
                    "No klines found in cache for symbol={} and interval={}",
                    symbol, interval
                );
                return Err(Error::msg("No klines found in cache"));
            }

            Ok(klines)
        } else {
            info!("Getting klines for symbol={} from Binance using REST API", symbol);

            let params = rest_api::KlineCandlestickDataParams::builder(
                symbol.to_string(),
                rest_api::KlineCandlestickDataIntervalEnum::from_str(interval)
                    .map_err(|e| Error::msg(format!("Invalid interval: {:?}", e)))?,
            )
            .limit(Some(limit as i64))
            .start_time(start_time)
            .end_time(end_time)
            .build()
            .context("Failed to build Kline candlestick data parameters")?;

            let guard = self.rest_market_client.lock().await;
            let response = guard
                .as_ref()
                .ok_or_else(|| Error::msg("REST market client not initialized"))?
                .kline_candlestick_data(params)
                .await?;
            let data = response.data().await?;
            let klines = data
                .into_iter()
                .map(|kline| KlineResult {
                    open_time: self._extract_kline_item(0, &kline) as u64,
                    open_price: self._extract_kline_item(1, &kline),
                    high_price: self._extract_kline_item(2, &kline),
                    low_price: self._extract_kline_item(3, &kline),
                    close_price: self._extract_kline_item(4, &kline),
                    volume: self._extract_kline_item(5, &kline),
                    close_time: self._extract_kline_item(6, &kline) as u64,
                })
                .collect();
            Ok(klines)
        }
    }

    async fn entry_position(&self, signal: EntryTradeSignal) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;

        let order_type_str = signal.open_pos.order_type.to_str();
        //let order_type = match order_type_str {
        //    "MARKET" => NewOrderTypeEnum::Market,
        //    "LIMITED" | "LIMIT" => NewOrderTypeEnum::Limit,
        //    _ => return Err(Error::msg(format!("Unsupported order type: {}", order_type_str))),
        //};
        let side = RestNewOrderSideEnum::from_str(signal.open_pos.side.to_side_str())
            .map_err(|e| Error::msg(format!("Invalid order side: {:?}", e)))?;

        let mut builder = NewOrderParams::builder(signal.open_pos.symbol.to_string(), side, order_type_str.to_owned())
            .quantity(Some(
                Decimal::from_str_exact(&signal.open_pos.quantity.to_string())
                    .map_err(|e| Error::msg(format!("Invalid quantity value: {}", e)))?,
            ));
        if let Some(price) = signal.open_pos.price {
            let price_str = price.to_string();
            builder = builder.price(Some(
                Decimal::from_str_exact(&price_str).map_err(|e| Error::msg(format!("Invalid price value: {}", e)))?,
            ));
        }
        let params = builder.build()?;

        let guard = self.rest_api_client.lock().await;
        let rest_client = guard
            .as_ref()
            .ok_or_else(|| Error::msg("REST API client not initialized"))?;

        info!(
            "[OPEN_POS] Opening position - symbol={}, side={}, quantity={}",
            signal.open_pos.symbol,
            signal.open_pos.side.to_side_str(),
            signal.open_pos.quantity
        );
        let response = rest_client.new_order(params).await.context("Failed to New order")?;
        let data = response.data().await?;
        info!(
            "[OPEN_POS] Opened position - orderId={}, price={}",
            data.order_id.context("New Order ID is required")?,
            data.avg_price.context("Average price is required")?
        );

        let order_id = data.order_id.context("New Order ID is required")?;
        Ok(TradeResult {
            success: true,
            order_id: order_id as u64,
            message: None,
        })
    }

    async fn exit_loss_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;

        let side = ModifyOrderSideEnum::from_str(signal.side.to_side_str())
            .map_err(|e| Error::msg(format!("Invalid order side: {:?}", e)))?;
        let price = signal
            .price
            .map(|p| {
                let price_str = p.to_string();
                Decimal::from_str_exact(&price_str).map_err(|e| Error::msg(format!("Invalid price value: {}", e)))
            })
            .transpose()?;
        let quantity = Decimal::from_str_exact(&signal.quantity_percent.to_string())
            .map_err(|e| Error::msg(format!("Invalid quantity value: {}", e)))?;

        info!(
            "[EXIT_LOSS] Modifying - symbol={}, side={}, quantity={}, original_order_id={}",
            signal.symbol,
            signal.side.to_side_str(),
            signal.quantity_percent,
            original_order_id
        );
        let guard = self.rest_api_client.lock().await;
        let response = guard
            .as_ref()
            .ok_or_else(|| Error::msg("REST API client not initialized"))?
            .modify_order(
                ModifyOrderParams::builder(
                    signal.symbol.to_string(),
                    side,
                    price.context("Modify the price is required")?,
                    quantity,
                )
                .order_id(Some(original_order_id as i64))
                .build()?,
            )
            .await
            .context("Failed to Modify order")?;
        info!(
            "[EXIT_LOSS] Modified - symbol={}, side={}, quantity={}, original_order_id={}",
            signal.symbol,
            signal.side.to_side_str(),
            signal.quantity_percent,
            original_order_id
        );

        let order_id = response
            .data()
            .await?
            .order_id
            .context("Modified Order ID is required")?;
        Ok(TradeResult {
            success: true,
            order_id: order_id as u64,
            message: None,
        })
    }

    async fn exit_profit_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;

        let side = ModifyOrderSideEnum::from_str(signal.side.to_side_str())
            .map_err(|e| Error::msg(format!("Invalid order side: {:?}", e)))?;
        let price = signal
            .price
            .map(|p| {
                Decimal::from_str_exact(&p.to_string()).map_err(|e| Error::msg(format!("Invalid price value: {}", e)))
            })
            .transpose()?;
        let quantity = Decimal::from_str_exact(&signal.quantity_percent.to_string().to_owned())
            .map_err(|e| Error::msg(format!("Invalid quantity value: {}", e)))?;

        info!(
            "[EXIT_PROFIT] Modifying - symbol={}, side={}, quantity={}, original_order_id={}",
            signal.symbol,
            signal.side.to_side_str(),
            signal.quantity_percent,
            original_order_id
        );
        let guard = self.rest_api_client.lock().await;
        let response = guard
            .as_ref()
            .ok_or_else(|| Error::msg("REST API client not initialized"))?
            .modify_order(
                ModifyOrderParams::builder(
                    signal.symbol.to_string(),
                    side,
                    price.context("Modify the price is required")?,
                    quantity,
                )
                .order_id(Some(original_order_id as i64))
                .build()?,
            )
            .await
            .context("Failed to Modify order")?;
        info!(
            "[EXIT_PROFIT] Modified - symbol={}, side={}, quantity={}, original_order_id={}",
            signal.symbol,
            signal.side.to_side_str(),
            signal.quantity_percent,
            original_order_id
        );

        let order_id = response
            .data()
            .await?
            .order_id
            .context("Modified Order ID is required")?;
        Ok(TradeResult {
            success: true,
            order_id: order_id as u64,
            message: None,
        })
    }
}
