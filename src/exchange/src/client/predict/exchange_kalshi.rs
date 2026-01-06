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

use crate::client::exchange_factory::ISigbotExchangeClient;
use crate::client::predict::ISigbotPredictExchangeClient;
use anyhow::{Context, Error};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use common_telemetry::{error, info};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sigbot_core::cache::ICache;
use sigbot_types::modules::exchange::exchange::ExchangeProvider;
use sigbot_types::modules::exchange::models::trade_position::TradeResult;
use sigbot_types::{
    modules::exchange::exchange::ExchangeInfo,
    modules::exchange::models::{
        trade_market::{KlineModel, OrderInfo, PriceModel, SymbolInfo},
        trade_position::{ExitTradePosition, PlaceTradeSignal},
    },
};
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub struct SigbotKalshiConfig {
    pub id: i64,
    pub name: String,
    // Kalshi API endpoints
    pub api_mainnet_endpoint: String,
    pub api_testnet_endpoint: String,
    pub ws_mainnet_endpoint: String,
    pub ws_testnet_endpoint: String,
    // Secrets configuration
    pub api_key: String,
    pub api_secret: String,
    // Kalshi specific configuration
    pub user_id: Option<String>,    // Kalshi user ID
    pub account_id: Option<String>, // Kalshi account ID
    // Generic configuration
    pub api_timeout: Duration,
    pub api_retries: u32,
    pub api_backoff: u32,
    pub use_websocket: bool,
    pub ws_timeout: Duration,
    pub ws_reconnect_delay: u64,
    pub description: Option<String>,
}

impl std::fmt::Display for SigbotKalshiConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SigbotKalshiConfig=(
                id={}, name={},
                endpoints=(api_mainnet={}, api_testnet={}, ws_mainnet={}, ws_testnet={}),
                use_websocket={}
            )",
            self.id,
            self.name,
            self.api_mainnet_endpoint,
            self.api_testnet_endpoint,
            self.ws_mainnet_endpoint,
            self.ws_testnet_endpoint,
            self.use_websocket,
        )
    }
}

impl SigbotKalshiConfig {
    pub fn from_exchange(exchange: Arc<ExchangeInfo>) -> Self {
        let plain_config = exchange.properties.as_ref().expect("Plain configuration is required");
        let secret_config = exchange.secrets.as_ref().expect("Secret configuration is required");
        Self {
            id: exchange.base.id.expect("Exchange ID is required"),
            name: exchange.name.as_ref().expect("Exchange name is required").to_string(),
            api_mainnet_endpoint: plain_config
                .get("api_mainnet_endpoint")
                .expect("API mainnet endpoint is required")
                .to_string(),
            api_testnet_endpoint: plain_config
                .get("api_testnet_endpoint")
                .expect("API testnet endpoint is required")
                .to_string(),
            ws_mainnet_endpoint: plain_config
                .get("ws_mainnet_endpoint")
                .expect("WebSocket mainnet endpoint is required")
                .to_string(),
            ws_testnet_endpoint: plain_config
                .get("ws_testnet_endpoint")
                .expect("WebSocket testnet endpoint is required")
                .to_string(),
            api_key: secret_config.get("api_key").expect("API key is required").to_string(),
            api_secret: secret_config
                .get("api_secret")
                .expect("API secret is required")
                .to_string(),
            user_id: plain_config.get("user_id").map(|s| s.to_string()),
            account_id: plain_config.get("account_id").map(|s| s.to_string()),
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
            description: exchange.description.clone(),
        }
    }
}

// API Response structures
#[derive(Debug, Deserialize)]
struct KalshiOrderBookResponse {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct KalshiMarketResponse {
    id: String,
    title: String,
    ticker: String,
    #[serde(rename = "event_ticker")]
    event_ticker: Option<String>,
    #[serde(rename = "subtitle")]
    subtitle: Option<String>,
    #[serde(rename = "close_time")]
    close_time: Option<String>,
    #[serde(rename = "open_time")]
    open_time: Option<String>,
    #[serde(rename = "status")]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KalshiMarketsResponse {
    markets: Vec<KalshiMarketResponse>,
}

#[derive(Debug, Deserialize)]
struct KalshiOrderResponse {
    success: bool,
    #[serde(rename = "error_msg")]
    error_msg: Option<String>,
    #[serde(rename = "order_id")]
    order_id: Option<String>,
    #[serde(rename = "order")]
    order: Option<KalshiOrder>,
}

#[derive(Debug, Deserialize)]
struct KalshiOrder {
    #[serde(rename = "order_id")]
    order_id: String,
    #[serde(rename = "ticker")]
    ticker: String,
    #[serde(rename = "side")]
    side: String,
    #[serde(rename = "action")]
    action: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(rename = "count")]
    count: i32,
    #[serde(rename = "price")]
    price: i32,
    #[serde(rename = "status")]
    status: String,
    #[serde(rename = "created_time")]
    created_time: Option<i64>,
    #[serde(rename = "execution_time")]
    execution_time: Option<i64>,
    #[serde(rename = "executed_count")]
    executed_count: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct KalshiOrdersResponse {
    orders: Vec<KalshiOrder>,
}

// Order request structures according to Kalshi API documentation
#[derive(Debug, Serialize)]
struct KalshiOrderRequest {
    ticker: String,
    side: String,   // "yes" or "no"
    action: String, // "buy" or "sell"
    #[serde(rename = "type")]
    order_type: String, // "market" or "limit"
    count: i32,
    price: Option<i32>, // Price in cents (required for limit orders)
}

#[derive(Debug, Serialize)]
struct KalshiCancelOrderRequest {
    #[serde(rename = "order_id")]
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct KalshiCancelOrderResponse {
    success: bool,
    #[serde(rename = "error_msg")]
    error_msg: Option<String>,
}

pub struct SigbotKalshiClient {
    config: SigbotKalshiConfig,
    http_client: Arc<Client>,
    // Kline store for caching market data
    kline_store: Arc<dyn ICache<Vec<KlineModel>>>,
}

impl SigbotKalshiClient {
    pub const KIND: &'static str = "KALSHI"; // ExchangeProvider::KALSHI

    pub async fn new(config: &SigbotKalshiConfig, kline_store: Box<dyn ICache<Vec<KlineModel>>>) -> Arc<Self> {
        // Build HTTP client with timeout configuration
        let http_client = Client::builder()
            .timeout(config.api_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Arc::new(Self {
            config: config.to_owned(),
            http_client: Arc::new(http_client),
            kline_store: Arc::from(kline_store),
        })
    }

    fn get_api_endpoint(&self) -> &str {
        // Use mainnet endpoint by default, can be configured based on environment
        &self.config.api_mainnet_endpoint
    }

    fn build_auth_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        // Kalshi uses API key and secret in Authorization header
        // Format: Basic base64(api_key:api_secret)
        let credentials = format!("{}:{}", self.config.api_key, self.config.api_secret);
        let encoded = general_purpose::STANDARD.encode(credentials.as_bytes());
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Basic {}", encoded).parse().unwrap(),
        );
        headers.insert(reqwest::header::CONTENT_TYPE, "application/json".parse().unwrap());
        headers
    }

    async fn execute_with_retry<F, Fut>(&self, mut request_fn: F) -> Result<reqwest::Response, Error>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    {
        let mut last_error = None;
        for attempt in 0..=self.config.api_retries {
            match request_fn().await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.config.api_retries {
                        let delay = Duration::from_millis(self.config.api_backoff as u64 * (attempt + 1) as u64 * 100);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        Err(Error::from(last_error.unwrap()).context("Request failed after retries"))
    }
}

#[async_trait]
impl ISigbotExchangeClient for SigbotKalshiClient {
    fn provider(&self) -> ExchangeProvider {
        ExchangeProvider::KALSHI
    }

    async fn init(&self) {
        info!("Starting Kalshi exchange client with config={}", self.config);

        // Test API connection by checking health endpoint
        let health_url = format!("{}/health", self.get_api_endpoint());
        match self.http_client.get(&health_url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    info!("Kalshi API connection test successful with configId={}", self.config.id);
                } else {
                    error!("Kalshi API connection test failed with status: {}", response.status());
                }
            }
            Err(e) => {
                error!("Failed to test Kalshi API connection: {:?}", e);
            }
        }

        if self.config.use_websocket {
            info!("WebSocket support is enabled but not yet fully implemented for Kalshi");
        }
    }

    async fn close(&self) {
        info!("Closing Kalshi exchange client with {}", self.config);
        // HTTP client doesn't need explicit cleanup
    }

    // see: https://trading-api.readme.io/reference/placeorder
    async fn enter_position(&self, signal: PlaceTradeSignal) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;
        info!("Entry position request for Kalshi: {:?}", signal);

        // According to Kalshi API documentation:
        // POST /portfolio/orders requires: ticker, side (yes/no), action (buy/sell), type (market/limit), count, price (optional for market)
        // See: https://trading-api.readme.io/reference/placeorder

        let side_str = signal.enter_pos.side.to_side_str();
        // Kalshi uses "yes" or "no" for side, convert from BUY/SELL
        let side = if side_str == "BUY" { "yes" } else { "no" };
        let action = "buy"; // Always buy for entry position
        let price = signal.enter_pos.price;
        let size = signal.enter_pos.quantity as i32; // Kalshi uses integer count
        let ticker = &signal.enter_pos.symbol;

        // Determine order type from signal (default to limit if price is provided, otherwise market)
        let order_type = match signal.enter_pos.order_type {
            sigbot_types::modules::exchange::models::trade_position::OrderType::MARKET => "market",
            sigbot_types::modules::exchange::models::trade_position::OrderType::LIMITED => "limit",
        };

        // Build order request
        let order_request = KalshiOrderRequest {
            ticker: ticker.to_string(),
            side: side.to_string(),
            action: action.to_string(),
            order_type: order_type.to_string(),
            count: size,
            price: price.map(|p| (p * 100.0) as i32), // Convert to cents
        };

        // Send order to Kalshi API
        let url = format!("{}/portfolio/orders", self.get_api_endpoint());
        let headers = self.build_auth_headers();

        info!(
            "[OPEN_POS] Placing order on Kalshi - ticker={}, side={}, action={}, type={}, count={}, price={:?}",
            ticker, side, action, order_type, size, order_request.price
        );

        let response = self
            .execute_with_retry(|| {
                self.http_client
                    .post(&url)
                    .headers(headers.clone())
                    .json(&order_request)
                    .send()
            })
            .await
            .context("Failed to place order")?;

        let status = response.status();
        let response_text = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(Error::msg(format!(
                "Kalshi API returned error status: {} - {}",
                status, response_text
            )));
        }

        let order_response: KalshiOrderResponse =
            serde_json::from_str(&response_text).context("Failed to parse order response")?;

        if !order_response.success {
            let error_msg = order_response.error_msg.unwrap_or_else(|| "Unknown error".to_string());
            return Err(Error::msg(format!("Order placement failed: {}", error_msg)));
        }

        let order_id = order_response
            .order
            .as_ref()
            .and_then(|o| o.order_id.parse::<u64>().ok())
            .ok_or_else(|| Error::msg("Order ID not found in response"))?;

        info!("[OPEN_POS] Order placed successfully - order_id={}", order_id);

        Ok(TradeResult {
            success: true,
            order_id,
            message: Some(format!("Order placed successfully on Kalshi")),
        })
    }

    // see: https://trading-api.readme.io/reference/cancelorder
    async fn exit_loss_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;
        info!(
            "Exit loss position request for Kalshi: order_id={}, signal={:?}",
            original_order_id, signal
        );

        // Cancel the original order according to Kalshi API documentation
        // POST /portfolio/orders/{order_id}/cancel
        // See: https://trading-api.readme.io/reference/cancelorder
        let cancel_url = format!(
            "{}/portfolio/orders/{}/cancel",
            self.get_api_endpoint(),
            original_order_id
        );
        let headers = self.build_auth_headers();

        let response = self
            .execute_with_retry(|| self.http_client.post(&cancel_url).headers(headers.clone()).send())
            .await
            .context("Failed to cancel order")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "Failed to cancel order: {} - {}",
                status, error_text
            )));
        }

        let cancel_response: KalshiCancelOrderResponse =
            response.json().await.context("Failed to parse cancel order response")?;

        if cancel_response.success {
            info!("Successfully cancelled order {}", original_order_id);
            Ok(TradeResult {
                success: true,
                order_id: original_order_id,
                message: Some("Order cancelled successfully".to_string()),
            })
        } else {
            let error_msg = cancel_response.error_msg.unwrap_or_else(|| "Unknown error".to_string());
            Err(Error::msg(format!("Failed to cancel order: {}", error_msg)))
        }
    }

    async fn exit_profit_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;
        info!(
            "Exit profit position request for Kalshi: order_id={}, signal={:?}",
            original_order_id, signal
        );

        // Same as exit_loss_position - cancel the original order
        let cancel_url = format!(
            "{}/portfolio/orders/{}/cancel",
            self.get_api_endpoint(),
            original_order_id
        );
        let headers = self.build_auth_headers();

        let response = self
            .execute_with_retry(|| self.http_client.post(&cancel_url).headers(headers.clone()).send())
            .await
            .context("Failed to cancel order")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "Failed to cancel order: {} - {}",
                status, error_text
            )));
        }

        let cancel_response: KalshiCancelOrderResponse =
            response.json().await.context("Failed to parse cancel order response")?;

        if cancel_response.success {
            info!("Successfully cancelled order {}", original_order_id);
            Ok(TradeResult {
                success: true,
                order_id: original_order_id,
                message: Some("Order cancelled successfully".to_string()),
            })
        } else {
            let error_msg = cancel_response.error_msg.unwrap_or_else(|| "Unknown error".to_string());
            Err(Error::msg(format!("Failed to cancel order: {}", error_msg)))
        }
    }

    async fn search_symbols(&self, query: &str, _sec_type: Option<&str>) -> Result<Vec<SymbolInfo>, Error> {
        info!("Searching symbols on Kalshi - query={}", query);

        // Use Kalshi API for market search
        let url = format!("{}/markets", self.get_api_endpoint());
        let headers = self.build_auth_headers();

        let response = self
            .execute_with_retry(|| {
                self.http_client
                    .get(&url)
                    .headers(headers.clone())
                    .query(&[("query", query)])
                    .send()
            })
            .await
            .context("Failed to search markets")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "Kalshi API returned error status: {} - {}",
                status, error_text
            )));
        }

        let markets: KalshiMarketsResponse = response.json().await.context("Failed to parse markets response")?;

        let symbols: Vec<SymbolInfo> = markets
            .markets
            .into_iter()
            .map(|market| SymbolInfo {
                symbol: market.ticker.clone(),
                contract_id: Some(market.id.clone()),
                name: Some(market.title.clone()),
                exchange: Some("Kalshi".to_string()),
                sec_type: Some("PREDICTION".to_string()),
                currency: Some("USD".to_string()),
                description: market.subtitle,
            })
            .collect();

        info!("Found {} symbols matching query '{}'", symbols.len(), query);
        Ok(symbols)
    }

    async fn get_orders(&self, _account_id: Option<&str>, _filters: Option<&str>) -> Result<Vec<OrderInfo>, Error> {
        info!("Getting orders from Kalshi");

        // According to Kalshi API documentation:
        // GET /portfolio/orders - Get all orders
        // See: https://trading-api.readme.io/reference/getorders

        let url = format!("{}/portfolio/orders", self.get_api_endpoint());
        let headers = self.build_auth_headers();

        let response = self
            .execute_with_retry(|| self.http_client.get(&url).headers(headers.clone()).send())
            .await
            .context("Failed to get orders")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "Kalshi API returned error status: {} - {}",
                status, error_text
            )));
        }

        let orders_response: KalshiOrdersResponse = response.json().await.context("Failed to parse orders response")?;

        // Convert Kalshi orders to OrderInfo
        let orders: Vec<OrderInfo> = orders_response
            .orders
            .into_iter()
            .map(|order| {
                let executed_count = order.executed_count.unwrap_or(0);
                let total_count = order.count;
                let remaining_count = total_count - executed_count;
                OrderInfo {
                    order_id: order.order_id,
                    symbol: order.ticker,
                    side: order.side,
                    order_type: order.order_type,
                    status: order.status,
                    quantity: total_count as f64,
                    filled_quantity: Some(executed_count as f64),
                    remaining_quantity: Some(remaining_count as f64),
                    price: Some(order.price as f64 / 100.0), // Convert from cents
                    avg_price: None,                         // Not available in Kalshi order response
                    create_time: order.created_time.map(|t| t as u64 * 1000), // Convert seconds to milliseconds
                    update_time: order.execution_time.map(|t| t as u64 * 1000), // Convert seconds to milliseconds
                    time_in_force: None,                     // Not available in Kalshi order response
                }
            })
            .collect();

        info!("Retrieved {} orders from Kalshi", orders.len());
        Ok(orders)
    }
}

#[async_trait]
impl ISigbotPredictExchangeClient for SigbotKalshiClient {}
