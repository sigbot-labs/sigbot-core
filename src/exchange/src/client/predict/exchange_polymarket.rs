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
pub struct SigbotPolymarketConfig {
    pub id: i64,
    pub name: String,
    // Polymarket API endpoints
    pub api_mainnet_endpoint: String,
    pub api_testnet_endpoint: String,
    pub ws_mainnet_endpoint: String,
    pub ws_testnet_endpoint: String,
    // Secrets configuration
    pub api_key: String,
    pub api_secret: String,
    // Polymarket specific configuration
    pub maker_address: Option<String>,  // Funder address
    pub signer_address: Option<String>, // Signing address
    pub taker_address: Option<String>,  // Operator address (default: zero address)
    pub fee_rate_bps: Option<u32>,      // Fee rate in basis points
    // Generic configuration
    pub api_timeout: Duration,
    pub api_retries: u32,
    pub api_backoff: u32,
    pub use_websocket: bool,
    pub ws_timeout: Duration,
    pub ws_reconnect_delay: u64,
    pub description: Option<String>,
}

impl std::fmt::Display for SigbotPolymarketConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SigbotPolymarketConfig=(
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

impl SigbotPolymarketConfig {
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
            maker_address: plain_config.get("maker_address").map(|s| s.to_string()),
            signer_address: plain_config.get("signer_address").map(|s| s.to_string()),
            taker_address: plain_config.get("taker_address").map(|s| s.to_string()),
            fee_rate_bps: plain_config.get("fee_rate_bps").and_then(|s| s.parse::<u32>().ok()),
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
struct PolymarketOrderBookResponse {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct PolymarketMarketResponse {
    id: String,
    question: String,
    slug: String,
    #[serde(rename = "conditionId")]
    condition_id: String,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    #[serde(rename = "outcomes")]
    outcomes: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct PolymarketMarketsResponse {
    data: Vec<PolymarketMarketResponse>,
}

#[derive(Debug, Deserialize)]
struct PolymarketOrderResponse {
    success: bool,
    #[serde(rename = "errorMsg")]
    error_msg: Option<String>,
    #[serde(rename = "orderId")]
    order_id: Option<String>,
    #[serde(rename = "orderHashes")]
    order_hashes: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct PolymarketOrder {
    id: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    side: String,
    price: String,
    size: String,
    status: String,
    #[serde(rename = "createdAt")]
    created_at: Option<String>,
    #[serde(rename = "filledSize")]
    filled_size: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PolymarketOrdersResponse {
    data: Vec<PolymarketOrder>,
}

// Order structures according to Polymarket CLOB API documentation
#[derive(Debug, Serialize, Deserialize)]
struct PolymarketSignedOrder {
    salt: String,
    maker: String,
    signer: String,
    taker: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    expiration: String,
    nonce: String,
    #[serde(rename = "feeRateBps")]
    fee_rate_bps: String,
    side: String,
    #[serde(rename = "signatureType")]
    signature_type: i32,
    signature: String,
}

#[derive(Debug, Serialize)]
struct PolymarketOrderRequest {
    order: PolymarketSignedOrder,
    owner: String,
    #[serde(rename = "orderType")]
    order_type: String, // "FOK", "GTC", "GTD"
}

#[derive(Debug, Deserialize)]
struct PolymarketOpenOrder {
    #[serde(rename = "associate_trades")]
    associate_trades: Vec<String>,
    id: String,
    status: String,
    market: String,
    #[serde(rename = "original_size")]
    original_size: String,
    outcome: String,
    #[serde(rename = "maker_address")]
    maker_address: String,
    owner: String,
    price: String,
    side: String,
    #[serde(rename = "size_matched")]
    size_matched: String,
    #[serde(rename = "asset_id")]
    asset_id: String,
    expiration: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(rename = "created_at")]
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct PolymarketGetOrderResponse {
    order: PolymarketOpenOrder,
}

#[derive(Debug, Serialize, Deserialize)]
struct PolymarketCancelOrderRequest {
    #[serde(rename = "orderID")]
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct PolymarketCancelOrderResponse {
    canceled: Vec<String>,
    #[serde(rename = "notCanceled")]
    not_canceled: std::collections::HashMap<String, String>,
}

pub struct SigbotPolymarketClient {
    config: SigbotPolymarketConfig,
    http_client: Arc<Client>,
    // Kline store for caching market data
    kline_store: Arc<dyn ICache<Vec<KlineModel>>>,
}

impl SigbotPolymarketClient {
    pub const KIND: &'static str = "POLYMARKET"; // ExchangeProvider::POLYMARKET

    pub async fn new(config: &SigbotPolymarketConfig, kline_store: Box<dyn ICache<Vec<KlineModel>>>) -> Arc<Self> {
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
        // Polymarket uses API key in Authorization header
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", self.config.api_key).parse().unwrap(),
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

    // Get maker's exchange nonce from Polymarket API
    async fn get_maker_nonce(&self, maker_address: &str) -> Result<String, Error> {
        // According to Polymarket API, nonce can be fetched from user info endpoint
        // For now, we'll use a placeholder - in production this should fetch from API
        let url = format!("{}/user", self.get_api_endpoint());
        let headers = self.build_auth_headers();

        let response = self
            .execute_with_retry(|| {
                self.http_client
                    .get(&url)
                    .headers(headers.clone())
                    .query(&[("address", maker_address)])
                    .send()
            })
            .await
            .context("Failed to get user nonce")?;

        let status = response.status();
        if !status.is_success() {
            // If API doesn't support this endpoint, return default nonce
            info!("Could not fetch nonce from API, using default");
            return Ok("0".to_string());
        }

        // Try to parse response for nonce
        // Note: Actual response structure may vary, this is a placeholder
        let _response_text = response.text().await.unwrap_or_default();
        // For now, return "0" as default nonce
        // TODO: Parse actual nonce from response when API structure is known
        Ok("0".to_string())
    }

    // Sign order using Web3 signature
    // This is a placeholder - actual implementation requires Web3 library (ethers-rs, etc.)
    fn sign_order(&self, _order: &PolymarketSignedOrder) -> Result<String, Error> {
        // TODO: Implement actual Web3 signing
        // This requires:
        // 1. Load private key from config
        // 2. Create order hash
        // 3. Sign hash with private key
        // 4. Return hex-encoded signature

        // For now, return error indicating signature is required
        Err(Error::msg(
            "Order signing requires Web3 library integration. \
            Please implement Web3 signing using ethers-rs or similar library. \
            The order needs to be hashed and signed with the private key corresponding to the signer address.",
        ))
    }
}

#[async_trait]
impl ISigbotExchangeClient for SigbotPolymarketClient {
    fn provider(&self) -> ExchangeProvider {
        ExchangeProvider::POLYMARKET
    }

    async fn init(&self) {
        info!("Starting Polymarket exchange client with config={}", self.config);

        // Test API connection by checking health endpoint
        let health_url = format!("{}/health", self.get_api_endpoint());
        match self.http_client.get(&health_url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    info!(
                        "Polymarket API connection test successful with configId={}",
                        self.config.id
                    );
                } else {
                    error!(
                        "Polymarket API connection test failed with status: {}",
                        response.status()
                    );
                }
            }
            Err(e) => {
                error!("Failed to test Polymarket API connection: {:?}", e);
            }
        }

        if self.config.use_websocket {
            info!("WebSocket support is enabled but not yet fully implemented for Polymarket");
        }
    }

    async fn close(&self) {
        info!("Closing Polymarket exchange client with {}", self.config);
        // HTTP client doesn't need explicit cleanup
    }

    // see:https://docs.polymarket.com/developers/CLOB/orders/create-order
    async fn enter_position(&self, signal: PlaceTradeSignal) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;
        info!("Entry position request for Polymarket: {:?}", signal);

        // According to Polymarket CLOB API documentation:
        // POST /order requires: order (signed object), owner (api key), orderType (FOK/GTC/GTD)
        // See: https://docs.polymarket.com/developers/CLOB/orders/create-order

        let side = signal.enter_pos.side.to_side_str();
        let price = signal
            .enter_pos
            .price
            .ok_or_else(|| Error::msg("Price is required for Polymarket orders"))?;
        let size = signal.enter_pos.quantity;
        let token_id = &signal.enter_pos.symbol;

        // Determine order type from signal (default to GTC if not specified)
        let order_type = match signal.enter_pos.order_type {
            sigbot_types::modules::exchange::models::trade_position::OrderType::MARKET => "FOK", // Fill-Or-Kill for market orders
            sigbot_types::modules::exchange::models::trade_position::OrderType::LIMITED => "GTC", // Good-Till-Cancelled for limit orders
        };

        // Get maker and signer addresses from config
        let maker_address = self
            .config
            .maker_address
            .as_ref()
            .ok_or_else(|| Error::msg("maker_address is required in configuration"))?;
        let signer_address = self.config.signer_address.as_ref().unwrap_or(maker_address); // Default to maker if signer not specified
        let taker_address = self
            .config
            .taker_address
            .as_ref()
            .cloned()
            .unwrap_or_else(|| "0x0000000000000000000000000000000000000000".to_string()); // Zero address for limit orders

        // Get fee rate (default to 0 if not specified)
        let fee_rate_bps = self.config.fee_rate_bps.unwrap_or(0);

        // Calculate makerAmount and takerAmount based on side
        // For BUY: makerAmount is USDC (price * size), takerAmount is tokens (size)
        // For SELL: makerAmount is tokens (size), takerAmount is USDC (price * size)
        let (maker_amount, taker_amount) = if side == "BUY" {
            (price * size, size)
        } else {
            (size, price * size)
        };

        // Generate salt (use timestamp + nanoseconds for unique order)
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();

        // Get current timestamp for expiration (default to 1 day from now)
        let expiration = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 86400; // 24 hours

        // Get maker's exchange nonce
        let nonce = self.get_maker_nonce(maker_address).await?;

        // Convert side to Polymarket format (0 for BUY, 1 for SELL)
        let side_enum = if side == "BUY" { "0" } else { "1" };

        // Build unsigned order
        let unsigned_order = PolymarketSignedOrder {
            salt: salt.clone(),
            maker: maker_address.clone(),
            signer: signer_address.clone(),
            taker: taker_address.clone(),
            token_id: token_id.to_string(),
            maker_amount: maker_amount.to_string(),
            taker_amount: taker_amount.to_string(),
            expiration: expiration.to_string(),
            nonce: nonce.clone(),
            fee_rate_bps: fee_rate_bps.to_string(),
            side: side_enum.to_string(),
            signature_type: 1,        // EIP-712 signature type
            signature: String::new(), // Will be filled after signing
        };

        // Sign the order
        let signature = self.sign_order(&unsigned_order)?;

        // Build signed order
        let signed_order = PolymarketSignedOrder {
            signature,
            ..unsigned_order
        };

        // Build order request
        let order_request = PolymarketOrderRequest {
            order: signed_order,
            owner: self.config.api_key.clone(),
            order_type: order_type.to_string(),
        };

        // Send order to Polymarket API
        let url = format!("{}/order", self.get_api_endpoint());
        let headers = self.build_auth_headers();

        info!(
            "[OPEN_POS] Placing order on Polymarket - token_id={}, side={}, price={}, size={}, order_type={}",
            token_id, side, price, size, order_type
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
                "Polymarket API returned error status: {} - {}",
                status, response_text
            )));
        }

        let order_response: PolymarketOrderResponse =
            serde_json::from_str(&response_text).context("Failed to parse order response")?;

        if !order_response.success {
            let error_msg = order_response.error_msg.unwrap_or_else(|| "Unknown error".to_string());
            return Err(Error::msg(format!("Order placement failed: {}", error_msg)));
        }

        let order_id = order_response
            .order_id
            .ok_or_else(|| Error::msg("Order ID not found in response"))?
            .parse::<u64>()
            .map_err(|_| Error::msg("Invalid order ID format"))?;

        info!(
            "[OPEN_POS] Order placed successfully - order_id={}, order_hashes={:?}",
            order_id, order_response.order_hashes
        );

        Ok(TradeResult {
            success: true,
            order_id,
            message: order_response
                .order_hashes
                .map(|hashes| format!("Order hashes: {:?}", hashes)),
        })
    }

    // see:https://docs.polymarket.com/developers/CLOB/orders/cancel-orders
    async fn exit_loss_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;
        info!(
            "Exit loss position request for Polymarket: order_id={}, signal={:?}",
            original_order_id, signal
        );

        // Cancel the original order according to Polymarket CLOB API documentation
        // DELETE /order with orderID in request body
        // See: https://docs.polymarket.com/developers/CLOB/orders/cancel-orders
        let cancel_url = format!("{}/order", self.get_api_endpoint());
        let headers = self.build_auth_headers();

        let cancel_request = PolymarketCancelOrderRequest {
            order_id: original_order_id.to_string(),
        };

        let response = self
            .execute_with_retry(|| {
                self.http_client
                    .delete(&cancel_url)
                    .headers(headers.clone())
                    .json(&cancel_request)
                    .send()
            })
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

        let cancel_response: PolymarketCancelOrderResponse =
            response.json().await.context("Failed to parse cancel order response")?;

        if cancel_response.canceled.contains(&original_order_id.to_string()) {
            info!("Successfully cancelled order {}", original_order_id);
            Ok(TradeResult {
                success: true,
                order_id: original_order_id,
                message: Some("Order cancelled successfully".to_string()),
            })
        } else {
            let error_msg = cancel_response
                .not_canceled
                .get(&original_order_id.to_string())
                .cloned()
                .unwrap_or_else(|| "Unknown error".to_string());
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
            "Exit profit position request for Polymarket: order_id={}, signal={:?}",
            original_order_id, signal
        );

        // Same as exit_loss_position - cancel the original order
        let cancel_url = format!("{}/order", self.get_api_endpoint());
        let headers = self.build_auth_headers();

        let cancel_request = PolymarketCancelOrderRequest {
            order_id: original_order_id.to_string(),
        };

        let response = self
            .execute_with_retry(|| {
                self.http_client
                    .delete(&cancel_url)
                    .headers(headers.clone())
                    .json(&cancel_request)
                    .send()
            })
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

        let cancel_response: PolymarketCancelOrderResponse =
            response.json().await.context("Failed to parse cancel order response")?;

        if cancel_response.canceled.contains(&original_order_id.to_string()) {
            info!("Successfully cancelled order {}", original_order_id);
            Ok(TradeResult {
                success: true,
                order_id: original_order_id,
                message: Some("Order cancelled successfully".to_string()),
            })
        } else {
            let error_msg = cancel_response
                .not_canceled
                .get(&original_order_id.to_string())
                .cloned()
                .unwrap_or_else(|| "Unknown error".to_string());
            Err(Error::msg(format!("Failed to cancel order: {}", error_msg)))
        }
    }

    async fn search_symbols(&self, query: &str, _sec_type: Option<&str>) -> Result<Vec<SymbolInfo>, Error> {
        info!("Searching symbols on Polymarket - query={}", query);

        // Use Gamma API for market search
        // Note: Gamma API endpoint might be different, adjust based on actual API
        let url = format!("{}/markets", self.get_api_endpoint());
        let headers = self.build_auth_headers();

        let response = self
            .execute_with_retry(|| {
                self.http_client
                    .get(&url)
                    .headers(headers.clone())
                    .query(&[("q", query)])
                    .send()
            })
            .await
            .context("Failed to search markets")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "Polymarket API returned error status: {} - {}",
                status, error_text
            )));
        }

        let markets: PolymarketMarketsResponse = response.json().await.context("Failed to parse markets response")?;

        let symbols: Vec<SymbolInfo> = markets
            .data
            .into_iter()
            .map(|market| SymbolInfo {
                symbol: market.condition_id.clone(),
                contract_id: Some(market.id.clone()),
                name: Some(market.question.clone()),
                exchange: Some("Polymarket".to_string()),
                sec_type: Some("PREDICTION".to_string()),
                currency: Some("USDC".to_string()),
                description: Some(market.slug),
            })
            .collect();

        info!("Found {} symbols matching query '{}'", symbols.len(), query);
        Ok(symbols)
    }

    async fn get_orders(&self, _account_id: Option<&str>, _filters: Option<&str>) -> Result<Vec<OrderInfo>, Error> {
        info!("Getting orders from Polymarket");

        // According to Polymarket CLOB API documentation:
        // GET /data/order/<order_hash> - Get single order by id
        // For getting active orders, we need to use a different endpoint or iterate through order IDs
        // See: https://docs.polymarket.com/developers/CLOB/orders/get-order

        // Note: Polymarket API doesn't have a direct "get all orders" endpoint
        // This would typically require maintaining a list of order IDs or using WebSocket subscriptions
        // For now, return an error indicating this limitation
        Err(Error::msg(
            "Polymarket CLOB API requires order hash to get individual orders. \
            Use GET /data/order/<order_hash> to get specific order details. \
            To get active orders, maintain a list of order IDs or use WebSocket subscriptions. \
            See: https://docs.polymarket.com/developers/CLOB/orders/get-order",
        ))
    }
}

#[async_trait]
impl ISigbotPredictExchangeClient for SigbotPolymarketClient {}
