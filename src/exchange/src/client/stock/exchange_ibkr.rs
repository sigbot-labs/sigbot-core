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

use crate::client::{
    exchange_factory::ISigbotExchangeClient, stock::ISigbotStockExchangeClient, ISigbotOrderBookExchangeClient,
};
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{error, info};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sigbot_types::{
    modules::exchange::exchange::{ExchangeInfo, ExchangeProvider},
    modules::exchange::models::{
        trade_market::{KlineModel, OrderInfo, PriceModel, SymbolInfo},
        trade_position::{PlaceTradeSignal, ExitTradePosition, TradeResult},
    },
};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct SigbotIBKRClientConfig {
    pub id: i64,
    pub name: String,
    // API endpoints
    pub api_endpoint: String,
    pub ws_endpoint: Option<String>,
    // Account configuration
    pub account_id: String,
    // Secrets configuration
    pub api_key: String,
    pub api_secret: Option<String>,
    // Generic configuration
    pub api_timeout: Duration,
    pub api_retries: u32,
    pub api_backoff: u32,
    pub use_websocket: bool,
    pub ws_timeout: Duration,
    pub ws_reconnect_delay: u64,
    pub description: Option<String>,
}

impl std::fmt::Display for SigbotIBKRClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SigbotIBKRConfig=(id={}, name={}, api_endpoint={}, ws_endpoint={:?}, account_id={})",
            self.id, self.name, self.api_endpoint, self.ws_endpoint, self.account_id
        )
    }
}

impl SigbotIBKRClientConfig {
    pub fn from_exchange(exchange: Arc<ExchangeInfo>) -> Self {
        let plain_config = exchange.properties.as_ref().expect("Plain configuration is required");
        let secret_config = exchange.secrets.as_ref().expect("Secret configuration is required");

        Self {
            id: exchange.base.id.expect("Exchange ID is required"),
            name: exchange.name.as_ref().expect("Exchange name is required").to_string(),
            api_endpoint: plain_config
                .get("api_endpoint")
                .expect("API endpoint is required")
                .to_string(),
            ws_endpoint: plain_config.get("ws_endpoint").map(|s| s.to_string()),
            account_id: plain_config
                .get("account_id")
                .expect("Account ID is required")
                .to_string(),
            api_key: secret_config.get("api_key").expect("API key is required").to_string(),
            api_secret: secret_config.get("api_secret").map(|s| s.to_string()),
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
                .unwrap_or(&"false".to_string())
                .parse::<bool>()
                .unwrap_or(false),
            ws_timeout: Duration::from_secs(
                plain_config
                    .get("ws_timeout")
                    .unwrap_or(&"30".to_string())
                    .parse::<u64>()
                    .unwrap_or(30),
            ),
            ws_reconnect_delay: plain_config
                .get("ws_reconnect_delay")
                .unwrap_or(&"5".to_string())
                .parse::<u64>()
                .unwrap_or(5),
            description: exchange.description.as_ref().map(|s| s.to_string()),
        }
    }
}

pub struct SigbotIBKRClient {
    config: SigbotIBKRClientConfig,
    http_client: Arc<Client>,
}

impl SigbotIBKRClient {
    pub async fn new(exchange: Arc<ExchangeInfo>) -> Arc<Self> {
        let config = SigbotIBKRClientConfig::from_exchange(exchange);

        // Build HTTP client with timeout and retry configuration
        let http_client = Client::builder()
            .timeout(config.api_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Arc::new(Self {
            config,
            http_client: Arc::new(http_client),
        })
    }

    fn build_auth_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
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
}

// see: https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-trading/
#[async_trait]
impl ISigbotExchangeClient for SigbotIBKRClient {
    fn provider(&self) -> ExchangeProvider {
        ExchangeProvider::IBKR
    }

    async fn init(&self) {
        info!("Starting IBKR exchange manager with config={}", self.config);

        // Test API connection
        let test_url = format!("{}/v1/api/iserver/auth/status", self.config.api_endpoint);
        let headers = self.build_auth_headers();

        match self.http_client.get(&test_url).headers(headers).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    info!("IBKR API connection test successful with configId={}", self.config.id);
                } else {
                    error!("IBKR API connection test failed with status: {}", response.status());
                }
            }
            Err(e) => {
                error!("Failed to test IBKR API connection: {:?}", e);
            }
        }

        if self.config.use_websocket {
            info!("WebSocket support is enabled but not yet implemented for IBKR");
        }
    }

    async fn close(&self) {
        info!("Closing IBKR operator with {}", self.config);
        // IBKR HTTP client doesn't need explicit cleanup
    }

    async fn enter_position(&self, signal: PlaceTradeSignal) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;

        // Convert order type to IBKR format
        let order_type = match signal.enter_pos.order_type {
            sigbot_types::modules::exchange::models::trade_position::OrderType::MARKET => "MKT",
            sigbot_types::modules::exchange::models::trade_position::OrderType::LIMITED => "LMT",
        };
        let side = signal.enter_pos.side.to_side_str();

        // TODO: Convert symbol to conid (contract ID)
        // This requires calling IBKR's contract search API first
        // For now, we'll assume the symbol is already a conid (as u64)
        let conid = signal.enter_pos.symbol.parse::<u64>().map_err(|_| {
            Error::msg(format!(
                "Symbol '{}' must be a valid IBKR conid (contract ID). Please convert symbol to conid first.",
                signal.enter_pos.symbol
            ))
        })?;

        // Determine time in force (default to DAY for now)
        let tif = "DAY".to_string();

        let order_request = IBKROrderRequest {
            conid,
            order_type: order_type.to_string(),
            side: side.to_string(),
            time_in_force: tif,
            quantity: signal.enter_pos.quantity,
            price: signal.enter_pos.price,
        };

        // IBKR requires the request body to be a JSON array
        let order_array = vec![order_request];

        let url = format!(
            "{}/v1/api/iserver/account/{}/orders",
            self.config.api_endpoint, self.config.account_id
        );
        let headers = self.build_auth_headers();

        info!(
            "[OPEN_POS] Opening position - conid={}, side={}, quantity={}, order_type={}",
            conid, side, signal.enter_pos.quantity, order_type
        );

        let response = self
            .execute_with_retry(|| {
                self.http_client
                    .post(&url)
                    .headers(headers.clone())
                    .json(&order_array)
                    .send()
            })
            .await
            .context("Failed to create order")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "IBKR API returned error status: {} - {}",
                status, error_text
            )));
        }

        // Check if response is an order reply message (array) or order confirmation (object)
        let response_text = response.text().await.context("Failed to read response body")?;

        // Try to parse as order reply message first (array)
        // Order reply messages are returned as an array
        if let Ok(reply_messages) = serde_json::from_str::<Vec<IBKROrderReplyMessage>>(&response_text) {
            if !reply_messages.is_empty() {
                let reply = &reply_messages[0];
                return Err(Error::msg(format!(
                    "Order requires confirmation: {} (message_id: {}). Use /iserver/reply/{} endpoint to confirm.",
                    reply.message.join("; "),
                    reply.id,
                    reply.id
                )));
            }
        }

        // Parse as order confirmation (object)
        let order_response: IBKROrderResponse =
            serde_json::from_str(&response_text).context("Failed to parse order response")?;

        let order_id_str = order_response
            .order_id
            .as_ref()
            .ok_or_else(|| Error::msg("Order ID not found in response"))?;

        let order_id = order_id_str
            .parse::<u64>()
            .map_err(|_| Error::msg(format!("Invalid order_id format: {}", order_id_str)))?;

        info!(
            "[OPEN_POS] Opened position - orderId={}, status={:?}",
            order_id, order_response.order_status
        );

        Ok(TradeResult {
            success: true,
            order_id,
            message: order_response.order_status,
        })
    }

    // see:https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-trading/#values-needed-43
    async fn exit_loss_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;

        // IBKR uses DELETE method to cancel orders
        let url = format!(
            "{}/v1/api/iserver/account/{}/order/{}",
            self.config.api_endpoint, self.config.account_id, original_order_id
        );
        let headers = self.build_auth_headers();

        info!(
            "[EXIT_LOSS] Canceling order - symbol={}, side={}, original_order_id={}, account_id={}",
            signal.symbol,
            signal.side.to_side_str(),
            original_order_id,
            self.config.account_id
        );

        let response = self
            .execute_with_retry(|| self.http_client.delete(&url).headers(headers.clone()).send())
            .await
            .context("Failed to cancel order")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "IBKR API returned error status: {} - {}",
                status, error_text
            )));
        }

        let cancel_response: IBKRCancelOrderResponse =
            response.json().await.context("Failed to parse cancel order response")?;

        info!(
            "[EXIT_LOSS] Canceled order - order_id={}, msg={:?}, account={:?}",
            cancel_response.order_id, cancel_response.msg, cancel_response.account
        );

        Ok(TradeResult {
            success: true,
            order_id: cancel_response.order_id,
            message: cancel_response.msg,
        })
    }

    async fn exit_profit_position(
        &self,
        original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(|e| Error::msg(e))?;

        // IBKR uses DELETE method to cancel orders
        let url = format!(
            "{}/v1/api/iserver/account/{}/order/{}",
            self.config.api_endpoint, self.config.account_id, original_order_id
        );
        let headers = self.build_auth_headers();

        info!(
            "[EXIT_PROFIT] Canceling order - symbol={}, side={}, original_order_id={}, account_id={}",
            signal.symbol,
            signal.side.to_side_str(),
            original_order_id,
            self.config.account_id
        );

        let response = self
            .execute_with_retry(|| self.http_client.delete(&url).headers(headers.clone()).send())
            .await
            .context("Failed to cancel order")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "IBKR API returned error status: {} - {}",
                status, error_text
            )));
        }

        let cancel_response: IBKRCancelOrderResponse =
            response.json().await.context("Failed to parse cancel order response")?;

        info!(
            "[EXIT_PROFIT] Canceled order - order_id={}, msg={:?}, account={:?}",
            cancel_response.order_id, cancel_response.msg, cancel_response.account
        );

        Ok(TradeResult {
            success: true,
            order_id: cancel_response.order_id,
            message: cancel_response.msg,
        })
    }

    async fn search_symbols(&self, query: &str, sec_type: Option<&str>) -> Result<Vec<SymbolInfo>, Error> {
        info!("Searching symbols - query={}, sec_type={:?}", query, sec_type);

        let url = format!("{}/v1/api/iserver/secdef/search", self.config.api_endpoint);
        let headers = self.build_auth_headers();

        let mut query_params = vec![("symbol", query.to_string())];
        if let Some(st) = sec_type {
            query_params.push(("secType", st.to_string()));
        } else {
            // Default to STK (stock) if not specified
            query_params.push(("secType", "STK".to_string()));
        }

        let response = self
            .execute_with_retry(|| {
                let mut request = self.http_client.get(&url).headers(headers.clone());
                for (key, value) in &query_params {
                    request = request.query(&[(key, value.as_str())]);
                }
                request.send()
            })
            .await
            .context("Failed to search symbols")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "IBKR API returned error status: {} - {}",
                status, error_text
            )));
        }

        let search_results: Vec<IBKRSymbolSearchResponse> = response
            .json()
            .await
            .context("Failed to parse symbol search response")?;

        let symbols: Vec<SymbolInfo> = search_results
            .into_iter()
            .map(|result| {
                // Extract sec_type from sections if available
                let sec_type_str = result
                    .sections
                    .as_ref()
                    .and_then(|sections| sections.first())
                    .map(|s| s.sec_type.clone())
                    .or_else(|| {
                        if sec_type.is_some() {
                            sec_type.map(|s| s.to_string())
                        } else {
                            Some("STK".to_string())
                        }
                    });

                SymbolInfo {
                    symbol: result.symbol.clone(),
                    contract_id: Some(result.conid),
                    name: result.company_name.clone(),
                    exchange: result.description.clone(),
                    sec_type: sec_type_str,
                    currency: None, // Not available in search response
                    description: result.company_header.clone(),
                }
            })
            .collect();

        info!("Found {} symbols matching query '{}'", symbols.len(), query);
        Ok(symbols)
    }

    async fn get_orders(&self, account_id: Option<&str>, filters: Option<&str>) -> Result<Vec<OrderInfo>, Error> {
        let target_account_id = account_id.unwrap_or(&self.config.account_id);
        info!(
            "Getting orders - account_id={}, filters={:?}",
            target_account_id, filters
        );

        let url = format!("{}/v1/api/iserver/account/orders", self.config.api_endpoint);
        let headers = self.build_auth_headers();

        let mut query_params = vec![
            ("accountId", target_account_id.to_string()),
            ("force", "true".to_string()),
        ];
        if let Some(f) = filters {
            query_params.push(("filters", f.to_string()));
        }

        let response = self
            .execute_with_retry(|| {
                let mut request = self.http_client.get(&url).headers(headers.clone());
                for (key, value) in &query_params {
                    request = request.query(&[(key, value.as_str())]);
                }
                request.send()
            })
            .await
            .context("Failed to get orders")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::msg(format!(
                "IBKR API returned error status: {} - {}",
                status, error_text
            )));
        }

        let order_list: IBKROrderListResponse = response.json().await.context("Failed to parse order list response")?;

        let orders: Vec<OrderInfo> = order_list
            .orders
            .into_iter()
            .filter_map(|order| {
                // Skip orders without order_id
                let order_id = order.order_id?;
                Some(OrderInfo {
                    order_id: order_id.to_string(),
                    symbol: order
                        .ticker
                        .or(order.description1)
                        .unwrap_or_else(|| "UNKNOWN".to_string()),
                    side: order.side.unwrap_or_else(|| "UNKNOWN".to_string()),
                    order_type: order
                        .order_type
                        .or(order.orig_order_type)
                        .unwrap_or_else(|| "UNKNOWN".to_string()),
                    status: order
                        .status
                        .or(order.order_ccp_status)
                        .unwrap_or_else(|| "UNKNOWN".to_string()),
                    quantity: order.total_size.unwrap_or(0.0),
                    filled_quantity: order.filled_quantity,
                    remaining_quantity: order.remaining_quantity,
                    price: order.price,
                    avg_price: order.avg_price.and_then(|p| p.parse::<f64>().ok()),
                    create_time: None, // Not available in IBKR response
                    update_time: order.last_execution_time_r,
                    time_in_force: order.time_in_force,
                })
            })
            .collect();

        info!("Retrieved {} orders for account {}", orders.len(), target_account_id);
        Ok(orders)
    }
}

#[async_trait]
impl ISigbotOrderBookExchangeClient for SigbotIBKRClient {
    async fn get_current_price(&self, symbol: &str) -> Result<PriceModel, Error> {
        info!("Getting current price for symbol={} from IBKR", symbol);

        let url = format!("{}/v1/api/marketdata/snapshot", self.config.api_endpoint);
        let headers = self.build_auth_headers();
        let symbol_str = symbol.to_string();

        let response = self
            .execute_with_retry(|| {
                self.http_client
                    .get(&url)
                    .headers(headers.clone())
                    .query(&[("conid", symbol_str.as_str())])
                    .send()
            })
            .await
            .context("Failed to get current price")?;

        let status = response.status();
        if !status.is_success() {
            return Err(Error::msg(format!("IBKR API returned error status: {}", status)));
        }

        let price_data: IBKRPriceResponse = response.json().await.context("Failed to parse price response")?;

        Ok(PriceModel {
            price: price_data
                .price
                .ok_or_else(|| Error::msg("No price available in response"))?,
            time: price_data
                .timestamp
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64),
        })
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Result<Vec<KlineModel>, Error> {
        info!(
            "Getting klines for symbol={} from IBKR with interval={}",
            symbol, interval
        );

        let url = format!("{}/v1/api/marketdata/history", self.config.api_endpoint);
        let headers = self.build_auth_headers();

        // Convert interval to IBKR format (e.g., "1min", "5min", "1day")
        let ibkr_interval = match interval {
            "1m" | "1min" => "1min",
            "5m" | "5min" => "5min",
            "15m" | "15min" => "15min",
            "30m" | "30min" => "30min",
            "1h" | "1hour" => "1hour",
            "1d" | "1day" => "1day",
            "1w" | "1week" => "1week",
            "1M" | "1month" => "1month",
            _ => return Err(Error::msg(format!("Unsupported interval: {}", interval))),
        };

        let mut query_params: Vec<(&str, String)> = vec![
            ("conid", symbol.to_string()),
            ("period", ibkr_interval.to_string()),
            ("bar", ibkr_interval.to_string()),
        ];

        if let Some(start) = start_time {
            query_params.push(("startTime", start.to_string()));
        }
        if let Some(end) = end_time {
            query_params.push(("endTime", end.to_string()));
        }
        query_params.push(("limit", limit.to_string()));

        let response = self
            .execute_with_retry(|| {
                let mut request = self.http_client.get(&url).headers(headers.clone());
                for (key, value) in &query_params {
                    request = request.query(&[(key, value.as_str())]);
                }
                request.send()
            })
            .await
            .context("Failed to get klines")?;

        let status = response.status();
        if !status.is_success() {
            return Err(Error::msg(format!("IBKR API returned error status: {}", status)));
        }

        let klines_data: Vec<IBKRKlineResponse> = response.json().await.context("Failed to parse klines response")?;

        let klines: Vec<KlineModel> = klines_data
            .into_iter()
            .map(|k| KlineModel {
                open_time: k.open_time,
                open_price: k.open_price,
                high_price: k.high_price,
                low_price: k.low_price,
                close_price: k.close_price,
                volume: k.volume,
                close_time: k.close_time,
            })
            .collect();

        Ok(klines)
    }
}

#[async_trait]
impl ISigbotStockExchangeClient for SigbotIBKRClient {
    async fn get_stock_orders(&self, account_id: Option<&str>, filters: Option<&str>) -> Result<Vec<OrderInfo>, Error> {
        unimplemented!()
    }
}

// IBKR API response models
// see:https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-market-data/#response-models

#[derive(Debug, Deserialize)]
struct IBKRPriceResponse {
    #[serde(rename = "last")]
    price: Option<f64>,
    #[serde(rename = "time")]
    timestamp: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct IBKRKlineResponse {
    #[serde(rename = "openTime")]
    open_time: u64,
    #[serde(rename = "open")]
    open_price: f64,
    #[serde(rename = "high")]
    high_price: f64,
    #[serde(rename = "low")]
    low_price: f64,
    #[serde(rename = "close")]
    close_price: f64,
    #[serde(rename = "volume")]
    volume: f64,
    #[serde(rename = "closeTime")]
    close_time: u64,
}

#[derive(Debug, Serialize)]
struct IBKROrderRequest {
    #[serde(rename = "conid")]
    conid: u64, // Contract ID - needs to be obtained from IBKR's contract search
    #[serde(rename = "orderType")]
    order_type: String, // "LMT" for limit, "MKT" for market
    side: String, // "BUY" or "SELL"
    #[serde(rename = "tif")]
    time_in_force: String, // "DAY" or "GTC"
    quantity: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<f64>, // Required for limit orders
}

#[derive(Debug, Deserialize)]
struct IBKROrderResponse {
    #[serde(rename = "order_id")]
    order_id: Option<String>, // IBKR returns order_id as string
    #[serde(rename = "order_status")]
    order_status: Option<String>,
    #[serde(rename = "encrypt_message")]
    #[allow(dead_code)]
    encrypt_message: Option<String>,
}

// Order reply message response (may be returned instead of order confirmation)
#[derive(Debug, Deserialize)]
struct IBKROrderReplyMessage {
    id: String,
    message: Vec<String>,
    #[serde(rename = "isSuppressed")]
    #[allow(dead_code)]
    is_suppressed: bool,
    #[serde(rename = "messageIds")]
    #[allow(dead_code)]
    message_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct IBKRSymbolSearchResponse {
    conid: String,
    #[serde(rename = "companyHeader")]
    company_header: Option<String>,
    #[serde(rename = "companyName")]
    company_name: Option<String>,
    symbol: String,
    description: Option<String>,
    restricted: Option<String>,
    sections: Option<Vec<IBKRSection>>,
}

#[derive(Debug, Deserialize)]
struct IBKRSection {
    #[serde(rename = "secType")]
    sec_type: String,
    months: Option<String>,
    exchange: Option<String>,
}

// IBKR order list response
#[derive(Debug, Deserialize)]
struct IBKROrderListResponse {
    orders: Vec<IBKROrderDetail>,
    snapshot: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct IBKROrderDetail {
    acct: Option<String>,
    conidex: Option<String>,
    conid: Option<u64>,
    account: Option<String>,
    #[serde(rename = "orderId")]
    order_id: Option<u64>,
    #[serde(rename = "cashCcy")]
    cash_ccy: Option<String>,
    #[serde(rename = "sizeAndFills")]
    size_and_fills: Option<String>,
    #[serde(rename = "orderDesc")]
    order_desc: Option<String>,
    description1: Option<String>,
    ticker: Option<String>,
    #[serde(rename = "secType")]
    sec_type: Option<String>,
    #[serde(rename = "listingExchange")]
    listing_exchange: Option<String>,
    #[serde(rename = "remainingQuantity")]
    remaining_quantity: Option<f64>,
    #[serde(rename = "filledQuantity")]
    filled_quantity: Option<f64>,
    #[serde(rename = "totalSize")]
    total_size: Option<f64>,
    #[serde(rename = "companyName")]
    company_name: Option<String>,
    status: Option<String>,
    #[serde(rename = "order_ccp_status")]
    order_ccp_status: Option<String>,
    #[serde(rename = "avgPrice")]
    avg_price: Option<String>,
    #[serde(rename = "origOrderType")]
    orig_order_type: Option<String>,
    #[serde(rename = "lastExecutionTime")]
    last_execution_time: Option<String>,
    #[serde(rename = "orderType")]
    order_type: Option<String>,
    #[serde(rename = "timeInForce")]
    time_in_force: Option<String>,
    #[serde(rename = "lastExecutionTime_r")]
    last_execution_time_r: Option<u64>,
    side: Option<String>,
    price: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct IBKRCancelOrderResponse {
    msg: Option<String>,
    #[serde(rename = "order_id")]
    order_id: u64,
    #[allow(dead_code)]
    conid: Option<u64>,
    #[allow(dead_code)]
    account: Option<String>,
}
