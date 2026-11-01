// SPDX-License-Identifier: GPL-3.0-only

use crate::exchange::IDecentralizedExchange;
use crate::exchange::IExchange;
use crate::exchange::IOrderBookExchange;
use anyhow::Error;
use async_trait::async_trait;
use common_context::IProvider;
use common_telemetry::info;
use sigbot_types::exchange::exchange::ExchangeInfo;
use sigbot_types::exchange::exchange::ExchangeProvider;
use sigbot_types::exchange::models::trade_market::KlineModel;
use sigbot_types::exchange::models::trade_market::OrderInfo;
use sigbot_types::exchange::models::trade_market::PriceModel;
use sigbot_types::exchange::models::trade_market::SymbolInfo;
use sigbot_types::exchange::models::trade_position::ExitTradePosition;
use sigbot_types::exchange::models::trade_position::PlaceTradeSignal;
use sigbot_types::exchange::models::trade_position::TradeResult;
use sigbot_types::Uuid;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct HyperliquidExchangeConfig {
    pub id: Uuid,
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

impl std::fmt::Display for HyperliquidExchangeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HyperliquidExchangeConfig=(
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

impl HyperliquidExchangeConfig {
    pub fn from_exchange(exchange: Arc<ExchangeInfo>) -> Self {
        let plain_config = exchange.properties.as_ref().expect("Plain configuration is required");
        let secret_config = exchange.secrets.as_ref().expect("Secret configuration is required");
        Self {
            id: exchange.base.id.expect("Exchange ID is required"),
            name: exchange.name.as_ref().expect("Exchange name is required").clone(),
            spot_api_mainnet_endpoint: plain_config
                .get("spot_api_mainnet_endpoint")
                .expect("Spot API mainnet endpoint is required")
                .clone(),
            spot_api_testnet_endpoint: plain_config
                .get("spot_api_testnet_endpoint")
                .expect("Spot API testnet endpoint is required")
                .clone(),
            spot_ws_mainnet_endpoint: plain_config
                .get("spot_ws_mainnet_endpoint")
                .expect("Spot WebSocket mainnet endpoint is required")
                .clone(),
            spot_ws_testnet_endpoint: plain_config
                .get("spot_ws_testnet_endpoint")
                .expect("Spot WebSocket testnet endpoint is required")
                .clone(),
            spot_market_api_mainnet_endpoint: plain_config
                .get("spot_market_api_mainnet_endpoint")
                .expect("Spot market API mainnet endpoint is required")
                .clone(),
            spot_market_ws_mainnet_endpoint: plain_config
                .get("spot_market_ws_mainnet_endpoint")
                .expect("Spot market WebSocket mainnet endpoint is required")
                .clone(),
            usds_api_mainnet_endpoint: plain_config
                .get("usds_api_mainnet_endpoint")
                .expect("USDS API mainnet endpoint is required")
                .clone(),
            usds_api_testnet_endpoint: plain_config
                .get("usds_api_testnet_endpoint")
                .expect("USDS API testnet endpoint is required")
                .clone(),
            usds_ws_mainnet_endpoint: plain_config
                .get("usds_ws_mainnet_endpoint")
                .expect("USDS WebSocket mainnet endpoint is required")
                .clone(),
            usds_ws_testnet_endpoint: plain_config
                .get("usds_ws_testnet_endpoint")
                .expect("USDS WebSocket testnet endpoint is required")
                .clone(),
            usds_market_api_mainnet_endpoint: plain_config
                .get("usds_market_api_mainnet_endpoint")
                .expect("USDS market API mainnet endpoint is required")
                .clone(),
            usds_market_ws_mainnet_endpoint: plain_config
                .get("usds_market_ws_mainnet_endpoint")
                .expect("USDS market WebSocket mainnet endpoint is required")
                .clone(),
            api_key: secret_config.get("api_key").expect("API key is required").clone(),
            api_secret: secret_config.get("api_secret").expect("API secret is required").clone(),
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

pub struct HyperliquidExchange {
    config: HyperliquidExchangeConfig,
}

impl HyperliquidExchange {
    pub const KIND: &'static str = "HYPERLIQUID"; // ExchangeProvider::Hyperliquid

    pub async fn new(exchange: Arc<ExchangeInfo>) -> Arc<Self> {
        let config = HyperliquidExchangeConfig::from_exchange(exchange.clone());
        Arc::new(Self { config })
    }
}

// see:https://github.com/wl4g-blockchain/hyperliquid-rust-sdk/blob/master/src/bin/ws_candles.rs
#[async_trait]
impl IExchange for HyperliquidExchange {
    fn provider(&self) -> ExchangeProvider {
        ExchangeProvider::Hyperliquid
    }

    async fn enter_position(&self, signal: PlaceTradeSignal) -> Result<TradeResult, Error> {
        signal.validate().map_err(Error::msg)?;
        Err(Error::msg(
            "Hyperliquid legacy position trading is disabled; use the scoped execution adapter",
        ))
    }

    async fn exit_loss_position(
        &self,
        _original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(Error::msg)?;
        Err(Error::msg(
            "Hyperliquid legacy stop-loss trading is disabled; use the scoped execution adapter",
        ))
    }

    async fn exit_profit_position(
        &self,
        _original_order_id: u64,
        signal: &ExitTradePosition,
    ) -> Result<TradeResult, Error> {
        signal.validate().map_err(Error::msg)?;
        Err(Error::msg(
            "Hyperliquid legacy take-profit trading is disabled; use the scoped execution adapter",
        ))
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
impl IOrderBookExchange for HyperliquidExchange {
    async fn get_current_price(&self, symbol: &str) -> Result<PriceModel, Error> {
        let transport = if self.config.use_websocket { "WebSocket" } else { "REST" };
        Err(Error::msg(format!(
            "Hyperliquid {transport} price lookup for {symbol} is unavailable on the legacy adapter; use MarketExchanges"
        )))
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        _start_time: Option<i64>,
        _end_time: Option<i64>,
        _limit: u32,
    ) -> Result<Vec<KlineModel>, Error> {
        let transport = if self.config.use_websocket { "WebSocket" } else { "REST" };
        Err(Error::msg(format!(
            "Hyperliquid {transport} kline lookup for {symbol}/{interval} is unavailable on the legacy adapter; use MarketExchanges"
        )))
    }
}

#[async_trait]
impl IDecentralizedExchange for HyperliquidExchange {}

#[async_trait::async_trait]
impl IProvider for HyperliquidExchange {
    async fn close(&self) -> anyhow::Result<()> {
        info!("Closing Hyperliquid operator with {}", self.config);
        Ok(())
    }
    async fn init(&self) -> anyhow::Result<()> {
        info!("Starting Hyperliquid exchange manager with config={}", self.config);
        let transport = if self.config.use_websocket { "WebSocket" } else { "REST" };
        Err(anyhow::anyhow!("Hyperliquid {transport} transport is not implemented"))
    }
}
