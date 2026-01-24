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

//! Binance market data tools for agent use
//!
//! This module provides tools for fetching market data from Binance exchange,
//! including current prices, klines, volume data, and order book information.

use adk_core::{Result as AdkResult, Tool, ToolContext};
use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use serde_json::{json, Value};
use sigbot_core::exchange::client::exchange_factory::SigbotExchangeClientFactory;
use sigbot_core::exchange::client::ISigbotOrderBookExchangeClient;
use std::sync::Arc;

/// Tool for fetching current market price from Binance
pub struct BinanceMarketDataTool;

impl BinanceMarketDataTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BinanceMarketDataTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for BinanceMarketDataTool {
    fn name(&self) -> &str {
        "binance_market_data"
    }

    fn description(&self) -> &str {
        "Get current market price for a trading symbol from Binance exchange. \
         Returns the current price and timestamp."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Trading symbol (e.g., 'BTCUSDT', 'ETHUSDT')"
                }
            },
            "required": ["symbol"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let symbol = args["symbol"]
            .as_str()
            .ok_or_else(|| Error::msg("Missing 'symbol' parameter"))?;

        info!("BinanceMarketDataTool: Fetching price for symbol={}", symbol);

        // Get Binance client from factory
        let client = SigbotExchangeClientFactory::get_implementation("BINANCE")
            .await
            .map_err(|e| Error::msg(format!("Failed to get Binance client: {}", e)))?;

        // Fetch current price
        let price_model = client
            .get_current_price(symbol)
            .await
            .map_err(|e| Error::msg(format!("Failed to fetch price: {}", e)))?;

        debug!(
            "BinanceMarketDataTool: Got price={} for symbol={}",
            price_model.price, symbol
        );

        Ok(json!({
            "symbol": symbol,
            "price": price_model.price,
            "timestamp": price_model.time,
            "source": "binance"
        }))
    }
}

/// Tool for fetching kline/candlestick data from Binance
pub struct BinanceKlineTool;

impl BinanceKlineTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BinanceKlineTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for BinanceKlineTool {
    fn name(&self) -> &str {
        "binance_kline"
    }

    fn description(&self) -> &str {
        "Get historical kline/candlestick data from Binance. \
         Returns OHLCV (Open, High, Low, Close, Volume) data for the specified interval."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Trading symbol (e.g., 'BTCUSDT')"
                },
                "interval": {
                    "type": "string",
                    "description": "Kline interval (e.g., '1m', '5m', '15m', '30m', '1h', '4h', '1d')",
                    "enum": ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of klines to fetch (default: 100, max: 1000)",
                    "default": 100
                },
                "start_time": {
                    "type": "integer",
                    "description": "Start time in milliseconds (optional)"
                },
                "end_time": {
                    "type": "integer",
                    "description": "End time in milliseconds (optional)"
                }
            },
            "required": ["symbol", "interval"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let symbol = args["symbol"]
            .as_str()
            .ok_or_else(|| Error::msg("Missing 'symbol' parameter"))?;
        let interval = args["interval"]
            .as_str()
            .ok_or_else(|| Error::msg("Missing 'interval' parameter"))?;
        let limit = args["limit"].as_u64().unwrap_or(100) as u32;
        let start_time = args["start_time"].as_i64();
        let end_time = args["end_time"].as_i64();

        info!(
            "BinanceKlineTool: Fetching klines for symbol={}, interval={}, limit={}",
            symbol, interval, limit
        );

        // Get Binance client
        let client = SigbotExchangeClientFactory::get_implementation("BINANCE")
            .await
            .map_err(|e| Error::msg(format!("Failed to get Binance client: {}", e)))?;

        // Fetch klines
        let klines = client
            .get_klines(symbol, interval, start_time, end_time, limit)
            .await
            .map_err(|e| Error::msg(format!("Failed to fetch klines: {}", e)))?;

        debug!("BinanceKlineTool: Got {} klines for symbol={}", klines.len(), symbol);

        // Convert klines to JSON
        let klines_json: Vec<Value> = klines
            .iter()
            .map(|k| {
                json!({
                    "open_time": k.open_time,
                    "open": k.open_price,
                    "high": k.high_price,
                    "low": k.low_price,
                    "close": k.close_price,
                    "volume": k.volume,
                    "close_time": k.close_time
                })
            })
            .collect();

        Ok(json!({
            "symbol": symbol,
            "interval": interval,
            "klines": klines_json,
            "count": klines.len(),
            "source": "binance"
        }))
    }
}

/// Tool for fetching trading volume data from Binance
pub struct BinanceVolumeTool;

impl BinanceVolumeTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BinanceVolumeTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for BinanceVolumeTool {
    fn name(&self) -> &str {
        "binance_volume"
    }

    fn description(&self) -> &str {
        "Get trading volume data and analysis from Binance. \
         Returns volume statistics including total volume, buy/sell ratio, and volume trend."
    }

    fn parameters_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Trading symbol (e.g., 'BTCUSDT')"
                },
                "interval": {
                    "type": "string",
                    "description": "Time interval for volume analysis (default: '1h')",
                    "default": "1h"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of periods to analyze (default: 24)",
                    "default": 24
                }
            },
            "required": ["symbol"]
        }))
    }

    async fn execute(&self, _ctx: Arc<dyn ToolContext>, args: Value) -> AdkResult<Value> {
        let symbol = args["symbol"]
            .as_str()
            .ok_or_else(|| Error::msg("Missing 'symbol' parameter"))?;
        let interval = args["interval"].as_str().unwrap_or("1h");
        let limit = args["limit"].as_u64().unwrap_or(24) as u32;

        info!(
            "BinanceVolumeTool: Analyzing volume for symbol={}, interval={}, limit={}",
            symbol, interval, limit
        );

        // Get Binance client
        let client = SigbotExchangeClientFactory::get_implementation("BINANCE")
            .await
            .map_err(|e| Error::msg(format!("Failed to get Binance client: {}", e)))?;

        // Fetch klines to get volume data
        let klines = client
            .get_klines(symbol, interval, None, None, limit)
            .await
            .map_err(|e| Error::msg(format!("Failed to fetch klines for volume: {}", e)))?;

        // Calculate volume statistics
        let total_volume: f64 = klines.iter().map(|k| k.volume).sum();
        let avg_volume = if !klines.is_empty() {
            total_volume / klines.len() as f64
        } else {
            0.0
        };

        // Calculate volume trend (comparing recent vs older periods)
        let mid_point = klines.len() / 2;
        let recent_volume: f64 = klines.iter().skip(mid_point).map(|k| k.volume).sum();
        let older_volume: f64 = klines.iter().take(mid_point).map(|k| k.volume).sum();
        let volume_trend = if older_volume > 0.0 {
            ((recent_volume - older_volume) / older_volume) * 100.0
        } else {
            0.0
        };

        debug!(
            "BinanceVolumeTool: Total volume={}, avg={}, trend={}%",
            total_volume, avg_volume, volume_trend
        );

        Ok(json!({
            "symbol": symbol,
            "interval": interval,
            "periods_analyzed": klines.len(),
            "total_volume": total_volume,
            "average_volume": avg_volume,
            "volume_trend_percent": volume_trend,
            "trend_direction": if volume_trend > 5.0 { "increasing" } else if volume_trend < -5.0 { "decreasing" } else { "stable" },
            "source": "binance"
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_metadata() {
        let market_tool = BinanceMarketDataTool::new();
        assert_eq!(market_tool.name(), "binance_market_data");
        assert!(market_tool.parameters_schema().is_some());

        let kline_tool = BinanceKlineTool::new();
        assert_eq!(kline_tool.name(), "binance_kline");
        assert!(kline_tool.parameters_schema().is_some());

        let volume_tool = BinanceVolumeTool::new();
        assert_eq!(volume_tool.name(), "binance_volume");
        assert!(volume_tool.parameters_schema().is_some());
    }
}
