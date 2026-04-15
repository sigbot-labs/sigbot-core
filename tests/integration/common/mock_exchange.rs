// SPDX-License-Identifier: GNU GENERAL LICENSE Version 3
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

//! Mock Exchange Client for E2E Tests
//!
//! Simulates exchange API responses for testing order execution
//! without connecting to real exchanges.

use anyhow::Result;
use serde_json;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Mock exchange configuration
#[derive(Debug, Clone, Default)]
pub struct MockExchangeConfig {
    pub base_url: String,
    pub api_key: String,
    pub api_secret: String,
}

/// Mock exchange client
#[derive(Debug, Clone)]
pub struct MockExchangeClient {
    #[allow(dead_code)] // Config kept for future real exchange implementation
    config: MockExchangeConfig,
    trade_counter: Arc<RwLock<i64>>,
}

impl MockExchangeClient {
    /// Create new mock exchange with config
    pub fn new(config: MockExchangeConfig) -> Self {
        Self {
            config,
            trade_counter: Arc::new(RwLock::new(0)),
        }
    }

    /// Get next trade ID
    async fn next_trade_id(&self) -> i64 {
        let mut counter = self.trade_counter.write().await;
        *counter += 1;
        *counter
    }

    /// Execute a trade (simulated)
    pub async fn execute_trade(&self, symbol: &str, side: &str, quantity: f64) -> Result<serde_json::Value> {
        // Simulate latency
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let trade_id = self.next_trade_id().await;
        let price = match symbol {
            "BTCUSDT" => 50000.0,
            "ETHUSDT" => 3000.0,
            "BNBUSDT" => 300.0,
            _ => 100.0,
        };

        let fee = quantity * price * 0.001; // 0.1% fee

        Ok(serde_json::json!({
            "trade_id": trade_id,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "fee": fee,
            "status": "FILLED",
            "timestamp": chrono::Utc::now().to_rfc3339()
        }))
    }
}

/// Create a mock exchange client for tests
pub fn create_mock_exchange() -> Arc<MockExchangeClient> {
    Arc::new(MockExchangeClient::new(MockExchangeConfig::default()))
}
