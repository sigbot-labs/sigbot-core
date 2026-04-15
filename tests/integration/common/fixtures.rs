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

//! Test Fixtures for E2E Tests
//!
//! Provides test data generators and fixtures for consistent,
//! reproducible test scenarios.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Market data fixture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataFixture {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Trade signal fixture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSignalFixture {
    pub signal_id: String,
    pub tenant_id: String,
    pub workflow_id: String,
    pub wallet_id: i64,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub price: Option<f64>,
    pub order_type: String,
}

/// Trade event fixture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeEventFixture {
    pub trade_id: i64,
    pub order_id: i64,
    pub signal_id: String,
    pub tenant_id: String,
    pub wallet_id: i64,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
}

/// Hyperparameter fixture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperparameterFixture {
    pub tenant_id: String,
    pub workflow_id: String,
    pub strategy_id: String,
    pub parameters: serde_json::Value,
}

/// Log entry fixture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFixture {
    pub workflow_id: String,
    pub node_id: String,
    pub level: String,
    pub content: Vec<String>,
}

/// Fixture generator
pub struct Fixtures;

impl Fixtures {
    /// Generate unique ID
    pub fn generate_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// Generate unique signal ID
    pub fn signal_id() -> String {
        format!("sig_{}", Uuid::new_v4())
    }

    /// Generate unique workflow ID
    pub fn workflow_id() -> String {
        format!("wf_{}", Uuid::new_v4())
    }

    /// Create market data fixture
    pub fn market_data(symbol: &str, price: f64) -> MarketDataFixture {
        MarketDataFixture {
            symbol: symbol.to_string(),
            timestamp: Utc::now(),
            open: price,
            high: price * 1.02,
            low: price * 0.98,
            close: price * 1.01,
            volume: 1000000.0,
        }
    }

    /// Create market data series
    pub fn market_data_series(symbol: &str, base_price: f64, count: usize) -> Vec<MarketDataFixture> {
        let mut data = Vec::with_capacity(count);

        for i in 0..count {
            let change = (i as f64 * 0.01) - (count as f64 * 0.005);
            let current_price = base_price * (1.0 + change);

            data.push(MarketDataFixture {
                symbol: symbol.to_string(),
                timestamp: Utc::now() - chrono::Duration::minutes((count - i) as i64),
                open: current_price,
                high: current_price * 1.02,
                low: current_price * 0.98,
                close: current_price * 1.01,
                volume: 1000000.0,
            });
        }

        data
    }

    /// Create trade signal fixture
    pub fn trade_signal(
        tenant_id: &str,
        workflow_id: &str,
        wallet_id: i64,
        symbol: &str,
        side: &str,
        quantity: f64,
        price: Option<f64>,
    ) -> TradeSignalFixture {
        TradeSignalFixture {
            signal_id: Self::signal_id(),
            tenant_id: tenant_id.to_string(),
            workflow_id: workflow_id.to_string(),
            wallet_id,
            exchange: "BINANCE".to_string(),
            symbol: symbol.to_string(),
            side: side.to_string(),
            quantity,
            price,
            order_type: if price.is_some() {
                "LIMIT"
            } else {
                "MARKET"
            }
            .to_string(),
        }
    }

    /// Create trade event fixture
    pub fn trade_event(signal: &TradeSignalFixture, order_id: i64, trade_id: i64) -> TradeEventFixture {
        TradeEventFixture {
            trade_id,
            order_id,
            signal_id: signal.signal_id.clone(),
            tenant_id: signal.tenant_id.clone(),
            wallet_id: signal.wallet_id,
            exchange: signal.exchange.clone(),
            symbol: signal.symbol.clone(),
            side: signal.side.clone(),
            price: signal.price.unwrap_or(50000.0),
            qty: signal.quantity,
            fee: signal.quantity * signal.price.unwrap_or(50000.0) * 0.001,
        }
    }

    /// Create hyperparameter fixture
    pub fn hyperparameters(
        tenant_id: &str,
        workflow_id: &str,
        support: f64,
        resistance: f64,
    ) -> HyperparameterFixture {
        let params = serde_json::json!({
            "support_level": support,
            "resistance_level": resistance,
            "stop_loss": support * 0.95,
            "take_profit": resistance * 1.05,
            "position_size": 0.1,
        });

        HyperparameterFixture {
            tenant_id: tenant_id.to_string(),
            workflow_id: workflow_id.to_string(),
            strategy_id: "default_strategy".to_string(),
            parameters: params,
        }
    }

    /// Create log fixture
    pub fn log(workflow_id: &str, node_id: &str, level: &str, content: &[&str]) -> LogFixture {
        LogFixture {
            workflow_id: workflow_id.to_string(),
            node_id: node_id.to_string(),
            level: level.to_string(),
            content: content.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Serialize trade signal to JSON bytes
    pub fn signal_to_bytes(signal: &TradeSignalFixture) -> Vec<u8> {
        serde_json::to_vec(signal).expect("Failed to serialize trade signal")
    }

    /// Serialize trade event to JSON bytes
    pub fn event_to_bytes(event: &TradeEventFixture) -> Vec<u8> {
        serde_json::to_vec(event).expect("Failed to serialize trade event")
    }

    /// Serialize hyperparameter to JSON bytes
    pub fn hyperparameter_to_bytes(hp: &HyperparameterFixture) -> Vec<u8> {
        serde_json::to_vec(hp).expect("Failed to serialize hyperparameter")
    }

    /// Serialize log to JSON bytes
    pub fn log_to_bytes(log: &LogFixture) -> Vec<u8> {
        serde_json::to_vec(log).expect("Failed to serialize log")
    }
}

/// Default test values
pub mod defaults {
    pub const TENANT_ID: &str = "test_tenant";
    pub const DEFAULT_WALLET_ID: i64 = 1001;
    pub const DEFAULT_SYMBOL: &str = "BTCUSDT";
    pub const DEFAULT_BASE_PRICE: f64 = 50000.0;
    pub const DEFAULT_QUANTITY: f64 = 0.1;
    pub const DEFAULT_FEE_RATE: f64 = 0.001;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_unique_ids() {
        let id1 = Fixtures::generate_id();
        let id2 = Fixtures::generate_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_market_data_fixture() {
        let data = Fixtures::market_data("BTCUSDT", 50000.0);
        assert_eq!(data.symbol, "BTCUSDT");
        assert!(data.open > 0.0);
        assert!(data.high >= data.low);
    }

    #[test]
    fn test_market_data_series() {
        let series = Fixtures::market_data_series("BTCUSDT", 50000.0, 10);
        assert_eq!(series.len(), 10);
        assert!(series.iter().all(|d| d.symbol == "BTCUSDT"));
    }

    #[test]
    fn test_trade_signal_fixture() {
        let signal = Fixtures::trade_signal(
            "tenant1",
            "wf1",
            1001,
            "BTCUSDT",
            "BUY",
            0.1,
            Some(50000.0),
        );

        assert_eq!(signal.tenant_id, "tenant1");
        assert_eq!(signal.symbol, "BTCUSDT");
        assert_eq!(signal.side, "BUY");
        assert_eq!(signal.quantity, 0.1);
        assert!(signal.signal_id.starts_with("sig_"));
    }

    #[test]
    fn test_trade_event_fixture() {
        let signal = Fixtures::trade_signal(
            "tenant1",
            "wf1",
            1001,
            "BTCUSDT",
            "BUY",
            0.1,
            Some(50000.0),
        );
        let event = Fixtures::trade_event(&signal, 1, 1);

        assert_eq!(event.signal_id, signal.signal_id);
        assert_eq!(event.order_id, 1);
        assert_eq!(event.trade_id, 1);
        assert!(event.fee > 0.0);
    }

    #[test]
    fn test_hyperparameter_fixture() {
        let hp = Fixtures::hyperparameters("tenant1", "wf1", 48000.0, 52000.0);

        let params = hp.parameters.as_object().unwrap();
        assert_eq!(params["support_level"], 48000.0);
        assert_eq!(params["resistance_level"], 52000.0);
    }

    #[test]
    fn test_serialization() {
        let signal = Fixtures::trade_signal(
            "tenant1",
            "wf1",
            1001,
            "BTCUSDT",
            "BUY",
            0.1,
            Some(50000.0),
        );

        let bytes = Fixtures::signal_to_bytes(&signal);
        assert!(!bytes.is_empty());

        let deserialized: TradeSignalFixture = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(deserialized.signal_id, signal.signal_id);
    }
}
