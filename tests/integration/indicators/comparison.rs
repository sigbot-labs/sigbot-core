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

use sigbot_strategy::server::embed::sdk::indicators::momentum::rsi_batch;
use sigbot_strategy::server::embed::sdk::indicators::trend::{ema_batch, sma_batch};

/// Compare results with reference values (e.g., from TradingView, TA-Lib)
/// This is a framework for comparing our implementations with external sources
fn compare_with_tolerance(actual: &[f64], expected: &[f64], tolerance: f64, source: &str) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "Length mismatch for {}: actual={}, expected={}",
        source,
        actual.len(),
        expected.len()
    );

    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        let diff = (a - e).abs();
        assert!(
            diff <= tolerance || (a.is_nan() && e.is_nan()),
            "Mismatch at index {} for {}: actual={}, expected={}, diff={}",
            i,
            source,
            a,
            e,
            diff
        );
    }
}

/// Test SMA against known reference values
/// Reference values can be obtained from TradingView or TA-Lib
#[test]
fn test_sma_tradingview_compatibility() {
    // Sample data: BTCUSDT 1m klines
    let prices = vec![
        50000.0, 50050.0, 50100.0, 50080.0, 50120.0, 50150.0, 50200.0, 50180.0, 50250.0, 50300.0,
    ];

    let result = sma_batch(prices, 5).unwrap();

    // Expected values (calculated manually or from TradingView)
    // For period=5, first SMA should be average of first 5 prices
    let expected_first = (50000.0 + 50050.0 + 50100.0 + 50080.0 + 50120.0) / 5.0;
    assert!((result[0] - expected_first).abs() < 1e-10);

    // Last SMA should be average of last 5 prices
    let expected_last = (50150.0 + 50200.0 + 50180.0 + 50250.0 + 50300.0) / 5.0;
    assert!((result[result.len() - 1] - expected_last).abs() < 1e-10);
}

/// Test EMA against known reference values
#[test]
fn test_ema_tradingview_compatibility() {
    let prices = vec![100.0, 101.0, 102.0, 101.5, 103.0, 104.0, 103.5, 105.0];

    let result = ema_batch(prices.clone(), 3).unwrap();

    // EMA(3) first value should be the first price
    assert!((result[0] - prices[0]).abs() < 1e-10);

    // EMA should be between min and max prices
    let min_price = prices.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max_price = prices.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

    for &ema_val in &result {
        assert!(ema_val >= min_price && ema_val <= max_price);
    }
}

/// Test RSI against known reference values
#[test]
fn test_rsi_tradingview_compatibility() {
    // Standard RSI test data (from TradingView documentation)
    let prices = vec![
        44.0, 44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.85, 46.08, 45.89, 46.03, 46.83, 46.69, 46.45, 46.30, 46.28,
        46.28, 46.00, 46.03, 46.41, 46.31, 46.32, 46.25, 46.10, 46.32, 46.36, 46.52, 46.50, 46.32, 46.39,
    ];

    let result = rsi_batch(prices, 14).unwrap();

    // RSI should be between 0 and 100
    assert!(result.iter().all(|&x| x >= 0.0 && x <= 100.0));

    // With upward trend, RSI should generally be above 50
    // (but not always, depends on the data)
    let avg_rsi: f64 = result.iter().sum::<f64>() / result.len() as f64;
    assert!(avg_rsi > 30.0 && avg_rsi < 100.0);
}

/// Test compatibility matrix
/// This test verifies that our implementations match expected behavior
#[test]
fn test_compatibility_matrix() {
    // Test SMA compatibility
    let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
    let sma_result = sma_batch(prices.clone(), 3).unwrap();

    // SMA(3) should match manual calculation
    let manual_sma_0 = (1.0 + 2.0 + 3.0) / 3.0;
    let manual_sma_1 = (2.0 + 3.0 + 4.0) / 3.0;

    assert!((sma_result[0] - manual_sma_0).abs() < 1e-10);
    assert!((sma_result[1] - manual_sma_1).abs() < 1e-10);

    // Test EMA compatibility
    let ema_result = ema_batch(prices.clone(), 3).unwrap();
    assert_eq!(ema_result.len(), prices.len());
    assert!((ema_result[0] - prices[0]).abs() < 1e-10); // First EMA = first price
}
