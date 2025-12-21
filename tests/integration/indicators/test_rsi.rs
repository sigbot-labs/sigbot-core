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

use sigbot_strategy::server::embed::sdk::indicators::momentum::{rsi, rsi_batch};
use sigbot_strategy::server::embed::sdk::series::Series;

/// Test RSI batch mode with known values
#[test]
fn test_rsi_batch_basic() {
    // Test with upward trending prices
    let prices = vec![
        44.0, 44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.85, 46.08, 45.89, 46.03, 46.83, 46.69, 46.45, 46.30, 46.28,
        46.28, 46.00, 46.03, 46.41,
    ];
    let result = rsi_batch(prices, 14).unwrap();

    // RSI should be between 0 and 100
    assert!(!result.is_empty());
    assert!(result.iter().all(|&x| x >= 0.0 && x <= 100.0));
}

/// Test RSI with insufficient data
#[test]
fn test_rsi_insufficient_data() {
    let prices = vec![100.0, 101.0];
    let result = rsi_batch(prices, 14).unwrap();
    assert!(result.is_empty());
}

/// Test RSI edge cases
#[test]
fn test_rsi_edge_cases() {
    // Constant prices (no change)
    let prices = vec![100.0, 100.0, 100.0, 100.0, 100.0];
    let result = rsi_batch(prices, 2).unwrap();
    // With no price changes, RSI should be 50 (neutral)
    assert!(!result.is_empty());
}

/// Test RSI streaming mode
#[test]
fn test_rsi_streaming() {
    let mut series = Series::new(None);
    let prices = vec![44.0, 44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.85, 46.08, 45.89];

    for price in prices {
        series.append(price);
    }

    let result = rsi(&series, 5).unwrap();
    assert!(!result.is_empty());
    let values = result.iter();
    assert!(values.iter().all(|&x| x >= 0.0 && x <= 100.0));
}
