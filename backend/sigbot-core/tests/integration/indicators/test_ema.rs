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

use sigbot_strategy::server::embed::sdk::indicators::trend::{ema, ema_batch};
use sigbot_strategy::server::embed::sdk::series::Series;

/// Test EMA batch mode with known values
#[test]
fn test_ema_batch_basic() {
    let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let result = ema_batch(prices, 3).unwrap();

    assert_eq!(result.len(), 5);
    assert!((result[0] - 1.0).abs() < 1e-10); // First value is price itself
    assert!(result[1] > result[0]); // EMA should increase with upward trend
}

/// Test EMA streaming mode
#[test]
fn test_ema_streaming() {
    let mut series = Series::new(None);
    series.append(1.0);
    series.append(2.0);
    series.append(3.0);
    series.append(4.0);
    series.append(5.0);

    let result = ema(&series, 3).unwrap();
    assert!(!result.is_empty());
}

/// Test EMA with insufficient data
#[test]
fn test_ema_insufficient_data() {
    let prices = vec![1.0];
    let result = ema_batch(prices, 5).unwrap();
    assert_eq!(result.len(), 1); // Should return at least first value
}

/// Test EMA edge cases
#[test]
fn test_ema_edge_cases() {
    // Single value
    let prices = vec![100.0];
    let result = ema_batch(prices, 1).unwrap();
    assert_eq!(result.len(), 1);
    assert!((result[0] - 100.0).abs() < 1e-10);

    // Constant prices
    let prices = vec![100.0, 100.0, 100.0];
    let result = ema_batch(prices, 2).unwrap();
    assert_eq!(result.len(), 3);
    assert!((result[0] - 100.0).abs() < 1e-10);
}
