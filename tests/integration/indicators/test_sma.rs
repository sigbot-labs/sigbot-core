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

use sigbot_strategy::server::embed::sdk::indicators::trend::{sma_batch, sma};
use sigbot_strategy::server::embed::sdk::series::Series;

/// Test SMA batch mode with known values
#[test]
fn test_sma_batch_basic() {
    let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
    let result = sma_batch(prices, 3).unwrap();
    
    assert_eq!(result.len(), 8);
    assert!((result[0] - 2.0).abs() < 1e-10); // (1+2+3)/3 = 2.0
    assert!((result[1] - 3.0).abs() < 1e-10); // (2+3+4)/3 = 3.0
    assert!((result[7] - 9.0).abs() < 1e-10); // (8+9+10)/3 = 9.0
}

/// Test SMA streaming mode
#[test]
fn test_sma_streaming() {
    let mut series = Series::new(None);
    series.append(1.0);
    series.append(2.0);
    series.append(3.0);
    series.append(4.0);
    series.append(5.0);
    
    let result = sma(&series, 3).unwrap();
    assert_eq!(result.len(), 3);
    assert!((result.get(0).unwrap() - 4.0).abs() < 1e-10); // (3+4+5)/3 = 4.0
}

/// Test SMA with insufficient data
#[test]
fn test_sma_insufficient_data() {
    let prices = vec![1.0, 2.0];
    let result = sma_batch(prices, 5).unwrap();
    assert!(result.is_empty());
}

/// Test SMA edge cases
#[test]
fn test_sma_edge_cases() {
    // Single value
    let prices = vec![100.0];
    let result = sma_batch(prices, 1).unwrap();
    assert_eq!(result.len(), 1);
    assert!((result[0] - 100.0).abs() < 1e-10);
    
    // Period equals data length
    let prices = vec![1.0, 2.0, 3.0];
    let result = sma_batch(prices, 3).unwrap();
    assert_eq!(result.len(), 1);
    assert!((result[0] - 2.0).abs() < 1e-10); // (1+2+3)/3 = 2.0
}
