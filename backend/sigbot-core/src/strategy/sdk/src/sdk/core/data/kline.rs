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
// You should have received a the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use sigbot_types::modules::exchange::models::trade_market::KlineModel;

/// Parse K-line data from JSON string
#[pyfunction]
pub fn parse_klines(json_data: &str) -> PyResult<Vec<PyObject>> {
    Python::with_gil(|py| {
        let klines: Vec<KlineModel> = serde_json::from_str(json_data)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to parse JSON: {}", e)))?;

        let mut result = Vec::new();
        for kline in klines {
            let dict = PyDict::new_bound(py);
            dict.set_item("open_time", kline.open_time)?;
            dict.set_item("open_price", kline.open_price)?;
            dict.set_item("high_price", kline.high_price)?;
            dict.set_item("low_price", kline.low_price)?;
            dict.set_item("close_price", kline.close_price)?;
            dict.set_item("volume", kline.volume)?;
            dict.set_item("close_time", kline.close_time)?;
            result.push(dict.into());
        }
        Ok(result)
    })
}

/// Validate K-line data
#[pyfunction]
pub fn validate_kline(
    open_time: u64,
    open_price: f64,
    high_price: f64,
    low_price: f64,
    close_price: f64,
    volume: f64,
) -> PyResult<bool> {
    // Basic validation
    if high_price < low_price {
        return Ok(false);
    }
    if high_price < open_price.max(close_price) {
        return Ok(false);
    }
    if low_price > open_price.min(close_price) {
        return Ok(false);
    }
    if volume < 0.0 {
        return Ok(false);
    }
    Ok(true)
}

/// Filter K-lines by symbol and timeframe
#[pyfunction]
pub fn filter_klines(
    klines: Vec<PyObject>,
    _symbol: Option<&str>,
    _timeframe: Option<&str>,
) -> PyResult<Vec<PyObject>> {
    // For now, just return all klines
    // In the future, we can add filtering logic based on metadata
    Ok(klines)
}
