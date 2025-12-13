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

use crate::sdk::core::series::Series;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Detect crossover between two series
/// Returns: 1 for golden cross (fast crosses above slow), -1 for death cross (fast crosses below slow), 0 for no cross
#[pyfunction]
pub fn crossover(fast: &Series, slow: &Series) -> PyResult<i32> {
    let fast_len = fast.len();
    let slow_len = slow.len();

    if fast_len < 2 || slow_len < 2 {
        return Ok(0);
    }

    let fast_curr = fast.get(0);
    let fast_prev = fast.get(1);
    let slow_curr = slow.get(0);
    let slow_prev = slow.get(1);

    match (fast_curr, fast_prev, slow_curr, slow_prev) {
        (Some(fc), Some(fp), Some(sc), Some(sp))
            if fc.is_finite() && fp.is_finite() && sc.is_finite() && sp.is_finite() =>
        {
            // Golden cross: fast was below slow, now above
            if fp <= sp && fc > sc {
                Ok(1)
            }
            // Death cross: fast was above slow, now below
            else if fp >= sp && fc < sc {
                Ok(-1)
            } else {
                Ok(0)
            }
        }
        _ => Ok(0),
    }
}

/// Detect breakout signal
/// Returns: 1 for upward breakout, -1 for downward breakout, 0 for no breakout
#[pyfunction]
pub fn breakout(price: f64, level: f64, direction: &str) -> PyResult<i32> {
    match direction {
        "up" | "above" => {
            if price > level {
                Ok(1)
            } else {
                Ok(0)
            }
        }
        "down" | "below" => {
            if price < level {
                Ok(-1)
            } else {
                Ok(0)
            }
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Direction must be 'up'/'above' or 'down'/'below'",
        )),
    }
}

/// Pattern recognition (basic implementation)
/// Returns: pattern name if detected, None otherwise
#[pyfunction]
pub fn pattern_recognition(klines: Vec<PyObject>, pattern: &str) -> PyResult<Option<String>> {
    // Basic pattern recognition
    // In the future, implement more sophisticated patterns
    match pattern {
        "doji" => {
            // Check if current kline is a doji (open ≈ close)
            if klines.is_empty() {
                return Ok(None);
            }
            Python::with_gil(|py| {
                let kline = klines[klines.len() - 1].bind(py);
                let kline_dict: &pyo3::Bound<'_, PyDict> = kline
                    .downcast()
                    .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Kline must be a dict"))?;

                let open: f64 = kline_dict
                    .get_item("open_price")?
                    .and_then(|v| v.extract::<f64>().ok())
                    .unwrap_or(0.0);
                let close: f64 = kline_dict
                    .get_item("close_price")?
                    .and_then(|v| v.extract::<f64>().ok())
                    .unwrap_or(0.0);
                let high: f64 = kline_dict
                    .get_item("high_price")?
                    .and_then(|v| v.extract::<f64>().ok())
                    .unwrap_or(0.0);
                let low: f64 = kline_dict
                    .get_item("low_price")?
                    .and_then(|v| v.extract::<f64>().ok())
                    .unwrap_or(0.0);

                let body_size = (open - close).abs();
                let total_range = high - low;

                if total_range > 0.0 && body_size / total_range < 0.1 {
                    Ok(Some("doji".to_string()))
                } else {
                    Ok(None)
                }
            })
        }
        _ => Ok(None),
    }
}
