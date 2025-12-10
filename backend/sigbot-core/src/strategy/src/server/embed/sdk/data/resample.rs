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

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Resample K-lines to a different timeframe
#[pyfunction]
pub fn resample_klines(klines: Vec<PyObject>, from_interval: &str, to_interval: &str) -> PyResult<Vec<PyObject>> {
    Python::with_gil(|py| {
        // Parse interval strings (e.g., "1m", "5m", "1h", "1d")
        let from_ms = parse_interval(from_interval)?;
        let to_ms = parse_interval(to_interval)?;

        if to_ms < from_ms {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Target interval must be greater than or equal to source interval",
            ));
        }

        if klines.is_empty() {
            return Ok(Vec::new());
        }

        let ratio = to_ms / from_ms;
        let mut result = Vec::new();
        let mut current_bucket: Option<PyObject> = None;
        let mut bucket_start_time = 0u64;

        for kline_obj in klines {
            let kline = kline_obj.bind(py);
            let kline_dict: &pyo3::Bound<'_, PyDict> = kline
                .downcast()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Kline must be a dict"))?;

            let open_time_obj = kline_dict
                .get_item("open_time")?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid kline data"))?;
            let open_time: u64 = open_time_obj
                .extract()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid kline data"))?;

            let bucket_time = (open_time / to_ms) * to_ms;

            if current_bucket.is_none() || bucket_start_time != bucket_time {
                // Start new bucket
                if let Some(bucket) = current_bucket.take() {
                    result.push(bucket);
                }
                current_bucket = Some(kline_obj.clone_ref(py));
                bucket_start_time = bucket_time;
            } else {
                // Merge into current bucket
                if let Some(ref mut bucket) = current_bucket {
                    let bucket_dict: &pyo3::Bound<'_, PyDict> = bucket
                        .bind(py)
                        .downcast()
                        .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Bucket must be a dict"))?;

                    // Helper function to extract f64 from dict item
                    let get_f64 = |dict: &pyo3::Bound<'_, PyDict>, key: &str| -> PyResult<f64> {
                        Ok(dict.get_item(key)?.and_then(|v| v.extract::<f64>().ok()).unwrap_or(0.0))
                    };

                    let get_f64_or_inf = |dict: &pyo3::Bound<'_, PyDict>, key: &str| -> PyResult<f64> {
                        Ok(dict
                            .get_item(key)?
                            .and_then(|v| v.extract::<f64>().ok())
                            .unwrap_or(f64::INFINITY))
                    };

                    let get_u64 = |dict: &pyo3::Bound<'_, PyDict>, key: &str| -> PyResult<u64> {
                        Ok(dict.get_item(key)?.and_then(|v| v.extract::<u64>().ok()).unwrap_or(0))
                    };

                    // Update high
                    let bucket_high = get_f64(bucket_dict, "high_price")?;
                    let kline_high = get_f64(kline_dict, "high_price")?;
                    bucket_dict.set_item("high_price", bucket_high.max(kline_high))?;

                    // Update low
                    let bucket_low = get_f64_or_inf(bucket_dict, "low_price")?;
                    let kline_low = get_f64_or_inf(kline_dict, "low_price")?;
                    bucket_dict.set_item("low_price", bucket_low.min(kline_low))?;

                    // Update close (use latest)
                    let kline_close = get_f64(kline_dict, "close_price")?;
                    bucket_dict.set_item("close_price", kline_close)?;

                    // Update volume
                    let bucket_volume = get_f64(bucket_dict, "volume")?;
                    let kline_volume = get_f64(kline_dict, "volume")?;
                    bucket_dict.set_item("volume", bucket_volume + kline_volume)?;

                    // Update close_time
                    let kline_close_time = get_u64(kline_dict, "close_time")?;
                    bucket_dict.set_item("close_time", kline_close_time)?;
                }
            }
        }

        // Add last bucket
        if let Some(bucket) = current_bucket {
            result.push(bucket);
        }

        Ok(result)
    })
}

/// Resample OHLCV data
#[pyfunction]
pub fn resample_ohlcv(
    opens: Vec<f64>,
    highs: Vec<f64>,
    lows: Vec<f64>,
    closes: Vec<f64>,
    volumes: Vec<f64>,
    timestamps: Vec<u64>,
    interval_ms: u64,
) -> PyResult<(Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<u64>)> {
    if opens.len() != highs.len()
        || opens.len() != lows.len()
        || opens.len() != closes.len()
        || opens.len() != volumes.len()
        || opens.len() != timestamps.len()
    {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "All arrays must have the same length",
        ));
    }

    if opens.is_empty() {
        return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()));
    }

    let mut resampled_opens = Vec::new();
    let mut resampled_highs = Vec::new();
    let mut resampled_lows = Vec::new();
    let mut resampled_closes = Vec::new();
    let mut resampled_volumes = Vec::new();
    let mut resampled_timestamps = Vec::new();

    let mut current_bucket_start = (timestamps[0] / interval_ms) * interval_ms;
    let mut bucket_high = highs[0];
    let mut bucket_low = lows[0];
    let mut bucket_close = closes[0];
    let mut bucket_volume = volumes[0];

    resampled_opens.push(opens[0]);
    resampled_timestamps.push(current_bucket_start);

    for i in 1..opens.len() {
        let bucket_time = (timestamps[i] / interval_ms) * interval_ms;

        if bucket_time != current_bucket_start {
            // Finalize current bucket
            resampled_highs.push(bucket_high);
            resampled_lows.push(bucket_low);
            resampled_closes.push(bucket_close);
            resampled_volumes.push(bucket_volume);

            // Start new bucket
            current_bucket_start = bucket_time;
            resampled_opens.push(opens[i]);
            resampled_timestamps.push(current_bucket_start);
            bucket_high = highs[i];
            bucket_low = lows[i];
            bucket_close = closes[i];
            bucket_volume = volumes[i];
        } else {
            // Merge into current bucket
            bucket_high = bucket_high.max(highs[i]);
            bucket_low = bucket_low.min(lows[i]);
            bucket_close = closes[i];
            bucket_volume += volumes[i];
        }
    }

    // Finalize last bucket
    resampled_highs.push(bucket_high);
    resampled_lows.push(bucket_low);
    resampled_closes.push(bucket_close);
    resampled_volumes.push(bucket_volume);

    Ok((
        resampled_opens,
        resampled_highs,
        resampled_lows,
        resampled_closes,
        resampled_volumes,
        resampled_timestamps,
    ))
}

fn parse_interval(interval: &str) -> PyResult<u64> {
    let interval = interval.trim().to_lowercase();
    let (num_str, unit) = interval.split_at(
        interval
            .char_indices()
            .find(|(_, c)| !c.is_ascii_digit())
            .map(|(i, _)| i)
            .unwrap_or(interval.len()),
    );

    let num: u64 = num_str.parse().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid interval number: {}", num_str))
    })?;

    let multiplier = match unit {
        "s" | "sec" | "second" | "seconds" => 1000,
        "m" | "min" | "minute" | "minutes" => 60 * 1000,
        "h" | "hour" | "hours" => 60 * 60 * 1000,
        "d" | "day" | "days" => 24 * 60 * 60 * 1000,
        "w" | "week" | "weeks" => 7 * 24 * 60 * 60 * 1000,
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid interval unit: {}",
                unit
            )));
        }
    };

    Ok(num * multiplier)
}
