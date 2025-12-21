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

use crate::sdk::core::models::time_series::TimeSeries;
use pyo3::prelude::*;

/// On-Balance Volume (OBV) - Streaming mode
#[pyfunction]
pub fn obv(close: &TimeSeries, volume: &TimeSeries) -> PyResult<TimeSeries> {
    let data_len = close.len().min(volume.len());
    if data_len < 2 {
        return Ok(TimeSeries::new(None));
    }

    let mut result = TimeSeries::new(None);
    let mut obv_value = 0.0;

    // First OBV value is the first volume
    if let Some(vol) = volume.get((data_len - 1) as isize) {
        obv_value = vol;
        result.append(obv_value);
    }

    // Calculate OBV incrementally
    for i in (0..data_len - 1).rev() {
        let curr_close = close.get(i as isize);
        let prev_close = close.get((i + 1) as isize);
        let curr_volume = volume.get(i as isize).unwrap_or(0.0);

        match (curr_close, prev_close) {
            (Some(curr), Some(prev)) => {
                if curr > prev {
                    obv_value += curr_volume;
                } else if curr < prev {
                    obv_value -= curr_volume;
                }
                // If equal, OBV stays the same
            }
            _ => {}
        }
        result.append(obv_value);
    }

    // Reverse to maintain chronological order
    let mut reversed = TimeSeries::new(None);
    for i in 0..result.len() {
        if let Some(val) = result.get(i as isize) {
            reversed.append(val);
        }
    }

    Ok(reversed)
}

/// On-Balance Volume (OBV) - Batch mode
#[pyfunction]
pub fn obv_batch(closes: Vec<f64>, volumes: Vec<f64>) -> PyResult<Vec<f64>> {
    if closes.len() != volumes.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Closes and volumes must have the same length",
        ));
    }

    if closes.is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(closes.len());
    let mut obv_value = volumes[0];
    result.push(obv_value);

    for i in 1..closes.len() {
        if closes[i] > closes[i - 1] {
            obv_value += volumes[i];
        } else if closes[i] < closes[i - 1] {
            obv_value -= volumes[i];
        }
        // If equal, OBV stays the same
        result.push(obv_value);
    }

    Ok(result)
}

/// Volume Weighted Average Price (VWAP) - Streaming mode
#[pyfunction]
pub fn vwap(high: &TimeSeries, low: &TimeSeries, close: &TimeSeries, volume: &TimeSeries) -> PyResult<TimeSeries> {
    let data_len = high.len().min(low.len()).min(close.len()).min(volume.len());

    if data_len == 0 {
        return Ok(TimeSeries::new(None));
    }

    let mut result = TimeSeries::new(None);
    let mut cumulative_tpv = 0.0; // Typical Price * Volume
    let mut cumulative_volume = 0.0;

    for i in 0..data_len {
        let h = high.get(i as isize).unwrap_or(0.0);
        let l = low.get(i as isize).unwrap_or(0.0);
        let c = close.get(i as isize).unwrap_or(0.0);
        let v = volume.get(i as isize).unwrap_or(0.0);

        let typical_price = (h + l + c) / 3.0;
        cumulative_tpv += typical_price * v;
        cumulative_volume += v;

        if cumulative_volume > 0.0 {
            result.append(cumulative_tpv / cumulative_volume);
        } else {
            result.append(0.0);
        }
    }

    Ok(result)
}

/// Volume Weighted Average Price (VWAP) - Batch mode
#[pyfunction]
pub fn vwap_batch(highs: Vec<f64>, lows: Vec<f64>, closes: Vec<f64>, volumes: Vec<f64>) -> PyResult<Vec<f64>> {
    if highs.len() != lows.len() || highs.len() != closes.len() || highs.len() != volumes.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "All arrays must have the same length",
        ));
    }

    if highs.is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(highs.len());
    let mut cumulative_tpv = 0.0;
    let mut cumulative_volume = 0.0;

    for i in 0..highs.len() {
        let typical_price = (highs[i] + lows[i] + closes[i]) / 3.0;
        cumulative_tpv += typical_price * volumes[i];
        cumulative_volume += volumes[i];

        if cumulative_volume > 0.0 {
            result.push(cumulative_tpv / cumulative_volume);
        } else {
            result.push(0.0);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_obv_batch() {
        let closes = vec![100.0, 101.0, 100.5, 102.0];
        let volumes = vec![1000.0, 1200.0, 800.0, 1500.0];
        let first_volume = volumes[0];

        let result = obv_batch(closes, volumes).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], first_volume); // First OBV = first volume
    }

    #[test]
    fn test_vwap_batch() {
        let highs = vec![105.0, 107.0, 106.0];
        let lows = vec![100.0, 102.0, 101.0];
        let closes = vec![103.0, 105.0, 104.0];
        let volumes = vec![1000.0, 1200.0, 800.0];

        let result = vwap_batch(highs, lows, closes, volumes).unwrap();
        assert_eq!(result.len(), 3);
        for &x in &result {
            assert!(x > 0.0);
        }
    }
}
