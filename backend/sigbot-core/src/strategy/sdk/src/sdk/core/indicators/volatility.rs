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

/// Bollinger Bands - Streaming mode
/// Returns (upper_band, middle_band, lower_band) as Series
#[pyfunction]
pub fn bollinger_bands(
    series: &TimeSeries,
    period: usize,
    std_dev: f64,
) -> PyResult<(TimeSeries, TimeSeries, TimeSeries)> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    let data_len = series.len();
    if data_len < period {
        return Ok((TimeSeries::new(None), TimeSeries::new(None), TimeSeries::new(None)));
    }

    let mut upper = TimeSeries::new(None);
    let mut middle = TimeSeries::new(None);
    let mut lower = TimeSeries::new(None);

    // Calculate SMA (middle band) and standard deviation
    for i in 0..=(data_len - period) {
        let mut sum = 0.0;
        let mut values = Vec::with_capacity(period);

        for j in 0..period {
            if let Some(val) = series.get((i + j) as isize) {
                sum += val;
                values.push(val);
            }
        }

        let sma = sum / period as f64;

        // Calculate standard deviation
        let variance: f64 = values.iter().map(|&x| (x - sma).powi(2)).sum::<f64>() / period as f64;
        let std = variance.sqrt();

        middle.append(sma);
        upper.append(sma + std_dev * std);
        lower.append(sma - std_dev * std);
    }

    Ok((upper, middle, lower))
}

/// Bollinger Bands - Batch mode
/// Returns (upper_band, middle_band, lower_band) as Vec<f64>
#[pyfunction]
pub fn bollinger_bands_batch(
    prices: Vec<f64>,
    period: usize,
    std_dev: f64,
) -> PyResult<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    if prices.len() < period {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    }

    let mut upper = Vec::new();
    let mut middle = Vec::new();
    let mut lower = Vec::new();

    for i in 0..=(prices.len() - period) {
        let window = &prices[i..i + period];
        let sma: f64 = window.iter().sum::<f64>() / period as f64;

        let variance: f64 = window.iter().map(|&x| (x - sma).powi(2)).sum::<f64>() / period as f64;
        let std = variance.sqrt();

        middle.push(sma);
        upper.push(sma + std_dev * std);
        lower.push(sma - std_dev * std);
    }

    Ok((upper, middle, lower))
}

/// Average True Range (ATR) - Streaming mode
#[pyfunction]
pub fn atr(high: &TimeSeries, low: &TimeSeries, close: &TimeSeries, period: usize) -> PyResult<TimeSeries> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    let data_len = high.len().min(low.len()).min(close.len());
    if data_len < period + 1 {
        return Ok(TimeSeries::new(None));
    }

    let mut result = TimeSeries::new(None);
    let mut tr_values = Vec::new();

    // Calculate True Range
    for i in 1..data_len {
        let prev_close = close.get((i - 1) as isize).unwrap_or(0.0);
        let curr_high = high.get(i as isize).unwrap_or(0.0);
        let curr_low = low.get(i as isize).unwrap_or(0.0);

        let tr = (curr_high - curr_low)
            .max((curr_high - prev_close).abs())
            .max((curr_low - prev_close).abs());
        tr_values.push(tr);
    }

    if tr_values.len() < period {
        return Ok(result);
    }

    // Calculate initial ATR (SMA of TR)
    let mut atr_value: f64 = tr_values[0..period].iter().sum::<f64>() / period as f64;
    result.append(atr_value);

    // Calculate subsequent ATR using Wilder's smoothing
    for i in period..tr_values.len() {
        atr_value = (atr_value * (period - 1) as f64 + tr_values[i]) / period as f64;
        result.append(atr_value);
    }

    Ok(result)
}

/// Average True Range (ATR) - Batch mode
#[pyfunction]
pub fn atr_batch(highs: Vec<f64>, lows: Vec<f64>, closes: Vec<f64>, period: usize) -> PyResult<Vec<f64>> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    if highs.len() != lows.len() || highs.len() != closes.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "High, low, and close arrays must have the same length",
        ));
    }

    if highs.len() < period + 1 {
        return Ok(Vec::new());
    }

    let mut tr_values = Vec::new();

    // Calculate True Range
    for i in 1..highs.len() {
        let tr = (highs[i] - lows[i])
            .max((highs[i] - closes[i - 1]).abs())
            .max((lows[i] - closes[i - 1]).abs());
        tr_values.push(tr);
    }

    if tr_values.len() < period {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();

    // Calculate initial ATR
    let mut atr_value: f64 = tr_values[0..period].iter().sum::<f64>() / period as f64;
    result.push(atr_value);

    // Calculate subsequent ATR using Wilder's smoothing
    for i in period..tr_values.len() {
        atr_value = (atr_value * (period - 1) as f64 + tr_values[i]) / period as f64;
        result.push(atr_value);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bollinger_bands_batch() {
        let prices = vec![100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0];
        let (upper, middle, lower) = bollinger_bands_batch(prices, 3, 2.0).unwrap();

        assert_eq!(upper.len(), 5);
        assert_eq!(middle.len(), 5);
        assert_eq!(lower.len(), 5);

        // Middle band should be SMA
        assert!((middle[0] - (100.0 + 102.0 + 101.0) / 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_atr_batch() {
        let highs = vec![105.0, 107.0, 106.0, 108.0, 110.0];
        let lows = vec![100.0, 102.0, 101.0, 103.0, 105.0];
        let closes = vec![103.0, 105.0, 104.0, 107.0, 109.0];

        let result = atr_batch(highs, lows, closes, 3).unwrap();
        assert!(!result.is_empty());
        for &x in &result {
            assert!(x > 0.0);
        }
    }
}
