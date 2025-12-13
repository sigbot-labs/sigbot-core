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

use crate::server::embed::sdk::series::Series;
use pyo3::prelude::*;

/// Simple Moving Average (SMA) - Streaming mode
/// Calculates SMA incrementally, maintaining state
#[pyfunction]
pub fn sma(series: &Series, period: usize) -> PyResult<Series> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    let mut result = Series::new(None);
    let data_len = series.len();

    if data_len < period {
        // Not enough data, return empty series
        return Ok(result);
    }

    // Calculate SMA for all available data points
    for i in 0..=(data_len - period) {
        let mut sum = 0.0;
        for j in 0..period {
            if let Some(val) = series.get((i + j) as isize) {
                sum += val;
            }
        }
        result.append(sum / period as f64);
    }

    Ok(result)
}

/// Exponential Moving Average (EMA) - Streaming mode
/// Calculates EMA incrementally with state caching
#[pyfunction]
pub fn ema(series: &Series, period: usize) -> PyResult<Series> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    let mut result = Series::new(None);
    let data_len = series.len();

    if data_len == 0 {
        return Ok(result);
    }

    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut ema_value = None;

    // Calculate EMA incrementally
    for i in 0..data_len {
        if let Some(current_value) = series.get((data_len - 1 - i) as isize) {
            ema_value = match ema_value {
                None => Some(current_value), // First value is the price itself
                Some(prev_ema) => Some((current_value - prev_ema) * multiplier + prev_ema),
            };
            if let Some(val) = ema_value {
                result.append(val);
            }
        }
    }

    // Reverse to maintain chronological order
    let mut reversed = Series::new(None);
    for i in 0..result.len() {
        if let Some(val) = result.get(i as isize) {
            reversed.append(val);
        }
    }

    Ok(reversed)
}

/// Simple Moving Average (SMA) - Batch mode
/// Calculates SMA for entire dataset in parallel
#[pyfunction]
pub fn sma_batch(prices: Vec<f64>, period: usize) -> PyResult<Vec<f64>> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    if prices.len() < period {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(prices.len() - period + 1);

    for i in 0..=(prices.len() - period) {
        let sum: f64 = prices[i..i + period].iter().sum();
        result.push(sum / period as f64);
    }

    Ok(result)
}

/// Exponential Moving Average (EMA) - Batch mode
/// Calculates EMA for entire dataset
#[pyfunction]
pub fn ema_batch(prices: Vec<f64>, period: usize) -> PyResult<Vec<f64>> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    if prices.is_empty() {
        return Ok(Vec::new());
    }

    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut result = Vec::with_capacity(prices.len());

    // First value is the price itself
    result.push(prices[0]);

    // Calculate EMA for remaining values
    for i in 1..prices.len() {
        let prev_ema = result[i - 1];
        let ema_value = (prices[i] - prev_ema) * multiplier + prev_ema;
        result.push(ema_value);
    }

    Ok(result)
}

/// MACD (Moving Average Convergence Divergence) - Streaming mode
/// Returns (macd_line, signal_line, histogram) as Series
#[pyfunction]
pub fn macd(
    series: &Series,
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> PyResult<(Series, Series, Series)> {
    if fast_period >= slow_period {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Fast period must be less than slow period",
        ));
    }

    // Calculate EMAs
    let fast_ema = ema(series, fast_period)?;
    let slow_ema = ema(series, slow_period)?;

    // Calculate MACD line (fast EMA - slow EMA)
    let data_len = fast_ema.len().min(slow_ema.len());
    let mut macd_line = Series::new(None);

    for i in 0..data_len {
        let fast_val = fast_ema.get(i as isize);
        let slow_val = slow_ema.get(i as isize);
        if let (Some(f), Some(s)) = (fast_val, slow_val) {
            macd_line.append(f - s);
        }
    }

    // Calculate signal line (EMA of MACD line)
    let signal_line = ema(&macd_line, signal_period)?;

    // Calculate histogram (MACD - Signal)
    let hist_len = macd_line.len().min(signal_line.len());
    let mut histogram = Series::new(None);

    for i in 0..hist_len {
        let macd_val = macd_line.get(i as isize);
        let signal_val = signal_line.get(i as isize);
        if let (Some(m), Some(s)) = (macd_val, signal_val) {
            histogram.append(m - s);
        }
    }

    Ok((macd_line, signal_line, histogram))
}

/// MACD (Moving Average Convergence Divergence) - Batch mode
/// Returns (macd_line, signal_line, histogram) as Vec<f64>
#[pyfunction]
pub fn macd_batch(
    prices: Vec<f64>,
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> PyResult<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    if fast_period >= slow_period {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Fast period must be less than slow period",
        ));
    }

    // Calculate EMAs
    let fast_ema = ema_batch(prices.clone(), fast_period)?;
    let slow_ema = ema_batch(prices, slow_period)?;

    // Calculate MACD line
    let min_len = fast_ema.len().min(slow_ema.len());
    let mut macd_line = Vec::with_capacity(min_len);

    for i in 0..min_len {
        macd_line.push(fast_ema[i] - slow_ema[i]);
    }

    // Calculate signal line
    let signal_line = ema_batch(macd_line.clone(), signal_period)?;

    // Calculate histogram
    let hist_len = macd_line.len().min(signal_line.len());
    let mut histogram = Vec::with_capacity(hist_len);

    for i in 0..hist_len {
        histogram.push(macd_line[i] - signal_line[i]);
    }

    Ok((macd_line, signal_line, histogram))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sma_batch() {
        let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let result = sma_batch(prices, 3).unwrap();
        assert_eq!(result.len(), 8);
        assert!((result[0] - 2.0).abs() < 1e-10); // (1+2+3)/3
        assert!((result[1] - 3.0).abs() < 1e-10); // (2+3+4)/3
        assert!((result[7] - 9.0).abs() < 1e-10); // (8+9+10)/3
    }

    #[test]
    fn test_ema_batch() {
        let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = ema_batch(prices, 3).unwrap();
        assert_eq!(result.len(), 5);
        assert!((result[0] - 1.0).abs() < 1e-10); // First value is price itself
    }
}
