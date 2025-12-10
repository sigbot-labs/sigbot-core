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

/// Relative Strength Index (RSI) - Streaming mode
/// Calculates RSI incrementally with state caching
#[pyfunction]
pub fn rsi(series: &Series, period: usize) -> PyResult<Series> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    let mut result = Series::new(None);
    let data_len = series.len();

    if data_len < period + 1 {
        // Not enough data
        return Ok(result);
    }

    // Calculate price changes
    let mut gains = Vec::new();
    let mut losses = Vec::new();

    for i in 1..data_len {
        if let (Some(current), Some(previous)) = (series.get(i as isize), series.get((i - 1) as isize)) {
            let change = current - previous;
            gains.push(if change > 0.0 { change } else { 0.0 });
            losses.push(if change < 0.0 { -change } else { 0.0 });
        }
    }

    if gains.len() < period {
        return Ok(result);
    }

    // Calculate initial average gain and loss
    let mut avg_gain: f64 = gains[0..period].iter().sum::<f64>() / period as f64;
    let mut avg_loss: f64 = losses[0..period].iter().sum::<f64>() / period as f64;

    // Calculate first RSI
    if avg_loss == 0.0 {
        result.append(100.0);
    } else {
        let rs = avg_gain / avg_loss;
        let rsi_value = 100.0 - (100.0 / (1.0 + rs));
        result.append(rsi_value);
    }

    // Calculate subsequent RSI values using Wilder's smoothing
    for i in period..gains.len() {
        avg_gain = (avg_gain * (period - 1) as f64 + gains[i]) / period as f64;
        avg_loss = (avg_loss * (period - 1) as f64 + losses[i]) / period as f64;

        if avg_loss == 0.0 {
            result.append(100.0);
        } else {
            let rs = avg_gain / avg_loss;
            let rsi_value = 100.0 - (100.0 / (1.0 + rs));
            result.append(rsi_value);
        }
    }

    Ok(result)
}

/// Relative Strength Index (RSI) - Batch mode
/// Calculates RSI for entire dataset
#[pyfunction]
pub fn rsi_batch(prices: Vec<f64>, period: usize) -> PyResult<Vec<f64>> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    if prices.len() < period + 1 {
        return Ok(Vec::new());
    }

    // Calculate price changes
    let mut gains = Vec::new();
    let mut losses = Vec::new();

    for i in 1..prices.len() {
        let change = prices[i] - prices[i - 1];
        gains.push(if change > 0.0 { change } else { 0.0 });
        losses.push(if change < 0.0 { -change } else { 0.0 });
    }

    if gains.len() < period {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();

    // Calculate initial average gain and loss
    let mut avg_gain: f64 = gains[0..period].iter().sum::<f64>() / period as f64;
    let mut avg_loss: f64 = losses[0..period].iter().sum::<f64>() / period as f64;

    // Calculate first RSI
    if avg_loss == 0.0 {
        result.push(100.0);
    } else {
        let rs = avg_gain / avg_loss;
        let rsi_value = 100.0 - (100.0 / (1.0 + rs));
        result.push(rsi_value);
    }

    // Calculate subsequent RSI values using Wilder's smoothing
    for i in period..gains.len() {
        avg_gain = (avg_gain * (period - 1) as f64 + gains[i]) / period as f64;
        avg_loss = (avg_loss * (period - 1) as f64 + losses[i]) / period as f64;

        if avg_loss == 0.0 {
            result.push(100.0);
        } else {
            let rs = avg_gain / avg_loss;
            let rsi_value = 100.0 - (100.0 / (1.0 + rs));
            result.push(rsi_value);
        }
    }

    Ok(result)
}

/// Stochastic Oscillator - Streaming mode
/// Returns (%K, %D) as Series
#[pyfunction]
pub fn stochastic(
    high: &Series,
    low: &Series,
    close: &Series,
    k_period: usize,
    d_period: usize,
) -> PyResult<(Series, Series)> {
    if k_period == 0 || d_period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Periods must be greater than 0",
        ));
    }

    let data_len = high.len().min(low.len()).min(close.len());
    if data_len < k_period {
        return Ok((Series::new(None), Series::new(None)));
    }

    let mut k_values = Series::new(None);

    // Calculate %K
    for i in 0..=(data_len - k_period) {
        let mut highest = f64::NEG_INFINITY;
        let mut lowest = f64::INFINITY;

        for j in 0..k_period {
            if let Some(h) = high.get((i + j) as isize) {
                highest = highest.max(h);
            }
            if let Some(l) = low.get((i + j) as isize) {
                lowest = lowest.min(l);
            }
        }

        if let Some(curr_close) = close.get(i as isize) {
            if highest != lowest {
                let k = 100.0 * (curr_close - lowest) / (highest - lowest);
                k_values.append(k);
            } else {
                k_values.append(50.0);
            }
        }
    }

    // Calculate %D (SMA of %K)
    let d_values = crate::server::embed::sdk::indicators::trend::sma(&k_values, d_period)?;

    Ok((k_values, d_values))
}

/// Stochastic Oscillator - Batch mode
/// Returns (%K, %D) as Vec<f64>
#[pyfunction]
pub fn stochastic_batch(
    highs: Vec<f64>,
    lows: Vec<f64>,
    closes: Vec<f64>,
    k_period: usize,
    d_period: usize,
) -> PyResult<(Vec<f64>, Vec<f64>)> {
    if k_period == 0 || d_period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Periods must be greater than 0",
        ));
    }

    if highs.len() != lows.len() || highs.len() != closes.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "High, low, and close arrays must have the same length",
        ));
    }

    if highs.len() < k_period {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut k_values = Vec::new();

    // Calculate %K
    for i in 0..=(highs.len() - k_period) {
        let window_highs = &highs[i..i + k_period];
        let window_lows = &lows[i..i + k_period];

        let highest = window_highs.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let lowest = window_lows.iter().fold(f64::INFINITY, |a, &b| a.min(b));

        if highest != lowest {
            let k = 100.0 * (closes[i] - lowest) / (highest - lowest);
            k_values.push(k);
        } else {
            k_values.push(50.0);
        }
    }

    // Calculate %D (SMA of %K)
    let d_values = crate::server::embed::sdk::indicators::trend::sma_batch(k_values.clone(), d_period)?;

    Ok((k_values, d_values))
}

/// Commodity Channel Index (CCI) - Streaming mode
#[pyfunction]
pub fn cci(high: &Series, low: &Series, close: &Series, period: usize) -> PyResult<Series> {
    if period == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Period must be greater than 0",
        ));
    }

    let data_len = high.len().min(low.len()).min(close.len());
    if data_len < period {
        return Ok(Series::new(None));
    }

    let mut result = Series::new(None);

    for i in 0..=(data_len - period) {
        let mut typical_prices = Vec::with_capacity(period);
        let mut sum = 0.0;

        for j in 0..period {
            let h = high.get((i + j) as isize).unwrap_or(0.0);
            let l = low.get((i + j) as isize).unwrap_or(0.0);
            let c = close.get((i + j) as isize).unwrap_or(0.0);
            let tp = (h + l + c) / 3.0;
            typical_prices.push(tp);
            sum += tp;
        }

        let sma_tp = sum / period as f64;
        let mean_deviation: f64 = typical_prices.iter().map(|&tp| (tp - sma_tp).abs()).sum::<f64>() / period as f64;

        if mean_deviation > 0.0 {
            let current_tp = typical_prices[period - 1];
            let cci_value = (current_tp - sma_tp) / (0.015 * mean_deviation);
            result.append(cci_value);
        } else {
            result.append(0.0);
        }
    }

    Ok(result)
}

/// Commodity Channel Index (CCI) - Batch mode
#[pyfunction]
pub fn cci_batch(highs: Vec<f64>, lows: Vec<f64>, closes: Vec<f64>, period: usize) -> PyResult<Vec<f64>> {
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

    if highs.len() < period {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();

    for i in 0..=(highs.len() - period) {
        let typical_prices: Vec<f64> = (i..i + period)
            .map(|j| (highs[j] + lows[j] + closes[j]) / 3.0)
            .collect();

        let sma_tp: f64 = typical_prices.iter().sum::<f64>() / period as f64;
        let mean_deviation: f64 = typical_prices.iter().map(|&tp| (tp - sma_tp).abs()).sum::<f64>() / period as f64;

        if mean_deviation > 0.0 {
            let current_tp = typical_prices[period - 1];
            let cci_value = (current_tp - sma_tp) / (0.015 * mean_deviation);
            result.push(cci_value);
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
    fn test_rsi_batch() {
        // Test with known RSI values
        let prices = vec![44.0, 44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.85, 46.08, 45.89];
        let result = rsi_batch(prices, 14).unwrap();
        // RSI should be between 0 and 100
        assert!(!result.is_empty());
        assert!(result.iter().all(|&x| x >= 0.0 && x <= 100.0));
    }
}
