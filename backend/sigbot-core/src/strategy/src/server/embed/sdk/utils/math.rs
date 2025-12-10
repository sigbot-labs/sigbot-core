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

/// Normalize values to [0, 1] range
#[pyfunction]
pub fn normalize(values: Vec<f64>) -> PyResult<Vec<f64>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

    if max == min {
        return Ok(vec![0.5; values.len()]);
    }

    Ok(values.iter().map(|&v| (v - min) / (max - min)).collect())
}

/// Standardize values (z-score normalization)
#[pyfunction]
pub fn standardize(values: Vec<f64>) -> PyResult<Vec<f64>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
    let variance: f64 = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
    let std_dev = variance.sqrt();

    if std_dev == 0.0 {
        return Ok(vec![0.0; values.len()]);
    }

    Ok(values.iter().map(|&v| (v - mean) / std_dev).collect())
}

/// Calculate correlation coefficient between two series
#[pyfunction]
pub fn correlation(x: Vec<f64>, y: Vec<f64>) -> PyResult<f64> {
    if x.len() != y.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "X and Y must have the same length",
        ));
    }

    if x.is_empty() {
        return Ok(0.0);
    }

    let x_mean: f64 = x.iter().sum::<f64>() / x.len() as f64;
    let y_mean: f64 = y.iter().sum::<f64>() / y.len() as f64;

    let mut numerator = 0.0;
    let mut x_variance = 0.0;
    let mut y_variance = 0.0;

    for i in 0..x.len() {
        let x_diff = x[i] - x_mean;
        let y_diff = y[i] - y_mean;
        numerator += x_diff * y_diff;
        x_variance += x_diff * x_diff;
        y_variance += y_diff * y_diff;
    }

    let denominator = (x_variance * y_variance).sqrt();
    if denominator == 0.0 {
        return Ok(0.0);
    }

    Ok(numerator / denominator)
}

/// Calculate Sharpe ratio
#[pyfunction]
pub fn sharpe_ratio(returns: Vec<f64>, risk_free_rate: f64) -> PyResult<f64> {
    if returns.is_empty() {
        return Ok(0.0);
    }

    let mean_return: f64 = returns.iter().sum::<f64>() / returns.len() as f64;
    let excess_return = mean_return - risk_free_rate;

    let variance: f64 = returns.iter().map(|&r| (r - mean_return).powi(2)).sum::<f64>() / returns.len() as f64;
    let std_dev = variance.sqrt();

    if std_dev == 0.0 {
        return Ok(0.0);
    }

    Ok(excess_return / std_dev)
}
