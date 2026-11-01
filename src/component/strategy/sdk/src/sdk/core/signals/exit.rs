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

/// Check if stop loss should be triggered
/// Returns: true if stop loss triggered, false otherwise
#[pyfunction]
pub fn stop_loss(entry_price: f64, current_price: f64, loss_pct: f64, side: &str) -> PyResult<bool> {
    if loss_pct <= 0.0 || loss_pct >= 100.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Loss percentage must be between 0 and 100",
        ));
    }

    match side {
        "long" | "buy" => {
            let loss = (entry_price - current_price) / entry_price * 100.0;
            Ok(loss >= loss_pct)
        }
        "short" | "sell" => {
            let loss = (current_price - entry_price) / entry_price * 100.0;
            Ok(loss >= loss_pct)
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Side must be 'long'/'buy' or 'short'/'sell'",
        )),
    }
}

/// Check if take profit should be triggered
/// Returns: true if take profit triggered, false otherwise
#[pyfunction]
pub fn take_profit(entry_price: f64, current_price: f64, profit_pct: f64, side: &str) -> PyResult<bool> {
    if profit_pct <= 0.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Profit percentage must be greater than 0",
        ));
    }

    match side {
        "long" | "buy" => {
            let profit = (current_price - entry_price) / entry_price * 100.0;
            Ok(profit >= profit_pct)
        }
        "short" | "sell" => {
            let profit = (entry_price - current_price) / entry_price * 100.0;
            Ok(profit >= profit_pct)
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Side must be 'long'/'buy' or 'short'/'sell'",
        )),
    }
}

/// Calculate trailing stop price
/// Returns: trailing stop price
#[pyfunction]
pub fn trailing_stop(highest_price: f64, lowest_price: f64, trail_pct: f64, side: &str) -> PyResult<f64> {
    if trail_pct <= 0.0 || trail_pct >= 100.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Trail percentage must be between 0 and 100",
        ));
    }

    match side {
        "long" | "buy" => Ok(highest_price * (1.0 - trail_pct / 100.0)),
        "short" | "sell" => Ok(lowest_price * (1.0 + trail_pct / 100.0)),
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Side must be 'long'/'buy' or 'short'/'sell'",
        )),
    }
}
