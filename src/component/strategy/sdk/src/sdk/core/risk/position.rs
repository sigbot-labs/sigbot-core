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

/// Calculate position size based on risk percentage
#[pyfunction]
pub fn calculate_position_size(balance: f64, risk_pct: f64, entry_price: f64, stop_loss: f64) -> PyResult<f64> {
    if balance <= 0.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Balance must be greater than 0",
        ));
    }

    if risk_pct <= 0.0 || risk_pct >= 100.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Risk percentage must be between 0 and 100",
        ));
    }

    if entry_price <= 0.0 || stop_loss <= 0.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Entry price and stop loss must be greater than 0",
        ));
    }

    let risk_amount = balance * (risk_pct / 100.0);
    let price_diff = (entry_price - stop_loss).abs();

    if price_diff == 0.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Entry price and stop loss cannot be the same",
        ));
    }

    Ok(risk_amount / price_diff)
}

/// Calculate leverage
#[pyfunction]
pub fn calculate_leverage(position_value: f64, margin: f64) -> PyResult<f64> {
    if margin <= 0.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Margin must be greater than 0",
        ));
    }

    Ok(position_value / margin)
}

/// Check margin requirement
#[pyfunction]
pub fn check_margin_requirement(
    position_value: f64,
    margin_requirement_pct: f64,
    available_margin: f64,
) -> PyResult<bool> {
    if margin_requirement_pct <= 0.0 || margin_requirement_pct >= 100.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Margin requirement percentage must be between 0 and 100",
        ));
    }

    let required_margin = position_value * (margin_requirement_pct / 100.0);
    Ok(available_margin >= required_margin)
}
