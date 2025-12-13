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

/// Calculate maximum drawdown from equity curve
/// Returns: maximum drawdown as percentage
#[pyfunction]
pub fn max_drawdown(equity_curve: Vec<f64>) -> PyResult<f64> {
    if equity_curve.is_empty() {
        return Ok(0.0);
    }

    let mut max_equity = equity_curve[0];
    let mut max_dd = 0.0;

    for &equity in &equity_curve {
        if equity > max_equity {
            max_equity = equity;
        }

        let drawdown = (max_equity - equity) / max_equity * 100.0;
        if drawdown > max_dd {
            max_dd = drawdown;
        }
    }

    Ok(max_dd)
}

/// Calculate drawdown duration
/// Returns: maximum drawdown duration in periods
#[pyfunction]
pub fn drawdown_duration(equity_curve: Vec<f64>) -> PyResult<usize> {
    if equity_curve.is_empty() {
        return Ok(0);
    }

    let mut max_equity = equity_curve[0];
    let mut max_duration = 0;
    let mut current_duration = 0;

    for &equity in &equity_curve {
        if equity > max_equity {
            max_equity = equity;
            current_duration = 0;
        } else if equity < max_equity {
            current_duration += 1;
            if current_duration > max_duration {
                max_duration = current_duration;
            }
        } else {
            current_duration = 0;
        }
    }

    Ok(max_duration)
}
