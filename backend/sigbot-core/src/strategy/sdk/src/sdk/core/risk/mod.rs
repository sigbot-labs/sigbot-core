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

pub mod drawdown;
pub mod position;

use pyo3::prelude::*;

pub use drawdown::{drawdown_duration, max_drawdown};
pub use position::{calculate_leverage, calculate_position_size, check_margin_requirement};

/// Risk management submodule for sigbotlib
#[pymodule]
#[pyo3(name = "risk")]
pub fn risk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ===== Risk Management =====
    m.add_function(wrap_pyfunction!(calculate_position_size, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_leverage, m)?)?;
    m.add_function(wrap_pyfunction!(check_margin_requirement, m)?)?;
    m.add_function(wrap_pyfunction!(max_drawdown, m)?)?;
    m.add_function(wrap_pyfunction!(drawdown_duration, m)?)?;

    Ok(())
}
