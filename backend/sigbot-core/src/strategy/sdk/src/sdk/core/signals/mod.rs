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

pub mod entry;
pub mod exit;

use pyo3::prelude::*;

pub use entry::{breakout, crossover, pattern_recognition};
pub use exit::{stop_loss, take_profit, trailing_stop};

/// Trading signals submodule for sigbotlib
#[pymodule]
#[pyo3(name = "signals")]
pub fn signals(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ===== Trading Signals =====
    m.add_function(wrap_pyfunction!(crossover, m)?)?;
    m.add_function(wrap_pyfunction!(breakout, m)?)?;
    m.add_function(wrap_pyfunction!(pattern_recognition, m)?)?;
    m.add_function(wrap_pyfunction!(stop_loss, m)?)?;
    m.add_function(wrap_pyfunction!(take_profit, m)?)?;
    m.add_function(wrap_pyfunction!(trailing_stop, m)?)?;

    Ok(())
}
