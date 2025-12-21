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

pub mod math;
pub mod time;

use pyo3::prelude::*;

pub use math::{correlation, normalize, sharpe_ratio, standardize};
pub use time::{align_timestamps, datetime_to_timestamp, timestamp_to_datetime};

/// Utility functions submodule for sigbotlib
#[pymodule]
#[pyo3(name = "utils")]
pub fn utils(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ===== Utility Functions =====
    m.add_function(wrap_pyfunction!(normalize, m)?)?;
    m.add_function(wrap_pyfunction!(standardize, m)?)?;
    m.add_function(wrap_pyfunction!(correlation, m)?)?;
    m.add_function(wrap_pyfunction!(sharpe_ratio, m)?)?;
    m.add_function(wrap_pyfunction!(timestamp_to_datetime, m)?)?;
    m.add_function(wrap_pyfunction!(datetime_to_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(align_timestamps, m)?)?;

    Ok(())
}
