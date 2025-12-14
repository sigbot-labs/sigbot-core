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

pub mod kline;
pub mod resample;
pub mod tick;

use pyo3::prelude::*;
use pyo3::wrap_pymodule;

pub use kline::{filter_klines, parse_klines, validate_kline};
pub use resample::{resample_klines, resample_ohlcv};
pub use tick::{parse_ticks, ticks_to_klines};

/// Data processing submodule for sigbotlib
#[pymodule]
#[pyo3(name = "data")]
pub fn data(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ===== Data Processing =====
    m.add_function(wrap_pyfunction!(parse_klines, m)?)?;
    m.add_function(wrap_pyfunction!(validate_kline, m)?)?;
    m.add_function(wrap_pyfunction!(filter_klines, m)?)?;
    m.add_function(wrap_pyfunction!(resample_klines, m)?)?;
    m.add_function(wrap_pyfunction!(resample_ohlcv, m)?)?;
    m.add_function(wrap_pyfunction!(ticks_to_klines, m)?)?;

    Ok(())
}
