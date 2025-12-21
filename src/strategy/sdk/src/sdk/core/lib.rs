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

use crate::sdk::core::models::time_series::{series_from_vec, TimeSeries};
use crate::sdk::core::models::trade_signal::{
    PyEntryPosition, PyExitPosition, PyOrderType, PyTradeSide, TradingSignal,
};
use crate::sdk::core::data::data;
use crate::sdk::core::indicators::indicators;
use crate::sdk::core::risk::risk;
use crate::sdk::core::signals::signals;
use crate::sdk::core::utils::utils;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pymodule;

/// Sigbot strategy Library Python module
/// Provides high-performance technical indicators and data processing functions
#[pymodule]
#[pyo3(name = "sigbotlib")]
pub fn sigbotlib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register Series class
    m.add_class::<TimeSeries>()?;

    // Register trading signal classes
    m.add_class::<TradingSignal>()?;
    m.add_class::<PyEntryPosition>()?;
    m.add_class::<PyExitPosition>()?;
    m.add_class::<PyTradeSide>()?;
    m.add_class::<PyOrderType>()?;

    // Register series utility functions
    m.add_function(wrap_pyfunction!(series_from_vec, m)?)?;

    // Register submodules
    // This allows importing submodules nicely from Python
    // e.g. import sigbotlib.indicators as ind
    m.add_wrapped(wrap_pymodule!(indicators))?;
    m.add_wrapped(wrap_pymodule!(risk))?;
    m.add_wrapped(wrap_pymodule!(signals))?;
    m.add_wrapped(wrap_pymodule!(utils))?;
    m.add_wrapped(wrap_pymodule!(data))?;

    // Inserting to sys.modules allows importing submodules nicely from Python
    // e.g. import sigbotlib.indicators as ind
    let py = m.py();
    let sys = py.import_bound("sys")?;
    let sys_modules = sys.getattr("modules")?;
    let sys_modules_dict = sys_modules.downcast::<PyDict>()?;
    sys_modules_dict.set_item("sigbotlib.indicators", m.getattr("indicators")?)?;
    sys_modules_dict.set_item("sigbotlib.risk", m.getattr("risk")?)?;
    sys_modules_dict.set_item("sigbotlib.signals", m.getattr("signals")?)?;
    sys_modules_dict.set_item("sigbotlib.utils", m.getattr("utils")?)?;
    sys_modules_dict.set_item("sigbotlib.data", m.getattr("data")?)?;

    Ok(())
}
