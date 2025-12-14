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

// see:https://github.com/TA-Lib/ta-lib-python/blob/master/tests/test_polars.py
pub mod momentum;
pub mod trend;
pub mod volatility;
pub mod volume;

use pyo3::prelude::*;

pub use momentum::{cci, cci_batch, rsi, rsi_batch, stochastic, stochastic_batch};
pub use trend::{ema, ema_batch, macd, macd_batch, sma, sma_batch};
pub use volatility::{atr, atr_batch, bollinger_bands, bollinger_bands_batch};
pub use volume::{obv, obv_batch, vwap, vwap_batch};

/// Indicators submodule for sigbotlib
#[pymodule]
#[pyo3(name = "indicators")]
pub fn indicators(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ===== Trend Indicators =====
    // Streaming mode
    m.add_function(wrap_pyfunction!(sma, m)?)?;
    m.add_function(wrap_pyfunction!(ema, m)?)?;
    m.add_function(wrap_pyfunction!(macd, m)?)?;

    // Batch mode
    m.add_function(wrap_pyfunction!(sma_batch, m)?)?;
    m.add_function(wrap_pyfunction!(ema_batch, m)?)?;
    m.add_function(wrap_pyfunction!(macd_batch, m)?)?;

    // ===== Momentum Indicators =====
    // Streaming mode
    m.add_function(wrap_pyfunction!(rsi, m)?)?;
    m.add_function(wrap_pyfunction!(stochastic, m)?)?;
    m.add_function(wrap_pyfunction!(cci, m)?)?;

    // Batch mode
    m.add_function(wrap_pyfunction!(rsi_batch, m)?)?;
    m.add_function(wrap_pyfunction!(stochastic_batch, m)?)?;
    m.add_function(wrap_pyfunction!(cci_batch, m)?)?;

    // ===== Volatility Indicators =====
    // Streaming mode
    m.add_function(wrap_pyfunction!(bollinger_bands, m)?)?;
    m.add_function(wrap_pyfunction!(atr, m)?)?;

    // Batch mode
    m.add_function(wrap_pyfunction!(bollinger_bands_batch, m)?)?;
    m.add_function(wrap_pyfunction!(atr_batch, m)?)?;

    // ===== Volume Indicators =====
    // Streaming mode
    m.add_function(wrap_pyfunction!(obv, m)?)?;
    m.add_function(wrap_pyfunction!(vwap, m)?)?;

    // Batch mode
    m.add_function(wrap_pyfunction!(obv_batch, m)?)?;
    m.add_function(wrap_pyfunction!(vwap_batch, m)?)?;

    Ok(())
}
