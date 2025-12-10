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

use crate::server::embed::sdk::data::{
    filter_klines, parse_klines, resample_klines, resample_ohlcv, ticks_to_klines, validate_kline,
};
use crate::server::embed::sdk::indicators::{
    atr, atr_batch, bollinger_bands, bollinger_bands_batch, cci, cci_batch, ema, ema_batch, macd, macd_batch, obv,
    obv_batch, rsi, rsi_batch, sma, sma_batch, stochastic, stochastic_batch, vwap, vwap_batch,
};
use crate::server::embed::sdk::risk::{
    calculate_leverage, calculate_position_size, check_margin_requirement, drawdown_duration, max_drawdown,
};
use crate::server::embed::sdk::series::{series_from_vec, Series};
use crate::server::embed::sdk::signals::{
    breakout, crossover, pattern_recognition, stop_loss, take_profit, trailing_stop,
};
use crate::server::embed::sdk::utils::{
    align_timestamps, correlation, datetime_to_timestamp, normalize, sharpe_ratio, standardize, timestamp_to_datetime,
};
use pyo3::prelude::*;

/// Sigbot SDK Python module
/// Provides high-performance technical indicators and data processing functions
#[pymodule]
#[pyo3(name = "sigbot_sdk")]
pub fn sigbot_sdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register Series class
    m.add_class::<Series>()?;

    // Register series utility functions
    m.add_function(wrap_pyfunction!(series_from_vec, m)?)?;

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

    // ===== Data Processing =====
    m.add_function(wrap_pyfunction!(parse_klines, m)?)?;
    m.add_function(wrap_pyfunction!(validate_kline, m)?)?;
    m.add_function(wrap_pyfunction!(filter_klines, m)?)?;
    m.add_function(wrap_pyfunction!(resample_klines, m)?)?;
    m.add_function(wrap_pyfunction!(resample_ohlcv, m)?)?;
    m.add_function(wrap_pyfunction!(ticks_to_klines, m)?)?;

    // ===== Trading Signals =====
    m.add_function(wrap_pyfunction!(crossover, m)?)?;
    m.add_function(wrap_pyfunction!(breakout, m)?)?;
    m.add_function(wrap_pyfunction!(pattern_recognition, m)?)?;
    m.add_function(wrap_pyfunction!(stop_loss, m)?)?;
    m.add_function(wrap_pyfunction!(take_profit, m)?)?;
    m.add_function(wrap_pyfunction!(trailing_stop, m)?)?;

    // ===== Risk Management =====
    m.add_function(wrap_pyfunction!(calculate_position_size, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_leverage, m)?)?;
    m.add_function(wrap_pyfunction!(check_margin_requirement, m)?)?;
    m.add_function(wrap_pyfunction!(max_drawdown, m)?)?;
    m.add_function(wrap_pyfunction!(drawdown_duration, m)?)?;

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
