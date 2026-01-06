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
use sigbot_types::modules::exchange::models::trade_position::{
    EntryPosition, PlaceTradeSignal, ExitTradePosition, OrderType, TradeSide,
};

/// Trade side enumeration for Python
#[pyclass]
#[derive(Clone, Debug)]
pub struct PyTradeSide {
    inner: TradeSide,
}

#[pymethods]
impl PyTradeSide {
    #[new]
    pub fn new(side: &str) -> PyResult<Self> {
        let inner = TradeSide::from_str(side).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
        Ok(Self { inner })
    }

    #[staticmethod]
    pub fn long() -> Self {
        Self { inner: TradeSide::LONG }
    }

    #[staticmethod]
    pub fn short() -> Self {
        Self {
            inner: TradeSide::SHORT,
        }
    }

    pub fn __str__(&self) -> String {
        self.inner.to_pos_str().to_string()
    }

    pub fn __repr__(&self) -> String {
        format!("TradeSide.{}", self.inner.to_pos_str())
    }
}

impl From<PyTradeSide> for TradeSide {
    fn from(py_side: PyTradeSide) -> Self {
        py_side.inner
    }
}

/// Order type enumeration for Python
#[pyclass]
#[derive(Clone, Debug)]
pub struct PyOrderType {
    inner: OrderType,
}

#[pymethods]
impl PyOrderType {
    #[new]
    pub fn new(order_type: &str) -> PyResult<Self> {
        let inner = OrderType::from_str(order_type).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
        Ok(Self { inner })
    }

    #[staticmethod]
    pub fn market() -> Self {
        Self {
            inner: OrderType::MARKET,
        }
    }

    #[staticmethod]
    pub fn limited() -> Self {
        Self {
            inner: OrderType::LIMITED,
        }
    }

    pub fn __str__(&self) -> String {
        self.inner.to_str().to_string()
    }

    pub fn __repr__(&self) -> String {
        format!("OrderType.{}", self.inner.to_str())
    }
}

impl From<PyOrderType> for OrderType {
    fn from(py_order_type: PyOrderType) -> Self {
        py_order_type.inner
    }
}

/// Entry position for Python
#[pyclass]
#[derive(Clone, Debug)]
pub struct PyEntryPosition {
    pub time: u64,
    pub symbol: String,
    pub side: PyTradeSide,
    pub order_type: PyOrderType,
    pub price: Option<f64>,
    pub quantity: f64,
    pub maker_only: bool,
}

#[pymethods]
impl PyEntryPosition {
    #[new]
    #[pyo3(signature = (symbol, side, quantity, *, time=0, order_type=None, price=None, maker_only=false))]
    pub fn new(
        symbol: String,
        side: &Bound<'_, PyAny>,
        quantity: f64,
        time: u64,
        order_type: Option<&Bound<'_, PyAny>>,
        price: Option<f64>,
        maker_only: bool,
    ) -> PyResult<Self> {
        // Parse side
        let py_side = if side.is_instance_of::<PyTradeSide>() {
            side.extract::<PyRef<PyTradeSide>>()?.clone()
        } else if let Ok(side_str) = side.extract::<String>() {
            PyTradeSide::new(&side_str)?
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "side must be TradeSide or string",
            ));
        };

        // Parse order_type
        let py_order_type = if let Some(ot) = order_type {
            if ot.is_instance_of::<PyOrderType>() {
                ot.extract::<PyRef<PyOrderType>>()?.clone()
            } else if let Ok(ot_str) = ot.extract::<String>() {
                PyOrderType::new(&ot_str)?
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "order_type must be OrderType or string",
                ));
            }
        } else {
            // Default to MARKET if price is None, LIMITED if price is Some
            if price.is_some() {
                PyOrderType::limited()
            } else {
                PyOrderType::market()
            }
        };

        Ok(Self {
            time,
            symbol,
            side: py_side,
            order_type: py_order_type,
            price,
            quantity,
            maker_only,
        })
    }
}

impl From<PyEntryPosition> for EntryPosition {
    fn from(py_pos: PyEntryPosition) -> Self {
        Self {
            time: py_pos.time,
            symbol: py_pos.symbol,
            side: py_pos.side.into(),
            order_type: py_pos.order_type.into(),
            price: py_pos.price,
            quantity: py_pos.quantity,
            maker_only: py_pos.maker_only,
        }
    }
}

/// Exit position for Python
#[pyclass]
#[derive(Clone, Debug)]
pub struct PyExitPosition {
    pub time: u64,
    pub symbol: String,
    pub side: PyTradeSide,
    pub order_type: PyOrderType,
    pub price: Option<f64>,
    pub quantity_percent: f64,
}

#[pymethods]
impl PyExitPosition {
    #[new]
    #[pyo3(signature = (symbol, side, quantity_percent, *, time=0, order_type=None, price=None))]
    pub fn new(
        symbol: String,
        side: &Bound<'_, PyAny>,
        quantity_percent: f64,
        time: u64,
        order_type: Option<&Bound<'_, PyAny>>,
        price: Option<f64>,
    ) -> PyResult<Self> {
        // Parse side
        let py_side = if side.is_instance_of::<PyTradeSide>() {
            side.extract::<PyRef<PyTradeSide>>()?.clone()
        } else if let Ok(side_str) = side.extract::<String>() {
            PyTradeSide::new(&side_str)?
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "side must be TradeSide or string",
            ));
        };

        // Parse order_type
        let py_order_type = if let Some(ot) = order_type {
            if ot.is_instance_of::<PyOrderType>() {
                ot.extract::<PyRef<PyOrderType>>()?.clone()
            } else if let Ok(ot_str) = ot.extract::<String>() {
                PyOrderType::new(&ot_str)?
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "order_type must be OrderType or string",
                ));
            }
        } else {
            // Default to MARKET if price is None, LIMITED if price is Some
            if price.is_some() {
                PyOrderType::limited()
            } else {
                PyOrderType::market()
            }
        };

        Ok(Self {
            time,
            symbol,
            side: py_side,
            order_type: py_order_type,
            price,
            quantity_percent,
        })
    }
}

impl From<PyExitPosition> for ExitTradePosition {
    fn from(py_pos: PyExitPosition) -> Self {
        Self {
            time: py_pos.time,
            symbol: py_pos.symbol,
            side: py_pos.side.into(),
            order_type: py_pos.order_type.into(),
            price: py_pos.price,
            quantity_percent: py_pos.quantity_percent,
        }
    }
}

/// Trading signal for Python strategy developers
/// This corresponds to EntryTradePosition in Rust
#[pyclass]
#[derive(Clone, Debug)]
pub struct TradingSignal {
    pub open_pos: PyEntryPosition,
    pub stop_loss: Option<PyExitPosition>,
    pub stop_profit: Option<PyExitPosition>,
    pub description: String,
}

#[pymethods]
impl TradingSignal {
    #[new]
    #[pyo3(signature = (open_pos, *, stop_loss=None, stop_profit=None, description=""))]
    pub fn new(
        open_pos: PyEntryPosition,
        stop_loss: Option<PyExitPosition>,
        stop_profit: Option<PyExitPosition>,
        description: &str,
    ) -> Self {
        Self {
            open_pos,
            stop_loss,
            stop_profit,
            description: description.to_string(),
        }
    }

    /// Convenience constructor for creating a long position signal
    #[staticmethod]
    #[pyo3(signature = (symbol, quantity, *, price=None, stop_loss_price=None, stop_profit_price=None, time=0, description=""))]
    pub fn long(
        symbol: String,
        quantity: f64,
        price: Option<f64>,
        stop_loss_price: Option<f64>,
        stop_profit_price: Option<f64>,
        time: u64,
        description: &str,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            let order_type = if price.is_some() {
                PyOrderType::limited()
            } else {
                PyOrderType::market()
            };

            let long_side = PyTradeSide::long().into_py(py);
            let order_type_py = order_type.into_py(py);
            let open_pos = PyEntryPosition::new(
                symbol.clone(),
                long_side.bind(py),
                quantity,
                time,
                Some(order_type_py.bind(py)),
                price,
                false,
            )?;

            let stop_loss = stop_loss_price
                .map(|sl_price| {
                    let short_side = PyTradeSide::short().into_py(py);
                    let limited_order = PyOrderType::limited().into_py(py);
                    PyExitPosition::new(
                        symbol.clone(),
                        short_side.bind(py),
                        1.0, // 100% of position
                        time,
                        Some(limited_order.bind(py)),
                        Some(sl_price),
                    )
                })
                .transpose()?;

            let stop_profit = stop_profit_price
                .map(|tp_price| {
                    let short_side = PyTradeSide::short().into_py(py);
                    let limited_order = PyOrderType::limited().into_py(py);
                    PyExitPosition::new(
                        symbol.clone(),
                        short_side.bind(py),
                        1.0, // 100% of position
                        time,
                        Some(limited_order.bind(py)),
                        Some(tp_price),
                    )
                })
                .transpose()?;

            Ok(Self {
                open_pos,
                stop_loss,
                stop_profit,
                description: description.to_string(),
            })
        })
    }

    /// Convenience constructor for creating a short position signal
    #[staticmethod]
    #[pyo3(signature = (symbol, quantity, *, price=None, stop_loss_price=None, stop_profit_price=None, time=0, description=""))]
    pub fn short(
        symbol: String,
        quantity: f64,
        price: Option<f64>,
        stop_loss_price: Option<f64>,
        stop_profit_price: Option<f64>,
        time: u64,
        description: &str,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            let order_type = if price.is_some() {
                PyOrderType::limited()
            } else {
                PyOrderType::market()
            };

            let short_side = PyTradeSide::short().into_py(py);
            let order_type_py = order_type.into_py(py);
            let open_pos = PyEntryPosition::new(
                symbol.clone(),
                short_side.bind(py),
                quantity,
                time,
                Some(order_type_py.bind(py)),
                price,
                false,
            )?;

            let stop_loss = stop_loss_price
                .map(|sl_price| {
                    let long_side = PyTradeSide::long().into_py(py);
                    let limited_order = PyOrderType::limited().into_py(py);
                    PyExitPosition::new(
                        symbol.clone(),
                        long_side.bind(py),
                        1.0, // 100% of position
                        time,
                        Some(limited_order.bind(py)),
                        Some(sl_price),
                    )
                })
                .transpose()?;

            let stop_profit = stop_profit_price
                .map(|tp_price| {
                    let long_side = PyTradeSide::long().into_py(py);
                    let limited_order = PyOrderType::limited().into_py(py);
                    PyExitPosition::new(
                        symbol.clone(),
                        long_side.bind(py),
                        1.0, // 100% of position
                        time,
                        Some(limited_order.bind(py)),
                        Some(tp_price),
                    )
                })
                .transpose()?;

            Ok(Self {
                open_pos,
                stop_loss,
                stop_profit,
                description: description.to_string(),
            })
        })
    }
}

impl TradingSignal {
    /// Convert to Rust EntryTradePosition (internal use only)
    pub fn to_entry_trade_position(&self) -> PlaceTradeSignal {
        PlaceTradeSignal {
            enter_pos: self.open_pos.clone().into(),
            exit_loss: self.stop_loss.as_ref().map(|sl| sl.clone().into()),
            exit_profit: self.stop_profit.as_ref().map(|sp| sp.clone().into()),
            description: self.description.clone(),
        }
    }
}
