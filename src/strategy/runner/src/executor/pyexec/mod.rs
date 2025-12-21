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

pub mod batch_executor;
pub mod streaming_executor;

pub mod pyo3_utils {
    use anyhow::Result;
    use pyo3::prelude::*;
    use serde_json;

    /// Convert Python object to serde_json::Value (no Python json module call)
    pub fn py_to_json_value<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> Result<serde_json::Value> {
        use pyo3::types::{PyDict, PyList};

        if obj.is_none() {
            return Ok(serde_json::Value::Null);
        }

        // Try to extract as common types first
        if let Ok(s) = obj.extract::<String>() {
            return Ok(serde_json::Value::String(s));
        }
        if let Ok(i) = obj.extract::<i64>() {
            return Ok(serde_json::Value::Number(i.into()));
        }
        if let Ok(f) = obj.extract::<f64>() {
            return Ok(serde_json::json!(f));
        }
        if let Ok(b) = obj.extract::<bool>() {
            return Ok(serde_json::Value::Bool(b));
        }

        // Handle dict
        if let Ok(dict) = obj.downcast::<PyDict>() {
            let mut map = serde_json::Map::new();
            for (key, value) in dict.iter() {
                let key_str = key
                    .extract::<String>()
                    .map_err(|_| anyhow::anyhow!("Dict key must be string"))?;
                let value_json = py_to_json_value(py, &value)?;
                map.insert(key_str, value_json);
            }
            return Ok(serde_json::Value::Object(map));
        }

        // Handle list
        if let Ok(list) = obj.downcast::<PyList>() {
            let mut vec = Vec::new();
            for item in list.iter() {
                vec.push(py_to_json_value(py, &item)?);
            }
            return Ok(serde_json::Value::Array(vec));
        }

        // Fallback: convert to string
        Ok(serde_json::Value::String(obj.to_string()))
    }
}
