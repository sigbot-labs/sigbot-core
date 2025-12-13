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
use std::collections::VecDeque;

/// Series type for maintaining state in streaming mode
/// Similar to banta's Series, maintains a rolling window of values
#[pyclass]
#[derive(Clone, Debug)]
pub struct Series {
    data: VecDeque<f64>,
    max_length: Option<usize>,
}

#[pymethods]
impl Series {
    #[new]
    #[pyo3(signature = (max_length=None))]
    pub fn new(max_length: Option<usize>) -> Self {
        Self {
            data: VecDeque::new(),
            max_length,
        }
    }

    /// Append a new value to the series
    pub fn append(&mut self, value: f64) {
        self.data.push_back(value);
        if let Some(max_len) = self.max_length {
            while self.data.len() > max_len {
                self.data.pop_front();
            }
        }
    }

    /// Get value at index (0 = latest, -1 = previous, etc.)
    pub fn get(&self, index: isize) -> Option<f64> {
        let len = self.data.len() as isize;
        if index >= len || index < -len {
            return None;
        }
        let actual_index = if index >= 0 { len - 1 - index } else { len + index };
        self.data.get(actual_index as usize).copied()
    }

    /// Get the latest value (index 0)
    fn latest(&self) -> Option<f64> {
        self.get(0)
    }

    /// Get the length of the series
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Get the length of the series (for Python)
    fn __len__(&self) -> usize {
        self.data.len()
    }

    /// Check if series is empty
    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Convert to Python list
    fn to_list(&self) -> Vec<f64> {
        self.data.iter().copied().collect()
    }

    /// Clear all data
    fn clear(&mut self) {
        self.data.clear();
    }

    /// Iterator over values (for testing)
    fn iter(&self) -> Vec<f64> {
        self.to_list()
    }
}

/// Create a Series from a Vec<f64>
#[pyfunction]
#[pyo3(signature = (data, max_length=None))]
pub fn series_from_vec(data: Vec<f64>, max_length: Option<usize>) -> Series {
    let mut s = Series::new(max_length);
    for value in data {
        s.append(value);
    }
    s
}
