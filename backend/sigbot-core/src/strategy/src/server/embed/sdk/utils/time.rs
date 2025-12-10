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

/// Convert timestamp (milliseconds) to datetime string
#[pyfunction]
pub fn timestamp_to_datetime(timestamp: u64) -> PyResult<String> {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

    let secs = (timestamp / 1000) as i64;
    let millis = (timestamp % 1000) as u32;
    let nanos = millis * 1_000_000;

    let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid timestamp"))?;
    Ok(dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
}

/// Convert datetime string to timestamp (milliseconds)
#[pyfunction]
pub fn datetime_to_timestamp(datetime: &str) -> PyResult<u64> {
    use chrono::{DateTime, NaiveDateTime, Utc};

    // Try parsing common datetime formats
    let dt_result: Result<DateTime<Utc>, _> = DateTime::parse_from_rfc3339(datetime)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").map(|ndt| DateTime::from_utc(ndt, Utc))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S%.3f").map(|ndt| DateTime::from_utc(ndt, Utc))
        });

    let dt = dt_result.map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid datetime format"))?;

    Ok(dt.timestamp_millis() as u64)
}

/// Align timestamps between two datasets
/// Returns: aligned indices for both datasets
#[pyfunction]
pub fn align_timestamps(timestamps1: Vec<u64>, timestamps2: Vec<u64>) -> PyResult<(Vec<usize>, Vec<usize>)> {
    let mut indices1 = Vec::new();
    let mut indices2 = Vec::new();

    let mut i = 0;
    let mut j = 0;

    while i < timestamps1.len() && j < timestamps2.len() {
        if timestamps1[i] == timestamps2[j] {
            indices1.push(i);
            indices2.push(j);
            i += 1;
            j += 1;
        } else if timestamps1[i] < timestamps2[j] {
            i += 1;
        } else {
            j += 1;
        }
    }

    Ok((indices1, indices2))
}
