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

//! Test Assertions for E2E Tests
//!
//! Provides common assertion macros and utilities for testing.

use chrono::{DateTime, Utc};
use std::time::Duration;

/// Assert that a condition eventually becomes true within a timeout
pub async fn assert_eventually<F, Fut>(
    condition: F,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if condition().await {
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    Err("Condition did not become true within timeout".to_string())
}

/// Assert that a condition eventually becomes true with custom error message
pub async fn assert_eventually_with_message<F, Fut, M>(
    condition: F,
    timeout: Duration,
    poll_interval: Duration,
    message: M,
) -> Result<(), String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
    M: Fn() -> String,
{
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if condition().await {
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    Err(message())
}

/// Assert message is published to a topic
pub async fn assert_message_published<T>(
    get_messages: impl Fn() -> T,
    filter: impl Fn(&str) -> bool,
    timeout: Duration,
) -> Result<(), String>
where
    T: std::future::Future<Output = Vec<String>>,
{
    assert_eventually(
        || {
            let get_messages = &get_messages;
            let filter = &filter;
            async move {
                let messages = get_messages().await;
                messages.iter().any(|m| filter(m))
            }
        },
        timeout,
        Duration::from_millis(50),
    )
    .await
}

/// Assert JSON contains a field
pub fn assert_json_has_field(
    json: &serde_json::Value,
    field: &str,
    expected: &serde_json::Value,
) -> Result<(), String> {
    let actual = json.get(field).ok_or(format!("Field '{}' not found", field))?;

    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "Field '{}' mismatch: expected {:?}, got {:?}",
            field, expected, actual
        ))
    }
}

/// Assert JSON has a string field equal to expected
pub fn assert_json_string_field(
    json: &serde_json::Value,
    field: &str,
    expected: &str,
) -> Result<(), String> {
    let actual = json
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or(format!("Field '{}' not found or not a string", field))?;

    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "Field '{}' mismatch: expected '{}', got '{}'",
            field, expected, actual
        ))
    }
}

/// Assert JSON has a number field equal to expected
pub fn assert_json_number_field(
    json: &serde_json::Value,
    field: &str,
    expected: f64,
) -> Result<(), String> {
    let actual = json
        .get(field)
        .and_then(|v| v.as_f64())
        .ok_or(format!("Field '{}' not found or not a number", field))?;

    if (actual - expected).abs() < f64::EPSILON {
        Ok(())
    } else {
        Err(format!(
            "Field '{}' mismatch: expected {}, got {}",
            field, expected, actual
        ))
    }
}

/// Assert JSON has an array field with expected length
pub fn assert_json_array_length(
    json: &serde_json::Value,
    field: &str,
    expected_len: usize,
) -> Result<(), String> {
    let array = json
        .get(field)
        .and_then(|v| v.as_array())
        .ok_or(format!("Field '{}' not found or not an array", field))?;

    if array.len() == expected_len {
        Ok(())
    } else {
        Err(format!(
            "Field '{}' length mismatch: expected {}, got {}",
            field, expected_len,
            array.len()
        ))
    }
}

/// Trait for comparing floating point numbers with tolerance
pub trait FloatEq {
    fn approx_eq(&self, other: &Self, tolerance: f64) -> bool;
}

impl FloatEq for f64 {
    fn approx_eq(&self, other: &Self, tolerance: f64) -> bool {
        (self - other).abs() < tolerance
    }
}

impl FloatEq for f32 {
    fn approx_eq(&self, other: &Self, tolerance: f64) -> bool {
        (*self as f64 - *other as f64).abs() < tolerance
    }
}

/// Assert two floats are approximately equal
pub fn assert_float_eq(actual: f64, expected: f64, tolerance: f64) -> Result<(), String> {
    if actual.approx_eq(&expected, tolerance) {
        Ok(())
    } else {
        Err(format!(
            "Float mismatch: expected {}, got {} (tolerance: {})",
            expected, actual, tolerance
        ))
    }
}

/// Assert a value is within a range
pub fn assert_in_range<T: Ord + std::fmt::Display>(value: T, min: T, max: T) -> Result<(), String> {
    if value >= min && value <= max {
        Ok(())
    } else {
        Err(format!(
            "Value {} is not in range [{}, {}]",
            value, min, max
        ))
    }
}

/// Assert a value is positive
pub fn assert_positive(value: f64) -> Result<(), String> {
    if value > 0.0 {
        Ok(())
    } else {
        Err(format!("Value {} is not positive", value))
    }
}

/// Assert a value is non-negative
pub fn assert_non_negative(value: f64) -> Result<(), String> {
    if value >= 0.0 {
        Ok(())
    } else {
        Err(format!("Value {} is negative", value))
    }
}

/// Assert two datetime values are within a duration
pub fn assert_datetime_within(
    actual: DateTime<Utc>,
    expected: DateTime<Utc>,
    tolerance: Duration,
) -> Result<(), String> {
    let diff = actual.signed_duration_since(expected);
    let tolerance = chrono::Duration::from_std(tolerance).unwrap();

    if diff.abs() <= tolerance {
        Ok(())
    } else {
        Err(format!(
            "Datetime {:?} is not within {:?} of {:?}",
            actual, tolerance, expected
        ))
    }
}

/// Macro for asserting with retry
#[macro_export]
macro_rules! assert_with_retry {
    ($condition:expr, $retries:expr, $delay:expr) => {{
        let mut last_error = None;
        for _ in 0..$retries {
            match $condition {
                Ok(_) => break,
                Err(e) => {
                    last_error = Some(e);
                    tokio::time::sleep($delay).await;
                }
            }
        }
        if let Some(e) = last_error {
            Err(e)
        } else {
            Ok(())
        }
    }};
}

/// Macro for asserting all elements in a collection satisfy a condition
#[macro_export]
macro_rules! assert_all {
    ($collection:expr, $condition:expr) => {{
        for (index, item) in $collection.iter().enumerate() {
            if !$condition(item) {
                return Err(format!("Assertion failed for element at index {}", index));
            }
        }
        Ok(())
    }};
}

/// Macro for asserting any element in a collection satisfies a condition
#[macro_export]
macro_rules! assert_any {
    ($collection:expr, $condition:expr) => {{
        if !$collection.iter().any(|item| $condition(item)) {
            return Err("No element satisfied the condition".to_string());
        }
        Ok(())
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn test_assert_eventually_success() {
        let counter = std::sync::Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });

        let result = assert_eventually(
            || {
                let counter = counter.clone();
                async move { counter.load(std::sync::atomic::Ordering::SeqCst) > 0 }
            },
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_assert_eventually_timeout() {
        let result = assert_eventually(
            || async move { false },
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_err());
    }

    #[test]
    fn test_assert_json_string_field() {
        let json = serde_json::json!({
            "name": "test",
            "value": 42
        });

        assert!(assert_json_string_field(&json, "name", "test").is_ok());
        assert!(assert_json_string_field(&json, "name", "wrong").is_err());
        assert!(assert_json_string_field(&json, "missing", "test").is_err());
    }

    #[test]
    fn test_assert_json_number_field() {
        let json = serde_json::json!({
            "value": 42
        });

        assert!(assert_json_number_field(&json, "value", 42.0).is_ok());
        assert!(assert_json_number_field(&json, "value", 43.0).is_err());
    }

    #[test]
    fn test_float_eq() {
        assert!(1.0.approx_eq(&1.0, 0.001));
        assert!(1.0005.approx_eq(&1.0006, 0.001));
        assert!(!1.01.approx_eq(&1.0, 0.001));
    }

    #[test]
    fn test_assert_float_eq() {
        assert!(assert_float_eq(1.0, 1.0, 0.001).is_ok());
        assert!(assert_float_eq(1.0005, 1.0006, 0.001).is_ok());
        assert!(assert_float_eq(1.01, 1.0, 0.001).is_err());
    }

    #[test]
    fn test_assert_in_range() {
        assert!(assert_in_range(5, 1, 10).is_ok());
        assert!(assert_in_range(1, 1, 10).is_ok());
        assert!(assert_in_range(10, 1, 10).is_ok());
        assert!(assert_in_range(0, 1, 10).is_err());
        assert!(assert_in_range(11, 1, 10).is_err());
    }

    #[test]
    fn test_assert_positive() {
        assert!(assert_positive(1.0).is_ok());
        assert!(assert_positive(0.0001).is_ok());
        assert!(assert_positive(0.0).is_err());
        assert!(assert_positive(-1.0).is_err());
    }

    #[test]
    fn test_assert_non_negative() {
        assert!(assert_non_negative(1.0).is_ok());
        assert!(assert_non_negative(0.0).is_ok());
        assert!(assert_non_negative(-1.0).is_err());
    }
}
