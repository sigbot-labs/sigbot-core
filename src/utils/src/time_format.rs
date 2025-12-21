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

use chrono::{Local, Timelike};
use std::fmt;

/// Formatting to ISO-8601 standard with timezone offset: YYYY-MM-DDTHH:MM:SS.nnn+HH:MM
pub struct Iso8601TimeFormatter {
    ansi_enabled: bool,
}

impl Iso8601TimeFormatter {
    /// Create a new ISO 8601 time formatter.
    ///
    /// # Arguments
    /// * `ansi_enabled` - Whether to enable ANSI color output.
    pub fn new(ansi_enabled: bool) -> Self {
        Self { ansi_enabled }
    }

    /// Format the current time to ISO 8601 format string with timezone offset.
    ///
    /// # Returns
    /// The formatted time string, format: YYYY-MM-DDTHH:MM:SS.nnn+HH or YYYY-MM-DDTHH:MM:SS.nnn+HH:MM
    pub fn format_now(&self) -> String {
        let now = Local::now();
        let offset = now.offset();

        // Format timezone offset as +HH or +HH:MM (if minutes are non-zero)
        let offset_seconds = offset.local_minus_utc();
        let offset_hours = offset_seconds / 3600;
        let offset_minutes = (offset_seconds.abs() % 3600) / 60;

        let offset_sign = if offset_seconds >= 0 { '+' } else { '-' };

        let offset_str = if offset_minutes == 0 {
            format!("{}{:02}", offset_sign, offset_hours.abs())
        } else {
            format!("{}{:02}{:02}", offset_sign, offset_hours.abs(), offset_minutes)
        };

        format!(
            "{}.{:03}{}",
            now.format("%Y-%m-%dT%H:%M:%S"),
            now.nanosecond() / 1_000_000, // Convert to milliseconds
            offset_str
        )
    }

    /// Format the time to ISO 8601 format string (with ANSI color).
    ///
    /// # Returns
    /// The formatted time string, if ANSI is enabled, the time part will be gray.
    pub fn format_now_colored(&self) -> String {
        let formatted = self.format_now();
        if self.ansi_enabled {
            format!("\x1b[90m{formatted}\x1b[0m")
        } else {
            formatted
        }
    }
}

impl tracing_subscriber::fmt::time::FormatTime for Iso8601TimeFormatter {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> fmt::Result {
        let now = Local::now();
        let offset = now.offset();

        // Format the timestamp (ISO 8601 format with timezone offset: YYYY-MM-DDTHH:MM:SS.nnn+HH or +HH:MM)
        let time_color = if self.ansi_enabled { "\x1b[90m" } else { "" };
        let reset = if self.ansi_enabled { "\x1b[0m" } else { "" };

        // Format timezone offset as +HH or +HH:MM (if minutes are non-zero)
        let offset_seconds = offset.local_minus_utc();
        let offset_hours = offset_seconds / 3600;
        let offset_minutes = (offset_seconds.abs() % 3600) / 60;

        let offset_sign = if offset_seconds >= 0 { '+' } else { '-' };

        let offset_str = if offset_minutes == 0 {
            format!("{}{:02}", offset_sign, offset_hours.abs())
        } else {
            format!("{}{:02}{:02}", offset_sign, offset_hours.abs(), offset_minutes)
        };

        let formatted = format!(
            "{}.{:03}{}",
            now.format("%Y-%m-%dT%H:%M:%S"),
            now.nanosecond() / 1_000_000, // Convert to milliseconds
            offset_str
        );
        write!(w, "{time_color}{formatted}{reset}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iso8601_format() {
        let formatter = Iso8601TimeFormatter::new(false);
        let formatted = formatter.format_now();
        // Verify the format: YYYY-MM-DDTHH:MM:SS.nnn+HH:MM
        assert!(formatted.contains('T'));
        assert!(formatted.contains('+') || formatted.contains('-'));
        // Should be around 23-24 characters: 2025-12-21T10:14:17.356+08
        assert!(formatted.len() >= 20);
    }
}
