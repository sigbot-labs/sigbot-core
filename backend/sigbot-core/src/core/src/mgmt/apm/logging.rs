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

use crate::config::config::AppConfig;
use serde::{Deserialize, Serialize};
use sigbot_utils::time_format::Iso8601TimeFormatter;
use std::{
    fmt::{self, Display},
    io::{IsTerminal, LineWriter},
    str::FromStr,
    sync::Arc,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    filter::Targets,
    fmt::{
        format::{FormatEvent, FormatFields},
        time::FormatTime,
        FmtContext, FormatFields as FormatFieldsTrait,
    },
    EnvFilter, Layer,
};

pub type LogRouteHandle = tracing_subscriber::reload::Handle<LogRouteType, tracing_subscriber::Registry>;

pub type LogRouteType = tracing_subscriber::filter::Filtered<
    Option<Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>>,
    Targets,
    tracing_subscriber::Registry,
>;

pub type SubscriberForSecondLayer = tracing_subscriber::layer::Layered<
    tracing_subscriber::reload::Layer<LogRouteType, tracing_subscriber::Registry>,
    tracing_subscriber::Registry,
>;

pub type LogStderrHandle = tracing_subscriber::reload::Handle<LogStderrType, SubscriberForSecondLayer>;

pub type LogStderrType = tracing_subscriber::filter::Filtered<
    Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>,
    Targets,
    SubscriberForSecondLayer,
>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogMode {
    #[default]
    HUMAN,
    JSON,
}

impl Display for LogMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogMode::HUMAN => Display::fmt("HUMAN", f),
            LogMode::JSON => Display::fmt("JSON", f),
        }
    }
}

impl FromStr for LogMode {
    type Err = LogModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "HUMAN" => Ok(LogMode::HUMAN),
            "JSON" => Ok(LogMode::JSON),
            _ => Err(LogModeError(s.to_owned())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unsupported log mode level `{0}`. Supported values are `HUMAN` and `JSON`.")]
pub struct LogModeError(String);

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

#[derive(Debug)]
pub struct LogLevelError {
    pub given_log_level: String,
}

impl Display for LogLevelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Log level '{}' is invalid. Accepted values are 'OFF', 'ERROR', 'WARN', 'INFO', 'DEBUG', and 'TRACE'.",
            self.given_log_level
        )
    }
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Off => Display::fmt("OFF", f),
            LogLevel::Error => Display::fmt("ERROR", f),
            LogLevel::Warn => Display::fmt("WARN", f),
            LogLevel::Info => Display::fmt("INFO", f),
            LogLevel::Debug => Display::fmt("DEBUG", f),
            LogLevel::Trace => Display::fmt("TRACE", f),
        }
    }
}

impl std::error::Error for LogLevelError {}

impl FromStr for LogLevel {
    type Err = LogLevelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "off" => Ok(LogLevel::Off),
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Err(LogLevelError {
                given_log_level: s.to_owned(),
            }),
        }
    }
}

// Fixed width module path formatter (with color support)
const TARGET_WIDTH: usize = 35;

// ANSI color codes
mod ansi {
    pub const RESET: &str = "\x1b[0m";

    // Module path color (blue/light)
    pub const TARGET: &str = "\x1b[94m"; // Bright blue

    // Log level color
    pub const ERROR: &str = "\x1b[31m"; // Red
    pub const WARN: &str = "\x1b[33m"; // Yellow
    pub const INFO: &str = "\x1b[32m"; // Green
    pub const DEBUG: &str = "\x1b[36m"; // Cyan
    pub const TRACE: &str = "\x1b[90m"; // Gray
}

/// Simplify the module path by removing the crate name (the part before the first `::`)
///
/// # Examples
/// - `sigbot_cmd::cmd::internal::management_server` -> `cmd::internal::management_server`
/// - `sigbot_order::server::order_server` -> `server::order_server`
/// - `datafeed::client::market::datafeed_binance` -> `client::market::datafeed_binance`
fn simplify_target(target: &str) -> String {
    // Remove the crate name (the part before the first `::`)
    if let Some(pos) = target.find("::") {
        let after_crate = &target[pos + 2..];
        if !after_crate.is_empty() {
            return after_crate.to_string();
        }
    }

    // If it doesn't contain `::`, return the original string
    target.to_string()
}

struct FixedWidthTargetFormatter {
    ansi_enabled: bool,
}

impl FixedWidthTargetFormatter {
    fn new(ansi_enabled: bool) -> Self {
        Self { ansi_enabled }
    }

    fn level_color(&self, level: &tracing::Level) -> &'static str {
        if !self.ansi_enabled {
            return "";
        }
        match *level {
            tracing::Level::ERROR => ansi::ERROR,
            tracing::Level::WARN => ansi::WARN,
            tracing::Level::INFO => ansi::INFO,
            tracing::Level::DEBUG => ansi::DEBUG,
            tracing::Level::TRACE => ansi::TRACE,
        }
    }

    fn reset_color(&self) -> &'static str {
        if self.ansi_enabled {
            ansi::RESET
        } else {
            ""
        }
    }
}

impl<S, N> FormatEvent<S, N> for FixedWidthTargetFormatter
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> FormatFieldsTrait<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let time_format = Iso8601TimeFormatter::new(self.ansi_enabled);
        let level = *meta.level();

        // Format the timestamp (ISO 8601 format) with color
        time_format.format_time(&mut writer)?;
        write!(writer, " ")?;

        // Format the log level with color
        let color = self.level_color(&level);
        let reset = self.reset_color();
        write!(writer, "{color}{:5}{reset} ", level)?;

        // Simplify the module path (remove the same prefix)
        let target = simplify_target(meta.target());

        // Format the fixed width target (module path), add color and separator
        let target_color = if self.ansi_enabled { ansi::TARGET } else { "" };
        let reset = self.reset_color();

        if target.len() > TARGET_WIDTH {
            // If it exceeds the fixed width, truncate and add ellipsis
            let truncated = &target[..TARGET_WIDTH.saturating_sub(3)];
            write!(writer, "{target_color}{truncated}...{reset}: ")?;
        } else {
            // If it is less than the fixed width, right-align fill spaces, then add separator
            write!(
                writer,
                "{target_color}{:>width$}{reset}: ",
                target,
                width = TARGET_WIDTH
            )?;
        }

        // Format the fields
        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)?;

        Ok(())
    }
}

pub(super) fn default_log_route_layer() -> LogRouteType {
    None.with_filter(tracing_subscriber::filter::Targets::new().with_target("", LevelFilter::OFF))
}

pub(super) fn default_log_stderr_layer(config: &Arc<AppConfig>) -> LogStderrType {
    // Check if ANSI color is supported (check if stderr is a TTY)
    // Use the standard library's IsTerminal trait, avoid using the unsafe atty crate
    let ansi_enabled = std::io::stderr().is_terminal();

    let layer = match config.logging.mode {
        LogMode::HUMAN => {
            // Use a custom formatter to format the module path to a fixed width, and add color support
            let formatter = FixedWidthTargetFormatter::new(ansi_enabled);
            let layer = tracing_subscriber::fmt::layer()
                .with_writer(|| LineWriter::new(std::io::stderr()))
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_ansi(ansi_enabled)
                .event_format(formatter);
            Box::new(layer) as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>
        }
        LogMode::JSON => {
            let layer = tracing_subscriber::fmt::layer()
                .with_writer(|| LineWriter::new(std::io::stderr()))
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_ansi(ansi_enabled)
                .json();
            Box::new(layer) as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>
        }
    };

    layer.with_filter(
        tracing_subscriber::filter::Targets::new()
            .with_target("", LevelFilter::from_str(&config.logging.level.to_string()).unwrap()),
    )
}

pub(super) fn default_log_levels_layer() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "debug".into())
        // .add_directive("debug".parse().unwrap()) // default level.
        .add_directive("sigbot=debug".parse().unwrap())
        .add_directive("hyper=warn".parse().unwrap())
        .add_directive("tokio=trace".parse().unwrap()) // Notice: Must be at trace level to collect
}
