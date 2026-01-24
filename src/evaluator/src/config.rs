// SPDX-LICENSE-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
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

/// Evaluator module configuration
///
/// Configuration has been moved to src/core/src/config/config.rs (EvaluatorProperties)
/// Access via get_tenant_config().services.evaluator
///
/// Example:
/// ```rust
/// use sigbot_core::config::config_tenant::get_tenant_config;
/// let config = get_tenant_config();
/// let cron = &config.services.evaluator.inner.cron;
/// let data_window = config.services.evaluator.data_window_hours;
/// ```

#[cfg(test)]
mod tests {}
