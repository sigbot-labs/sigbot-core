// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyright (c) 2027 James Wong. This file is part of James Wong.
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

use sigbot_core::config::config::get_config;
use sigbot_core::config::config::GIT_BUILD_DATE;
use sigbot_core::config::config::GIT_COMMIT_HASH;
use sigbot_core::config::config::GIT_VERSION;

/// Print the service banner with version information and configuration details.
///
/// # Arguments
///
/// * `verbose` - Whether to print verbose configuration information
/// * `ascii_name` - The ASCII art name of the service
/// * `server_description` - Optional custom server description. If None, uses default "Web Serve listen on"
pub fn print_banner(verbose: bool, ascii_name: &str, server_description: Option<&str>) {
    let config = get_config();
    eprintln!();
    eprintln!("{}", ascii_name);
    eprintln!("                Program Version: {:?}", GIT_VERSION);
    eprintln!(
        "                Package Version: {:?}",
        env!("CARGO_PKG_VERSION").to_string()
    );
    eprintln!("                Git Commit Hash: {:?}", GIT_COMMIT_HASH);
    eprintln!("                 Git Build Date: {:?}", GIT_BUILD_DATE);
    let server_desc = server_description.unwrap_or("Serve listen on");
    eprintln!(
        "            {}: \"http://{}:{}\"",
        server_desc, &config.server.host, config.server.port
    );
    if config.mgmt.enabled {
        eprintln!(
            "     Management serve listen on: \"http://{}:{}\"",
            config.mgmt.host, config.mgmt.port
        );
        if config.mgmt.tokio_console.enabled {
            #[cfg(feature = "profiling-tokio-console")]
            let server_addr = &config.mgmt.tokio_console.server_bind;
            #[cfg(feature = "profiling-tokio-console")]
            eprintln!("   TokioConsole serve listen on: \"{}://{}\"", "http", server_addr);
        }
        if config.mgmt.pyroscope.enabled {
            #[cfg(feature = "profiling-pyroscope")]
            let server_url = &config.mgmt.pyroscope.server_url;
            #[cfg(feature = "profiling-pyroscope")]
            eprintln!("     Pyroscope agent connect to: \"{}\"", server_url);
        }
        if config.mgmt.otel.enabled {
            let endpoint = &config.mgmt.otel.endpoint;
            eprintln!("          Otel agent connect to: \"{}\"", endpoint);
        }
    }
    if verbose {
        eprintln!(
            "Configuration loaded: service={}, storage={:?}, cache={:?}, logging={:?}",
            config.service_name, config.storage.provider, config.cache.provider, config.logging.mode
        );
    }
    eprintln!();
}
