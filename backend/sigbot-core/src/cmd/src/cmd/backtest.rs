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

use crate::cmd::management::ManagementServer;
use axum::Router;
use clap::Command;
use sigbot_backtest::backtest_base::SigBotVerifierManager;
use sigbot_core::config::config::AppConfig;
use sigbot_core::context::state::SigbotState;
use sigbot_core::llm::handler::llm_base::LLMManager;
use sigbot_core::mgmt::health::init as health_router;
use sigbot_core::{
    config::config::{self, GIT_BUILD_DATE, GIT_COMMIT_HASH, GIT_VERSION},
    mgmt::apm,
};
use sigbot_utils::panics::PanicHelper;
use sigbot_utils::tokio_signal::tokio_graceful_shutdown_signal;
use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub struct SigBotVerifierServer {}

impl SigBotVerifierServer {
    pub const COMMAND_NAME: &'static str = "backtest";

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run SigBot AI generated ModSec rules Verifier.")
    }

    #[allow(unused)]
    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) -> () {
        PanicHelper::set_hook_default();

        let config = config::get_config();

        Self::print_banner(config.to_owned(), verbose);

        // Initial APM components.
        apm::init_components(&config).await;

        let (signal_s, signal_r) = oneshot::channel();
        let signal_handle = ManagementServer::start(&config, true, signal_s).await;

        signal_r.await.expect("Failed to start Management server.");
        tracing::info!("Management server is ready on {}", config.mgmt.get_bind_addr());

        Self::start(&config, true).await;

        signal_handle.await.unwrap();
    }

    #[allow(unused)]
    async fn start(config: &Arc<AppConfig>, verbose: bool) {
        LLMManager::init().await;
        SigBotVerifierManager::init().await;

        let app_state = SigbotState::new(&config).await;

        let bind_addr = config.server.get_bind_addr();
        tracing::info!("Starting SigBot Verifier server on {}", bind_addr);
        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(l) => {
                tracing::info!("SigBot Verifier server is ready on {}", bind_addr);
                l
            }
            Err(e) => {
                tracing::error!("Failed to bind to {}: {}", bind_addr, e);
                panic!("Failed to bind to {}: {}", bind_addr, e);
            }
        };

        let app_router = Router::new().merge(health_router()).with_state(app_state);
        match axum::serve(listener, app_router.into_make_service())
            .with_graceful_shutdown(tokio_graceful_shutdown_signal())
            // .tcp_nodelay(true)
            .await
        {
            Ok(_) => {
                tracing::info!("SigBot Verifier server shut down gracefully");
            }
            Err(e) => {
                tracing::error!("Error running web server: {}", e);
                panic!("Error start SigBot Verifier server: {}", e);
            }
        }
    }

    fn print_banner(config: Arc<AppConfig>, verbose: bool) {
        // http://www.network-science.de/ascii/#larry3d,graffiti,basic,drpepper,rounded,roman
        let ascii_name = r#"
 ____                     __          __                   __      
/\  _`\                  /\ \        /\ \__               /\ \__   
\ \ \L\ \     __      ___\ \ \/'\    \ \ ,_\    __    ____\ \ ,_\ 
 \ \  _ <'  /'__`\   /'___\ \ , <     \ \ \/  /'__`\ /',__\\ \ \/  
  \ \ \L\ \/\ \L\.\_/\ \__/\ \ \\`\    \ \ \_/\  __//\__, `\\ \ \_ 
   \ \____/\ \__/.\_\ \____\\ \_\ \_\   \ \__\ \____\/\____/ \ \__\
    \/___/  \/__/\/_/\/____/ \/_/\/_/    \/__/\/____/\/___/   \/__/

                                                   (Sigbot Backtest)
 "#;
        eprintln!("");
        eprintln!("{}", ascii_name);
        eprintln!("                Program Version: {:?}", GIT_VERSION);
        eprintln!(
            "                Package Version: {:?}",
            env!("CARGO_PKG_VERSION").to_string()
        );
        eprintln!("                Git Commit Hash: {:?}", GIT_COMMIT_HASH);
        eprintln!("                 Git Build Date: {:?}", GIT_BUILD_DATE);
        let path = env::var("SIGBOT_CFG_PATH").unwrap_or("none".to_string());
        eprintln!("        Configuration file path: {:?}", path);
        eprintln!(
            "            Web Serve listen on: \"{}://{}:{}\"",
            "http", &config.server.host, config.server.port
        );
        if config.mgmt.enabled {
            eprintln!(
                "     Management serve listen on: \"{}://{}:{}\"",
                "http", config.mgmt.host, config.mgmt.port
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
            let config_json = serde_json::to_string(&config.inner).unwrap_or_default();
            eprintln!("Configuration loaded: {}", config_json);
        }
        eprintln!("");
    }
}
