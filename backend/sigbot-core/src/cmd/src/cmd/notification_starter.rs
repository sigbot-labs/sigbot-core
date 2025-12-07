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

use crate::cmd::internal::management_server::SigbotManagementServer;
use axum::Router;
use clap::Command;
use common_telemetry::{error, info};
use sigbot_core::config::config::AppConfig;
use sigbot_core::config::config::{self, GIT_BUILD_DATE, GIT_COMMIT_HASH, GIT_VERSION};
use sigbot_core::context::state::SigbotState;
use sigbot_core::llm::handler::llm_engine::LLMEngine;
use sigbot_core::mgmt::{apm, health::init as health_router};
use sigbot_datafeed::server::datafeed_ingestor::SigbotDatafeedIngestor;
use sigbot_notification::server::notification_forwarder::SigbotNotificationForwarder;
use sigbot_utils::panics::PanicHelper;
use sigbot_utils::tokio_signal::tokio_graceful_shutdown_signal;
use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub struct SigbotNotificationForwarderStarter {}

impl SigbotNotificationForwarderStarter {
    pub const COMMAND_NAME: &'static str = "notification";

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot Tenant (Isolated) Notification Forwarder")
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
        let signal_handle = SigbotManagementServer::start(&config, true, signal_s).await;

        signal_r.await.expect("Failed to start Management server.");
        info!("Management server is ready on {}", config.mgmt.get_bind_addr());

        Self::start(&config, true).await;

        signal_handle.await.unwrap();
    }

    #[allow(unused)]
    async fn start(config: &Arc<AppConfig>, verbose: bool) {
        LLMEngine::init().await;
        SigbotNotificationForwarder::startup().await;

        let app_state = SigbotState::new(&config).await;

        let bind_addr = config.server.get_bind_addr();
        info!("Starting Sigbot Datafeed Ingestor on {}", bind_addr);
        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(l) => {
                info!("Sigbot Datafeed Ingestor is ready on {}", bind_addr);
                l
            }
            Err(e) => {
                error!("Failed to bind to {}: {}", bind_addr, e);
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
                info!("Sigbot Datafeed Ingestor shutdown gracefully");
            }
            Err(e) => {
                error!("Error running web server: {}", e);
                panic!("Error start Sigbot Datafeed Ingestor: {}", e);
            }
        }
    }

    fn print_banner(config: Arc<AppConfig>, verbose: bool) {
        // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
        let ascii_name = r#"
 ____              __             ____                  __     
/\  _`\           /\ \__         /\  _`\               /\ \    
\ \ \/\ \     __  \ \ ,_\    __  \ \ \L\_\ __     __   \_\ \   
 \ \ \ \ \  /'__`\ \ \ \/  /'__`\ \ \  _\/'__`\ /'__`\ /'_` \  
  \ \ \_\ \/\ \L\.\_\ \ \_/\ \L\.\_\ \ \/\  __//\  __//\ \L\ \ 
   \ \____/\ \__/.\_\\ \__\ \__/.\_\\ \_\ \____\ \____\ \___,_\
    \/___/  \/__/\/_/ \/__/\/__/\/_/ \/_/\/____/\/____/\/__,_ /
                                                                         
                                        (Sigbot Datafeed Runner)
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
            "            DataFeed Server listen on: \"{}://{}:{}\"",
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
