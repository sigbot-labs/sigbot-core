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

use crate::startup::internal::banner::print_banner;
use crate::startup::internal::management::SigbotManagementStarter;
use axum::Router;
use clap::Command;
use common_context::util::signal::ShutdownSignal;
use common_context::ApplicationContext;
use common_telemetry::error;
use common_telemetry::info;
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use sigbot_http::http::AxumHttpApi;
use sigbot_http::http::{IHttpApi, MiddlewareFunction};
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Default)]
pub struct SigbotHttpStarter {
    management: SigbotManagementStarter,
}

impl SigbotHttpStarter {
    pub const COMMAND_NAME: &'static str = "api";

    // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
    // http://patorjk.com/software/taag/#p=display&f=Larry+3D&t=API+Server&x=none&v=4&h=4&w=80&we=false
    pub const ASCII_NAME: &'static str = r#"
 ____                __              __        ______  ____    ______     
/\  _`\   __        /\ \            /\ \__    /\  _  \/\  _`\ /\__  _\    
\ \,\L\_\/\_\     __\ \ \____    ___\ \ ,_\   \ \ \L\ \ \ \L\ \/_/\ \/    
 \/_\__ \\/\ \  /'_ `\ \ '__`\  / __`\ \ \/    \ \  __ \ \ ,__/  \ \ \    
   /\ \L\ \ \ \/\ \L\ \ \ \L\ \/\ \L\ \ \ \_    \ \ \/\ \ \ \/    \_\ \__ 
   \ `\____\ \_\ \____ \ \_,__/\ \____/\ \__\    \ \_\ \_\ \_\    /\_____\
    \/_____/\/_/\/___L\ \/___/  \/___/  \/__/     \/_/\/_/\/_/    \/_____/
                  /\____/                                                 
                  \_/__/                              (Sigbot API Server)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot Platformization HTTP API for Operations.")
    }

    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) {
        print_banner(verbose, Self::ASCII_NAME, None);
        let service = Self::default();
        let signal_handle = service.management.start().await;
        Self::startup(matches, verbose, None, None).await;
        service
            .management
            .wait(signal_handle)
            .await
            .expect("service lifecycle failed");
    }

    pub async fn startup(
        _matches: &clap::ArgMatches,
        _verbose: bool,
        addition_router: Option<Router<SigbotState>>,
        addition_middleware: Option<MiddlewareFunction>,
    ) {
        let config = get_config();
        let app_state = SigbotState::new(&config).await;
        assert!(
            app_state.authguard.is_configured(),
            "API requires a configured Authguard verifier"
        );
        let http_state = app_state.clone();
        let context = ApplicationContext::global();
        context
            .register_if_absent::<dyn IHttpApi, _>("http-main", move || {
                let state = http_state.clone();
                async move { Ok(Arc::new(AxumHttpApi::new(state)) as Arc<dyn IHttpApi>) }
            })
            .expect("HTTP API registration failed");
        let http = context
            .get::<dyn IHttpApi>("http-main")
            .await
            .expect("HTTP API initialization failed");
        let app_router = http.router(addition_router, addition_middleware);

        let bind_addr = config.server.get_bind_addr();
        info!("Starting API Server on {}", bind_addr);
        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(l) => {
                info!("API Server is ready on {}", bind_addr);
                l
            }
            Err(e) => {
                error!("Failed to bind to {}: {}", bind_addr, e);
                panic!("Failed to bind to {}: {}", bind_addr, e);
            }
        };

        common_telemetry::readiness::RuntimeReadiness::business("api", true);
        let readiness = common_telemetry::readiness::RuntimeReadiness::task("api.listener");
        readiness.set(true);
        match axum::serve(listener, app_router.into_make_service())
            .with_graceful_shutdown(ShutdownSignal::wait())
            // .tcp_nodelay(true)
            .await
        {
            Ok(_) => info!("API Server shut down gracefully"),
            Err(e) => {
                error!("Error running API Server: {}", e);
                panic!("Error starting API Server: {}", e);
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/startup/api/http_test.rs"]
mod tests;
