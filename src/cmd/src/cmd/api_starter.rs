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

use crate::cmd::internal::banner::print_banner;
use crate::cmd::internal::management_server::SigbotManagementServer;
use axum::{
    body::Body,
    extract::{Request, State},
    middleware::Next,
    response::Response,
    Router,
};
use clap::Command;
use common_telemetry::{error, info};
use sigbot_core::{
    config::{config::get_config, swagger},
    context::state::SigbotState,
    mgmt::{apm, health::init as health_router},
    sys::route::{
        auth_router::{auth_middleware, init as auth_router},
        log_router::init as log_router,
        tenant_router::init as tenant_router,
        user_router::init as user_router,
    },
};
use sigbot_utils::{panics::PanicHelper, tokio_signal::tokio_graceful_shutdown_handler};
use std::{future::Future, pin::Pin};
use tokio::{net::TcpListener, sync::oneshot};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
pub struct SigbotAPIServer {}

pub type MiddlewareFunction =
    fn(State<SigbotState>, Request<Body>, Next) -> Pin<Box<dyn Future<Output = Response<Body>> + Send + 'static>>;

impl SigbotAPIServer {
    pub const COMMAND_NAME: &'static str = "api";

    // http://www.network-science.de/ascii/#larry3d,graffiti,doom,basic,drpepper,rounded,roman
    pub const ASCII_NAME: &'static str = r#"
 ______                  ____                                           
/\  _  \          __    /\  _`\                                         
\ \ \L\ \  _____ /\_\   \ \,\L\_\     __   _ __   __  __     __   _ __  
 \ \  __ \/\ '__`\/\ \   \/_\__ \   /'__`\/\`'__\/\ \/\ \  /'__`\/\`'__\
  \ \ \/\ \ \ \L\ \ \ \    /\ \L\ \/\  __/\ \ \/ \ \ \_/ |/\  __/\ \ \/ 
   \ \_\ \_\ \ ,__/\ \_\   \ `\____\ \____\\ \_\  \ \___/ \ \____\\ \_\ 
    \/_/\/_/\ \ \/  \/_/    \/_____/\/____/ \/_/   \/__/   \/____/ \/_/ 
             \ \_\                                                      
              \/_/                                     (Sigbot API Server)
 "#;

    pub fn build() -> Command {
        Command::new(Self::COMMAND_NAME).about("Run Sigbot platformization API Server.")
    }

    #[tokio::main]
    pub async fn run(matches: &clap::ArgMatches, verbose: bool) -> () {
        PanicHelper::set_hook_default(get_config().logging.is_human_mode());

        print_banner(verbose, Self::ASCII_NAME, None);

        apm::init().await;

        let (signal_s, signal_r) = oneshot::channel();
        let signal_handle = SigbotManagementServer::start(verbose, signal_s).await;

        signal_r.await.expect("Failed to start Management server.");
        info!("Management server is started");

        Self::startup(matches, verbose, None, None).await;

        signal_handle.await.expect("Failed to start Management server.");
    }

    #[allow(unused_variables)]
    pub async fn startup(
        matches: &clap::ArgMatches,
        verbose: bool,
        addition_router: Option<Router<SigbotState>>,
        addition_middleware: Option<MiddlewareFunction>,
    ) {
        let config = get_config();
        let app_state = SigbotState::new(&config).await;

        // 1. Merge the biz modules routes.
        info!("Register Web server app routers ...");
        let mut register_router = Router::new()
            .merge(auth_router())
            .merge(user_router())
            .merge(tenant_router())
            .merge(log_router());

        // 1.1 Merge the addition router.
        register_router = if let Some(addition_router) = addition_router {
            register_router.merge(addition_router)
        } else {
            register_router
        };

        // 2. Merge of all routes.
        let mut app_router = match &config.server.context_path {
            // If the context path is "/" then should not be use nest on axum-0.8+
            Some(ctx_path) if ctx_path == "/" => Router::new()
                .merge(health_router())
                .merge(register_router)
                .with_state(app_state.clone()),
            // If the context path is not "/" then should be use nest on axum-0.8+
            Some(ctx_path) => {
                let prefixed_router = Router::new().nest(&ctx_path, register_router);
                Router::new()
                    .merge(health_router())
                    .merge(prefixed_router) // support the context-path.
                    .with_state(app_state.clone()) // TODO: remove clone
            }
            None => {
                Router::new()
                    .merge(health_router())
                    .merge(register_router)
                    .with_state(app_state.clone()) // TODO: remove clone
            }
        };

        // 3. Merge the swagger router.
        if config.swagger.enabled {
            info!("Registering Web server swagger middlewares ...");
            app_router = app_router.merge(swagger::init(&config));
        }

        // 4. Finally add the (auth) middlewares.
        // Notice: The settings of middlewares are in order, which will affect the priority of route matching.
        // The later the higher the priority? For example, if auth_middleware is set at the end, it will
        // enter when requesting '/', otherwise it will not enter if it is set at the front, and will
        // directly enter handle_root().
        info!("Registering API Server auth middlewares ...");
        app_router = app_router.layer(
            ServiceBuilder::new()
                .layer(axum::middleware::from_fn_with_state(
                    app_state.to_owned(),
                    auth_middleware,
                ))
                // Optional: add logs to tracing.
                .layer(
                    TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                        tracing::info_span!(
                            "http_request",
                            method = %request.method(),
                            uri = %request.uri(),
                        )
                    }),
                ),
        );
        if addition_middleware.is_some() {
            let layer = axum::middleware::from_fn_with_state(app_state.to_owned(), addition_middleware.unwrap());
            app_router = app_router.layer(layer);
        }
        //.route_layer(axum::Extension(app_state));

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

        match axum::serve(listener, app_router.into_make_service())
            .with_graceful_shutdown(tokio_graceful_shutdown_handler())
            // .tcp_nodelay(true)
            .await
        {
            Ok(_) => {
                info!("API Server shut down gracefully");
            }
            Err(e) => {
                error!("Error running API Server: {}", e);
                panic!("Error starting API Server: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_no_args() {
        let app = SigbotAPIServer::build();
        let matches = app.try_get_matches_from(vec![""]).unwrap();
        assert!(matches.subcommand_name().is_none());
    }

    #[test]
    fn test_cli_start_command() {
        let app = SigbotAPIServer::build();
        let matches = app.try_get_matches_from(vec!["", "start"]).unwrap();
        assert_eq!(matches.subcommand_name(), Some("start"));
    }

    #[test]
    fn test_cli_start_with_config() {
        let app = SigbotAPIServer::build();
        let matches = app
            .try_get_matches_from(vec!["", "start", "--config", "config.yaml"])
            .unwrap();
        let start_matches = matches.subcommand_matches("start").unwrap();
        assert_eq!(start_matches.get_one::<String>("config").unwrap(), "config.yaml");
    }

    #[test]
    fn test_cli_invalid_command() {
        let app = SigbotAPIServer::build();
        let result = app.try_get_matches_from(vec!["", "invalid"]);
        assert!(result.is_err());
    }
}
