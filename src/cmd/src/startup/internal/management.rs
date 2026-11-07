// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use axum_prometheus::PrometheusMetricLayer;
use common_context::util::panics::PanicHelper;
use common_context::util::signal::ShutdownSignal;
use common_context::ApplicationContext;
use common_telemetry::{debug, info};
use prometheus::{Encoder, TextEncoder};
use sigbot_core::config::config::get_config;
use sigbot_core::context::state::SigbotState;
use sigbot_core::mgmt::apm;
use sigbot_core::mgmt::health::HealthRouter;
use tokio::task::JoinHandle;

#[derive(Default)]
pub struct SigbotManagementStarter;

impl SigbotManagementStarter {
    pub async fn start(&self) -> JoinHandle<()> {
        let config = get_config();
        PanicHelper::set_hook_default(config.logging.is_human_mode());
        ApplicationContext::global()
            .init()
            .await
            .expect("provider context initialization failed");
        apm::init().await;

        if !config.mgmt.enabled {
            return tokio::spawn(ShutdownSignal::wait());
        }
        let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
        let handler = move || {
            let metric_handle = metric_handle.clone();
            async move {
                let mut buffer = Vec::new();
                let custom = TextEncoder::new()
                    .encode(&prometheus::gather(), &mut buffer)
                    .ok()
                    .and_then(|_| String::from_utf8(buffer).ok())
                    .unwrap_or_default();
                let http = metric_handle.render();
                match (custom.trim(), http.trim()) {
                    ("", _) => http,
                    (_, "") => custom,
                    (custom, http) => format!("{custom}\n{http}"),
                }
            }
        };
        let app = Router::new()
            .route("/metrics", get(handler))
            .layer(prometheus_layer)
            .merge(HealthRouter::routes().with_state(SigbotState::new(&config).await));
        let bind_addr = config.mgmt.get_bind_addr();
        debug!("Starting Management server on {}", bind_addr);
        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .expect("management listener bind failed");
        info!("Management server is ready on {}", bind_addr);
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(ShutdownSignal::wait())
                .await
                .expect("management server failed");
        })
    }

    /// Close providers even when the management task fails or is cancelled.
    pub async fn wait(&self, handle: JoinHandle<()>) -> anyhow::Result<()> {
        let result = handle.await;
        let shutdown = ApplicationContext::global().shutdown().await;
        result.context("management task failed")?;
        shutdown.context("provider shutdown failed")
    }
}

#[cfg(test)]
#[path = "../../../tests/startup/internal/management_test.rs"]
mod tests;
