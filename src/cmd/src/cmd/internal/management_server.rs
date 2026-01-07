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

use crate::apm;
use axum::{routing::get, Router};
use axum_prometheus::PrometheusMetricLayer;
use common_telemetry::debug;
use prometheus::{Encoder, TextEncoder};
use sigbot_core::config::config::get_config;
use sigbot_utils::tokio_signal::tokio_graceful_shutdown_handler;
use tokio::{sync::oneshot, task::JoinHandle};

pub struct SigbotManagementServer {}

impl SigbotManagementServer {
    #[allow(unused)]
    pub async fn start(verbose: bool, signal_s: oneshot::Sender<()>) -> JoinHandle<()> {
        let config = get_config();

        let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();

        // Handler that collects metrics from both systems:
        // 1. prometheus::gather() - collects custom metrics from prometheus crate's default registry
        // 2. metric_handle.render() - collects HTTP request metrics from axum-prometheus
        let metric_handle_clone = metric_handle.clone();
        let handler = move || {
            let handle = metric_handle_clone.clone();
            async move {
                // Collect custom metrics from prometheus crate
                let mut buffer = Vec::new();
                let encoder = TextEncoder::new();
                let prometheus_metrics = match encoder.encode(&prometheus::gather(), &mut buffer) {
                    Ok(_) => String::from_utf8(buffer).unwrap_or_default(),
                    Err(_) => String::new(),
                };

                // Collect HTTP metrics from axum-prometheus
                let axum_metrics = handle.render();

                // Merge both metrics outputs
                if prometheus_metrics.trim().is_empty() {
                    axum_metrics
                } else if axum_metrics.trim().is_empty() {
                    prometheus_metrics
                } else {
                    // Combine both, prometheus metrics first, then axum metrics
                    format!("{}\n{}", prometheus_metrics.trim(), axum_metrics.trim())
                }
            }
        };

        let app: Router = Router::new()
            .route("/metrics", get(handler))
            .layer(prometheus_layer)
            .merge(apm::debug_router());

        let bind_addr = config.mgmt.get_bind_addr();
        debug!("Starting Management server on {}", bind_addr);

        tokio::spawn(async move {
            // When started call to signal sender.
            let _ = signal_s.send(());
            axum::serve(
                tokio::net::TcpListener::bind(&bind_addr).await.unwrap(),
                app.into_make_service(),
            )
            .with_graceful_shutdown(tokio_graceful_shutdown_handler())
            .await
            .unwrap_or_else(|e| panic!("Error starting Management server: {}", e));
        })
    }
}
