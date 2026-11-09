// SPDX-License-Identifier: GPL-3.0-only
//! Bounded dependency and business lifecycle checks used by API and worker probes.
use crate::config::config::CacheProvider;
use crate::config::config::StorageProvider;
use crate::context::state::SigbotState;
use crate::storage::sqlite::SQLiteRepository;
use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Json;
use axum::Router;
use common_telemetry::readiness::RuntimeReadiness;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Serialize)]
pub struct HealthReport {
    pub status: &'static str,
    pub details: BTreeMap<String, &'static str>,
}
pub struct HealthRouter;

impl HealthRouter {
    pub fn routes() -> Router<SigbotState> {
        Router::new()
            .route("/_/healthz", get(Self::health))
            .route("/_/livez", get(Self::liveness))
    }
    pub fn response(healthy: bool, details: BTreeMap<String, &'static str>) -> (StatusCode, Json<HealthReport>) {
        (
            if healthy {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            Json(HealthReport {
                status: if healthy { "UP" } else { "DOWN" },
                details,
            }),
        )
    }
    async fn database(state: &SigbotState) -> Result<()> {
        match state.config.storage.provider {
            StorageProvider::Postgres => {
                let pool = state.postgres_pool().await?;
                sqlx::query("SELECT 1").execute(&pool).await?;
            }
            StorageProvider::SQLite => {
                let repository = SQLiteRepository::<()>::get_or_init(&state.config.storage.sqlite).await?;
                sqlx::query("SELECT 1").execute(repository.get_pool()).await?;
            }
        }
        Ok(())
    }
    async fn liveness() -> (StatusCode, Json<HealthReport>) {
        let (alive, tasks) = RuntimeReadiness::liveness_snapshot();
        let details = tasks
            .into_iter()
            .map(|(name, alive)| (name, if alive { "UP" } else { "DOWN" }))
            .collect();
        Self::response(alive, details)
    }
    async fn health(State(state): State<SigbotState>) -> (StatusCode, Json<HealthReport>) {
        let (business, components) = RuntimeReadiness::snapshot();
        let mut details = components
            .into_iter()
            .map(|(name, ready)| (name, if ready { "UP" } else { "DOWN" }))
            .collect::<BTreeMap<_, _>>();
        details.insert("runtime".into(), if business { "UP" } else { "DOWN" });
        let database = matches!(
            tokio::time::timeout(Duration::from_secs(2), Self::database(&state)).await,
            Ok(Ok(()))
        );
        details.insert("database".into(), if database { "UP" } else { "DOWN" });
        let cache = if matches!(state.config.cache.provider, CacheProvider::REDIS) {
            matches!(
                tokio::time::timeout(Duration::from_secs(2), state.string_cache.get("sigbot:health".into())).await,
                Ok(Ok(_))
            )
        } else {
            true
        };
        details.insert("cache".into(), if cache { "UP" } else { "DOWN" });
        Self::response(business && database && cache, details)
    }
}
