// SPDX-License-Identifier: GPL-3.0-only

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use sigbot_core::context::identity::{RequestIdentity, ScopeQuery};
use sigbot_core::context::state::SigbotState;
use sigbot_core::modules::notifier::handler::notifier::NotifierHandler;
use sigbot_types::Uuid;

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NotificationRead {
    pub run_id: Uuid,
    pub seq: i64,
    pub producer_node_id: i64,
}

pub struct NotifierHttpApi;

impl NotifierHttpApi {
    pub fn routes() -> Router<SigbotState> {
        Router::new()
            .route("/api/v1/notifications", get(Self::list))
            .route("/api/v1/notifications/read", post(Self::mark_read))
    }

    async fn list(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(scope): Query<ScopeQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        let data = NotifierHandler::list(&state, &identity, &scope)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(json!({"data": data})))
    }

    async fn mark_read(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(scope): Query<ScopeQuery>,
        Json(request): Json<NotificationRead>,
    ) -> Result<Json<Value>, StatusCode> {
        NotifierHandler::mark_read(
            &state,
            &identity,
            &scope,
            &request.run_id,
            request.seq,
            request.producer_node_id,
        )
        .await
        .map_err(|error| {
            if error.to_string().contains("invalid notification identity") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        })?;
        Ok(Json(json!({"read": true})))
    }
}
