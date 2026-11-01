use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use sigbot_core::context::identity::{RequestIdentity, ScopeQuery};
use sigbot_core::context::state::SigbotState;
use sigbot_core::modules::controller::handler::worker::WorkerHandler;
use sigbot_types::controller::{SaveWorker, WorkerStatus};
use sigbot_types::Uuid;

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerStatusRequest {
    pub status: WorkerStatus,
}

pub struct WorkerHttpApi;

impl WorkerHttpApi {
    pub fn routes() -> Router<SigbotState> {
        Router::new()
            .route("/api/v1/workers", get(Self::list).post(Self::save))
            .route("/api/v1/workers/defaults", get(Self::defaults))
            .route("/api/v1/workers/{id}", axum::routing::delete(Self::delete))
            .route("/api/v1/workers/{id}/status", axum::routing::post(Self::status))
    }

    async fn defaults(State(state): State<SigbotState>) -> Json<Value> {
        Json(json!(state.config.services.controller.middleware))
    }

    async fn list(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(scope): Query<ScopeQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        let data = WorkerHandler::list(&state, &identity, &scope)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(json!({"data": data})))
    }

    async fn save(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Json(request): Json<SaveWorker>,
    ) -> Result<Json<Value>, StatusCode> {
        let id = WorkerHandler::save(&state, &identity, request)
            .await
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        Ok(Json(json!({"id": id})))
    }

    async fn delete(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(scope): Query<ScopeQuery>,
        Path(id): Path<Uuid>,
    ) -> Result<Json<Value>, StatusCode> {
        let deleted = WorkerHandler::delete(&state, &identity, &scope, id)
            .await
            .map_err(|_| StatusCode::CONFLICT)?;
        Ok(Json(json!({"deleted": deleted})))
    }

    async fn status(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(scope): Query<ScopeQuery>,
        Path(id): Path<Uuid>,
        Json(request): Json<WorkerStatusRequest>,
    ) -> Result<Json<Value>, StatusCode> {
        let updated = WorkerHandler::status(&state, &identity, &scope, id, request.status)
            .await
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        Ok(Json(json!({"updated": updated})))
    }
}
