use crate::{
    context::{
        identity::{RequestIdentity, ScopeQuery},
        state::SigbotState,
    },
    modules::workflow::store::observation_postgres::WorkflowObservationRepository,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Extension, Json, Router,
};
use serde_json::{json, Value};

pub struct WorkflowObservationRouter;
impl WorkflowObservationRouter {
    pub fn routes() -> Router<SigbotState> {
        Router::new()
            .route("/api/v1/overview", get(Self::overview))
            .route("/api/v1/workflows/{id}/events", get(Self::events))
    }
    async fn overview(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(scope): Query<ScopeQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        let tenant = scope
            .authorize(&identity, "overview")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let pool = state
            .postgres_pool()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let mut result = WorkflowObservationRepository::new(pool)
            .overview(tenant, &scope.env, scope.space_id)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if let Some(runs) = result["recent_runs"].as_array_mut() {
            runs.retain(|run| {
                run["workflow_id"]
                    .as_str()
                    .and_then(|id| id.parse().ok())
                    .is_some_and(|id| identity.require_workflow(&scope.env, scope.space_id, Some(id)).is_ok())
            });
        }
        Ok(Json(result))
    }
    async fn events(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Path(id): Path<i64>,
        Query(scope): Query<ScopeQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        identity
            .require_workflow(&scope.env, scope.space_id, Some(id))
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let tenant = identity.tenant_id().parse().map_err(|_| StatusCode::FORBIDDEN)?;
        let pool = state
            .postgres_pool()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let data = WorkflowObservationRepository::new(pool)
            .events(tenant, &scope.env, scope.space_id, id)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(json!({"data":data})))
    }
}
