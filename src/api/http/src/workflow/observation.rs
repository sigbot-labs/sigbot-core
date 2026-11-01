use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Extension;
use axum::Json;
use axum::Router;
use serde_json::Value;
use sigbot_core::context::identity::RequestIdentity;
use sigbot_core::context::identity::ScopeQuery;
use sigbot_core::context::state::SigbotState;
use sigbot_core::modules::workflow::handler::workflow::IWorkflowHandler;
use sigbot_core::modules::workflow::handler::workflow::WorkflowHandler;
use sigbot_types::Uuid;
use std::sync::Arc;

pub struct WorkflowObservationHttpApi;
impl WorkflowObservationHttpApi {
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
        scope
            .authorize(&identity, "overview")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let handler = WorkflowHandler::for_identity(Arc::new(state), &identity).map_err(|_| StatusCode::FORBIDDEN)?;
        let result = handler
            .overview(&scope.env)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(result))
    }
    async fn events(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Path(id): Path<Uuid>,
        Query(scope): Query<ScopeQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        identity
            .require_workflow(&scope.env, Some(id))
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let handler = WorkflowHandler::for_identity(Arc::new(state), &identity).map_err(|_| StatusCode::FORBIDDEN)?;
        let data = handler
            .events(&scope.env, id)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(serde_json::json!({"data":data})))
    }
}
