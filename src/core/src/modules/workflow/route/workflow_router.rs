//! Public workflow API backed by an explicitly scoped business handler.
use crate::{
    context::{identity::RequestIdentity, state::SigbotState},
    modules::workflow::handler::workflow_handler::{IWorkflowInfoHandler, WorkflowInfoHandler},
    util::web::ValidatedJson,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use sigbot_types::{
    modules::workflow::workflow::{
        DeleteWorkflowRequest, JobStatus, QueryWorkflowRequest, QueryWorkflowResponse, SaveWorkflowRequest,
    },
    PageRequest,
};
use std::sync::Arc;

pub struct WorkflowRouter;

impl WorkflowRouter {
    pub fn routes() -> Router<SigbotState> {
        Router::new()
            .route("/api/v1/workflows", get(Self::list).post(Self::save))
            .route("/api/v1/workflows/{id}", get(Self::get).delete(Self::delete))
            .route("/api/v1/workflows/{id}/start", post(Self::start))
            .route("/api/v1/workflows/{id}/stop", post(Self::stop))
    }

    fn handler(state: SigbotState, identity: &RequestIdentity) -> Result<WorkflowInfoHandler, StatusCode> {
        WorkflowInfoHandler::for_identity(Arc::new(state), identity).map_err(|_| StatusCode::FORBIDDEN)
    }

    async fn list(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(query): Query<QueryWorkflowRequest>,
        Query(page): Query<PageRequest>,
    ) -> Response {
        let Ok(handler) = Self::handler(state, &identity) else {
            return StatusCode::FORBIDDEN.into_response();
        };
        match handler.find(query, page).await {
            Ok((page, mut workflows)) => {
                for workflow in &mut workflows {
                    workflow.secrets = None;
                }
                Json(QueryWorkflowResponse::new(page, workflows)).into_response()
            }
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }

    async fn get(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Path(id): Path<i64>,
    ) -> Response {
        let Ok(handler) = Self::handler(state, &identity) else {
            return StatusCode::FORBIDDEN.into_response();
        };
        match handler.get(Some(id)).await {
            Ok(Some(workflow)) => {
                let mut workflow = (*workflow).clone();
                workflow.secrets = None;
                Json(workflow).into_response()
            }
            Ok(None) => StatusCode::NOT_FOUND.into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }

    async fn save(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        ValidatedJson(request): ValidatedJson<SaveWorkflowRequest>,
    ) -> Response {
        let Ok(handler) = Self::handler(state, &identity) else {
            return StatusCode::FORBIDDEN.into_response();
        };
        match handler.save(request).await {
            Ok(id) => Json(serde_json::json!({"id": id})).into_response(),
            Err(_) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid_workflow_or_scope"})),
            )
                .into_response(),
        }
    }

    async fn delete(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Path(id): Path<i64>,
    ) -> Response {
        let Ok(handler) = Self::handler(state, &identity) else {
            return StatusCode::FORBIDDEN.into_response();
        };
        match handler.delete(DeleteWorkflowRequest { id }).await {
            Ok(count) => Json(serde_json::json!({"count": count})).into_response(),
            Err(_) => StatusCode::CONFLICT.into_response(),
        }
    }

    async fn start(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Path(id): Path<i64>,
    ) -> Response {
        Self::transition(state, identity, id, JobStatus::PENDING).await
    }

    async fn stop(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Path(id): Path<i64>,
    ) -> Response {
        Self::transition(state, identity, id, JobStatus::STOPPING).await
    }

    async fn transition(state: SigbotState, identity: RequestIdentity, id: i64, status: JobStatus) -> Response {
        let Ok(handler) = Self::handler(state.clone(), &identity) else {
            return StatusCode::FORBIDDEN.into_response();
        };
        let Ok(Some(workflow)) = handler.get(Some(id)).await else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if workflow.env.as_deref() == Some("legacy") || workflow.space_id.is_none() {
            return StatusCode::CONFLICT.into_response();
        }
        let Ok(pool) = state.postgres_pool().await else {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        let result = super::super::store::run_postgres::WorkflowRunRepository::new(pool)
            .request(
                workflow.tenant_id.expect("scoped workflow"),
                workflow.env.as_deref().expect("scoped workflow"),
                workflow.space_id.expect("scoped workflow"),
                id,
                identity.principal_id(),
                status == JobStatus::PENDING,
            )
            .await;
        match result {
            Ok(run_id) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({"id": id, "run_id":run_id, "accepted":true})),
            )
                .into_response(),
            Err(_) => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error":"workflow_transition_rejected"})),
            )
                .into_response(),
        }
    }
}
