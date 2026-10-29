//! Authguard owns login, logout, users and permissions. Sigbot exposes its binding only.
use crate::{
    context::{identity::RequestIdentity, state::SigbotState},
    util::auths::RequestPaths,
};
use axum::{
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
    Extension, Json, Router,
};

pub struct AuthRouter;

impl AuthRouter {
    pub fn routes() -> Router<SigbotState> {
        Router::new().route("/api/v1/identity", get(Self::identity))
    }

    pub async fn authenticate(State(state): State<SigbotState>, mut request: Request<Body>, next: Next) -> Response {
        let path = RequestPaths::clean_context_path(&state.config.server.context_path, request.uri().path());
        // Only infrastructure probes are anonymous. Hosted Login is served by the Gateway.
        if matches!(path, "/_/healthz" | "/healthz" | "/readyz") {
            return next.run(request).await;
        }
        match state
            .authguard
            .authenticate(request.headers(), request.method(), path)
            .await
        {
            Ok(identity) => {
                request.extensions_mut().insert(identity);
                next.run(request).await
            }
            Err(_) => (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "authguard_context_required"})),
            )
                .into_response(),
        }
    }

    async fn identity(Extension(identity): Extension<RequestIdentity>) -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "principal_id": identity.principal_id(),
            "tenant_id": identity.tenant_id(),
            "action": identity.access.action,
        }))
    }
}
