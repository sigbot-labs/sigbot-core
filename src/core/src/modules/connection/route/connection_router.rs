use crate::{
    context::{
        identity::{RequestIdentity, ScopeQuery},
        state::SigbotState,
    },
    modules::connection::handler::connection_handler::{ConnectionHandler, SaveConnection},
};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
pub struct ConnectionQuery {
    pub env: String,
    pub space_id: i64,
    pub kind: String,
}
pub struct ConnectionRouter;
impl ConnectionRouter {
    pub fn routes() -> Router<SigbotState> {
        Router::new().route("/api/v1/connections", get(Self::list).post(Self::save))
    }
    async fn list(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(query): Query<ConnectionQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        let scope = ScopeQuery {
            env: query.env.clone(),
            space_id: query.space_id,
        };
        scope
            .authorize(&identity, "connection")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let data = ConnectionHandler::list(&state, &identity, &scope, &query.kind)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(json!({"data":data})))
    }
    async fn save(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Json(request): Json<SaveConnection>,
    ) -> Result<Json<Value>, StatusCode> {
        identity
            .require_resource(&request.env, request.space_id, "connection", "all")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let id = ConnectionHandler::save(&state, &identity, request)
            .await
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        Ok(Json(json!({"id":id})))
    }
}
