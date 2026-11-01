use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Extension;
use axum::Json;
use axum::Router;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use sigbot_core::context::identity::RequestIdentity;
use sigbot_core::context::identity::ScopeQuery;
use sigbot_core::context::state::SigbotState;
use sigbot_core::modules::controller::handler::connection::ConnectionHandler;
use sigbot_core::modules::controller::handler::connection::SaveConnection;

#[derive(Deserialize)]
pub struct ConnectionQuery {
    pub env: String,
    pub kind: String,
}
pub struct ConnectionHttpApi;
impl ConnectionHttpApi {
    pub fn routes() -> Router<SigbotState> {
        Router::new().route("/api/v1/connections", get(Self::list).post(Self::save))
    }
    async fn list(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(query): Query<ConnectionQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        let scope = ScopeQuery { env: query.env.clone() };
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
            .require_resource(&request.env, "connection", "all")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let id = ConnectionHandler::save(&state, &identity, request)
            .await
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        Ok(Json(json!({"id":id})))
    }
}
