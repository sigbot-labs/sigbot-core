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
use sigbot_core::modules::datafeed::handler::market::MarketHandler;

#[derive(Deserialize)]
pub struct MarketQuery {
    pub env: String,
    pub source: String,
    pub category: String,
}
pub struct MarketHttpApi;
impl MarketHttpApi {
    pub fn routes() -> Router<SigbotState> {
        Router::new().route("/api/v1/market", get(Self::recent))
    }
    async fn recent(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(query): Query<MarketQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        let scope = ScopeQuery { env: query.env.clone() };
        scope
            .authorize(&identity, "market")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let data = MarketHandler::recent(&state, &identity, &scope, &query.source, &query.category)
            .await
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        Ok(Json(json!({"data":data})))
    }
}
