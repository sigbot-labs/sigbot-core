use crate::{
    context::{
        identity::{RequestIdentity, ScopeQuery},
        state::SigbotState,
    },
    modules::market::store::postgres::MarketPostgresRepository,
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
pub struct MarketQuery {
    pub env: String,
    pub space_id: i64,
    pub source: String,
    pub category: String,
}
pub struct MarketRouter;
impl MarketRouter {
    pub fn routes() -> Router<SigbotState> {
        Router::new().route("/api/v1/market", get(Self::recent))
    }
    async fn recent(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(query): Query<MarketQuery>,
    ) -> Result<Json<Value>, StatusCode> {
        let scope = ScopeQuery {
            env: query.env.clone(),
            space_id: query.space_id,
        };
        let tenant = scope
            .authorize(&identity, "market")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        if !matches!(
            query.category.as_str(),
            "all" | "news" | "liquidation" | "funding" | "positioning" | "trade" | "quote"
        ) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let pool = state
            .postgres_pool()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let data = MarketPostgresRepository::new(pool)
            .recent(tenant, &scope.env, scope.space_id, &query.source, &query.category)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(json!({"data":data})))
    }
}
