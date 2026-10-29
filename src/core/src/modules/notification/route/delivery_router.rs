use crate::{
    context::{
        identity::{RequestIdentity, ScopeQuery},
        state::SigbotState,
    },
    modules::notification::store::delivery_postgres::NotificationDeliveryRepository,
};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationRead {
    pub run_id: String,
    pub seq: i64,
    pub producer_node_id: i64,
}
pub struct NotificationDeliveryRouter;
impl NotificationDeliveryRouter {
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
        let tenant = scope
            .authorize(&identity, "notification")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        let pool = state
            .postgres_pool()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let data = NotificationDeliveryRepository::new(pool)
            .list(tenant, &scope.env, scope.space_id, identity.principal_id())
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(json!({"data":data})))
    }
    async fn mark_read(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Query(scope): Query<ScopeQuery>,
        Json(request): Json<NotificationRead>,
    ) -> Result<Json<Value>, StatusCode> {
        let tenant = scope
            .authorize(&identity, "notification")
            .map_err(|_| StatusCode::FORBIDDEN)?;
        if request.seq < 0 || request.run_id.is_empty() || request.producer_node_id <= 0 {
            return Err(StatusCode::BAD_REQUEST);
        }
        let pool = state
            .postgres_pool()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        NotificationDeliveryRepository::new(pool)
            .mark_read(
                tenant,
                &scope.env,
                scope.space_id,
                identity.principal_id(),
                &request.run_id,
                request.seq,
                request.producer_node_id,
            )
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(json!({"read":true})))
    }
}
