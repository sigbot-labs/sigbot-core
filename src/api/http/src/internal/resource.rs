// SPDX-License-Identifier: GPL-3.0-only
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sigbot_core::context::{identity::RequestIdentity, state::SigbotState};
use sigbot_core::modules::domain::handler::resource::ResourceHandler;
use sigbot_types::domain::{Domain, ResourceCommand, ResourceOperation};
use sigbot_types::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceQuery {
    env: String,
    #[serde(default = "ResourceQuery::default_limit")]
    limit: u32,
    #[serde(default)]
    offset: u32,
}
impl ResourceQuery {
    fn default_limit() -> u32 {
        50
    }
}

pub struct ResourceHttpApi;
impl ResourceHttpApi {
    pub fn routes(domain: Domain) -> Router<SigbotState> {
        let collection = format!("/api/v1/{}", domain.collection());
        Router::new()
            .route(&collection, axum::routing::get(Self::list).post(Self::create))
            .route(
                &format!("{collection}/{{id}}"),
                axum::routing::get(Self::get).put(Self::update).delete(Self::delete),
            )
            .layer(Extension(domain))
    }
    async fn invoke(
        state: SigbotState,
        identity: RequestIdentity,
        domain: Domain,
        query: ResourceQuery,
        id: Option<Uuid>,
        operation: ResourceOperation,
        fields: Map<String, Value>,
    ) -> Response {
        let target = id.map(|id| id.to_string()).unwrap_or_else(|| "all".into());
        if identity.require_resource(&query.env, domain.as_str(), &target).is_err() {
            return StatusCode::FORBIDDEN.into_response();
        }
        let command = ResourceCommand {
            domain,
            operation,
            env: query.env,
            id,
            fields,
            limit: query.limit,
            offset: query.offset,
        };
        match ResourceHandler::execute(&state, &identity, command).await {
            Ok(value) if operation == ResourceOperation::Get && value["data"].is_null() => {
                StatusCode::NOT_FOUND.into_response()
            }
            Ok(value) => (
                if operation == ResourceOperation::Create {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                Json(value),
            )
                .into_response(),
            Err(error) => {
                tracing::warn!(domain=domain.as_str(),?operation,%error,"domain operation failed");
                let status = if error.downcast_ref::<sqlx::Error>().is_some() {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::BAD_REQUEST
                };
                (status, Json(json!({"error":"domain_operation_rejected"}))).into_response()
            }
        }
    }
    async fn list(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Extension(domain): Extension<Domain>,
        Query(query): Query<ResourceQuery>,
    ) -> Response {
        Self::invoke(
            state,
            identity,
            domain,
            query,
            None,
            ResourceOperation::List,
            Map::new(),
        )
        .await
    }
    async fn get(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Extension(domain): Extension<Domain>,
        Query(query): Query<ResourceQuery>,
        Path(id): Path<Uuid>,
    ) -> Response {
        Self::invoke(
            state,
            identity,
            domain,
            query,
            Some(id),
            ResourceOperation::Get,
            Map::new(),
        )
        .await
    }
    async fn create(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Extension(domain): Extension<Domain>,
        Query(query): Query<ResourceQuery>,
        Json(fields): Json<Map<String, Value>>,
    ) -> Response {
        Self::invoke(state, identity, domain, query, None, ResourceOperation::Create, fields).await
    }
    async fn update(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Extension(domain): Extension<Domain>,
        Query(query): Query<ResourceQuery>,
        Path(id): Path<Uuid>,
        Json(fields): Json<Map<String, Value>>,
    ) -> Response {
        Self::invoke(
            state,
            identity,
            domain,
            query,
            Some(id),
            ResourceOperation::Update,
            fields,
        )
        .await
    }
    async fn delete(
        State(state): State<SigbotState>,
        Extension(identity): Extension<RequestIdentity>,
        Extension(domain): Extension<Domain>,
        Query(query): Query<ResourceQuery>,
        Path(id): Path<Uuid>,
    ) -> Response {
        Self::invoke(
            state,
            identity,
            domain,
            query,
            Some(id),
            ResourceOperation::Delete,
            Map::new(),
        )
        .await
    }
}
