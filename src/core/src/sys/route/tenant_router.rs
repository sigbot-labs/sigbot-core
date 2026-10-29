use crate::context::identity::RequestIdentity;
use axum::Extension;
// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::sys::handler::tenant_handler::TenantHandler;
use crate::util::web::ValidatedJson;
use crate::{context::state::SigbotState, sys::handler::tenant_handler::ITenantHandler};
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use sigbot_types::sys::tenant::{DeleteTenantRequest, QueryTenantRequest, SaveTenantRequest};
use sigbot_types::{
    sys::tenant::{DeleteTenantResponse, QueryTenantResponse, SaveTenantResponse},
    PageRequest,
};

pub fn init() -> Router<SigbotState> {
    Router::new()
        .route("/api/v1/tenants", get(handle_query_tenants).post(handle_save_tenant))
        .route("/api/v1/tenants/delete", post(handle_delete_tenant))
        .route("/sys/tenant/query", get(handle_query_tenants))
        .route("/sys/tenant/save", post(handle_save_tenant))
        .route("/sys/tenant/delete", post(handle_delete_tenant))
}

#[utoipa::path(
    get,
    path = "/sys/tenant/query",
    params(QueryTenantRequest, PageRequest),
    responses((status = 200, description = "Getting for all tenants.", body = QueryTenantResponse)),
    tag = "Tenant"
)]
async fn handle_query_tenants(
    State(state): State<SigbotState>,
    Extension(identity): Extension<RequestIdentity>,
    Query(param): Query<QueryTenantRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_tenant_handler(&state, &identity).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryTenantResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/sys/tenant/save",
    request_body = SaveTenantRequest,
    responses((status = 200, description = "Save for tenant.", body = SaveTenantResponse)),
    tag = "Tenant"
)]
async fn handle_save_tenant(
    State(state): State<SigbotState>,
    Extension(identity): Extension<RequestIdentity>,
    ValidatedJson(param): ValidatedJson<SaveTenantRequest>,
) -> impl IntoResponse {
    match get_tenant_handler(&state, &identity).save(param).await {
        Ok(result) => Ok(Json(result)),
        Err(e) => {
            common_telemetry::error!("Failed to save tenant: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[utoipa::path(
    post,
    path = "/sys/tenant/delete",
    request_body = DeleteTenantRequest,
    responses((status = 200, description = "Delete for tenant.", body = DeleteTenantResponse)),
    tag = "Tenant"
)]
async fn handle_delete_tenant(
    State(state): State<SigbotState>,
    Extension(identity): Extension<RequestIdentity>,
    Json(param): Json<DeleteTenantRequest>,
) -> impl IntoResponse {
    match get_tenant_handler(&state, &identity).delete(param).await {
        Ok(result) => Ok(Json(DeleteTenantResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_tenant_handler<'a>(state: &'a SigbotState, identity: &'a RequestIdentity) -> Box<dyn ITenantHandler + 'a> {
    Box::new(TenantHandler::new(state, identity))
}
