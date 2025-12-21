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

use crate::modules::exchange::handler::exchange_handler::ExchangeInfoHandler;
use crate::util::web::ValidatedJson;
use crate::{context::state::SigbotState, modules::exchange::handler::exchange_handler::IExchangeInfoHandler};
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use sigbot_types::modules::exchange::exchange::{DeleteExchangeRequest, QueryExchangeRequest, SaveExchangeRequest};
use sigbot_types::{
    modules::exchange::exchange::{DeleteExchangeResponse, QueryExchangeResponse, SaveExchangeResponse},
    PageRequest,
};

pub fn init() -> Router<SigbotState> {
    Router::new()
        .route("/services/exchange/query", get(handle_query_exchanges))
        .route("/services/exchange/save", post(handle_save_exchange))
        .route("/services/exchange/delete", post(handle_delete_exchange))
}

#[utoipa::path(
    get,
    path = "/services/exchange/query",
    params(QueryExchangeRequest, PageRequest),
    responses((status = 200, description = "Getting for all exchanges.", body = QueryExchangeResponse)),
    tag = "Exchange"
)]
async fn handle_query_exchanges(
    State(state): State<SigbotState>,
    Query(param): Query<QueryExchangeRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_exchange_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryExchangeResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/exchange/save",
    request_body = SaveExchangeRequest,
    responses((status = 200, description = "Save for exchange.", body = SaveExchangeResponse)),
    tag = "Exchange"
)]
async fn handle_save_exchange(
    State(state): State<SigbotState>,
    ValidatedJson(param): ValidatedJson<SaveExchangeRequest>,
) -> impl IntoResponse {
    match get_exchange_handler(&state).save(param).await {
        Ok(result) => Ok(Json(SaveExchangeResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/exchange/delete",
    request_body = DeleteExchangeRequest,
    responses((status = 200, description = "Delete for exchange.", body = DeleteExchangeResponse)),
    tag = "Exchange"
)]
async fn handle_delete_exchange(
    State(state): State<SigbotState>,
    Json(param): Json<DeleteExchangeRequest>,
) -> impl IntoResponse {
    match get_exchange_handler(&state).delete(param).await {
        Ok(result) => Ok(Json(DeleteExchangeResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_exchange_handler(state: &SigbotState) -> Box<dyn IExchangeInfoHandler + '_> {
    Box::new(ExchangeInfoHandler::new(state))
}
