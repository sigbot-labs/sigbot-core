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

use crate::modules::strategy::handler::strategy_handler::StrategyInfoHandler;
use crate::util::web::ValidatedJson;
use crate::{context::state::SigbotState, modules::strategy::handler::strategy_handler::IStrategyInfoHandler};
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use sigbot_types::modules::strategy::strategy::{DeleteStrategyRequest, QueryStrategyRequest, SaveStrategyRequest};
use sigbot_types::{
    modules::strategy::strategy::{DeleteStrategyResponse, QueryStrategyResponse, SaveStrategyResponse},
    PageRequest,
};

pub fn init() -> Router<SigbotState> {
    Router::new()
        .route("/services/strategy/query", get(handle_query_strategys))
        .route("/services/strategy/save", post(handle_save_strategy))
        .route("/services/strategy/delete", post(handle_delete_strategy))
}

#[utoipa::path(
    get,
    path = "/services/strategy/query",
    params(QueryStrategyRequest, PageRequest),
    responses((status = 200, description = "Getting for all strategys.", body = QueryStrategyResponse)),
    tag = "Strategy"
)]
async fn handle_query_strategys(
    State(state): State<SigbotState>,
    Query(param): Query<QueryStrategyRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_strategy_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryStrategyResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/strategy/save",
    request_body = SaveStrategyRequest,
    responses((status = 200, description = "Save for strategy.", body = SaveStrategyResponse)),
    tag = "Strategy"
)]
async fn handle_save_strategy(
    State(state): State<SigbotState>,
    ValidatedJson(param): ValidatedJson<SaveStrategyRequest>,
) -> impl IntoResponse {
    match get_strategy_handler(&state).save(param).await {
        Ok(result) => Ok(Json(SaveStrategyResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/strategy/delete",
    request_body = DeleteStrategyRequest,
    responses((status = 200, description = "Delete for strategy.", body = DeleteStrategyResponse)),
    tag = "Strategy"
)]
async fn handle_delete_strategy(
    State(state): State<SigbotState>,
    Json(param): Json<DeleteStrategyRequest>,
) -> impl IntoResponse {
    match get_strategy_handler(&state).delete(param).await {
        Ok(result) => Ok(Json(DeleteStrategyResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_strategy_handler(state: &SigbotState) -> Box<dyn IStrategyInfoHandler + '_> {
    Box::new(StrategyInfoHandler::new(state))
}
