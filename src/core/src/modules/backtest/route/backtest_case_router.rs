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

use crate::context::state::SigbotState;
use crate::modules::backtest::handler::backtest_case_handler::{BacktestCaseInfoHandler, IBacktestCaseInfoHandler};
use crate::util::web::ValidatedJson;
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use sigbot_types::modules::backtest::backtest_case::{
    DeleteBacktestCaseRequest, DeleteBacktestCaseResponse, QueryBacktestCaseRequest, QueryBacktestCaseResponse,
    SaveBacktestCaseRequest, SaveBacktestCaseResponse,
};
use sigbot_types::PageRequest;

pub fn init() -> Router<SigbotState> {
    Router::new()
        .route("/services/backtest_case/query", get(handle_query_backtest_cases))
        .route("/services/backtest_case/save", post(handle_save_backtest_case))
        .route("/services/backtest_case/delete", post(handle_delete_backtest_case))
}

#[utoipa::path(
    get,
    path = "/services/backtest_case/query",
    params(QueryBacktestCaseRequest, PageRequest),
    responses((status = 200, description = "Getting for all backtest cases.", body = QueryBacktestCaseResponse)),
    tag = "BacktestCase"
)]
async fn handle_query_backtest_cases(
    State(state): State<SigbotState>,
    Query(param): Query<QueryBacktestCaseRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_backtest_case_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryBacktestCaseResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/backtest_case/save",
            request_body = SaveBacktestCaseRequest,
    responses((status = 200, description = "Save for backtest case.", body = SaveBacktestCaseResponse)),
    tag = "BacktestCase"
)]
async fn handle_save_backtest_case(
    State(state): State<SigbotState>,
    ValidatedJson(param): ValidatedJson<SaveBacktestCaseRequest>,
) -> impl IntoResponse {
    match get_backtest_case_handler(&state).save(param).await {
        Ok(result) => Ok(Json(SaveBacktestCaseResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/backtest_case/delete",
    request_body = DeleteBacktestCaseRequest,
    responses((status = 200, description = "Delete for backtest case.", body = DeleteBacktestCaseResponse)),
    tag = "BacktestCase"
)]
async fn handle_delete_backtest_case(
    State(state): State<SigbotState>,
    Json(param): Json<DeleteBacktestCaseRequest>,
) -> impl IntoResponse {
    match get_backtest_case_handler(&state).delete(param).await {
        Ok(result) => Ok(Json(DeleteBacktestCaseResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_backtest_case_handler(state: &SigbotState) -> Box<dyn IBacktestCaseInfoHandler + '_> {
    Box::new(BacktestCaseInfoHandler::new(state))
}
