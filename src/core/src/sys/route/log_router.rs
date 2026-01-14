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
use crate::sys::handler::log_handler::{ILogHandler, LogHandler};
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use sigbot_types::sys::log::{
    SearchLogRequest, SearchLogResponse, StatsLogRequest, StatsLogResponse, TailLogRequest, TailLogResponse,
};
use sigbot_types::PageRequest;

pub fn init() -> Router<SigbotState> {
    Router::new()
        .route("/sys/log/tail", get(handle_tail_log))
        .route("/sys/log/search", get(handle_search_log))
        .route("/sys/log/stats", get(handle_stats_log))
}

#[utoipa::path(
    get,
    path = "/sys/log/tail",
    params(TailLogRequest),
    responses((status = 200, description = "Get recent log entries.", body = TailLogResponse)),
    tag = "Log"
)]
async fn handle_tail_log(State(state): State<SigbotState>, Query(param): Query<TailLogRequest>) -> impl IntoResponse {
    match get_log_handler(&state).tail(param).await {
        Ok(logs) => Ok(Json(TailLogResponse::new(logs))),
        Err(e) => {
            common_telemetry::error!("Failed to tail logs: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[utoipa::path(
    get,
    path = "/sys/log/search",
    params(SearchLogRequest, PageRequest),
    responses((status = 200, description = "Search log entries.", body = SearchLogResponse)),
    tag = "Log"
)]
async fn handle_search_log(
    State(state): State<SigbotState>,
    Query(param): Query<SearchLogRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_log_handler(&state).search(param, page).await {
        Ok((page_resp, data)) => Ok(Json(SearchLogResponse::new(page_resp, data))),
        Err(e) => {
            common_telemetry::error!("Failed to search logs: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[utoipa::path(
    get,
    path = "/sys/log/stats",
    params(StatsLogRequest),
    responses((status = 200, description = "Get log statistics.", body = StatsLogResponse)),
    tag = "Log"
)]
async fn handle_stats_log(State(state): State<SigbotState>, Query(param): Query<StatsLogRequest>) -> impl IntoResponse {
    match get_log_handler(&state).stats(param).await {
        Ok(stats) => Ok(Json(StatsLogResponse::new(stats))),
        Err(e) => {
            common_telemetry::error!("Failed to get log stats: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn get_log_handler(state: &SigbotState) -> Box<dyn ILogHandler + '_> {
    Box::new(LogHandler::new(state))
}
