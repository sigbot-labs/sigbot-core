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

use crate::modules::datafeed::handler::datafeed_handler::DatafeedInfoHandler;
use crate::util::web::ValidatedJson;
use crate::{context::state::SigbotState, modules::datafeed::handler::datafeed_handler::IDatafeedInfoHandler};
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use sigbot_types::modules::datafeed::datafeed::{DeleteDatafeedRequest, QueryDatafeedRequest, SaveDatafeedRequest};
use sigbot_types::{
    modules::datafeed::datafeed::{DeleteDatafeedResponse, QueryDatafeedResponse, SaveDatafeedResponse},
    PageRequest,
};

pub fn init() -> Router<SigbotState> {
    Router::new()
        .route("/services/datafeed/query", get(handle_query_datafeeds))
        .route("/services/datafeed/save", post(handle_save_datafeed))
        .route("/services/datafeed/delete", post(handle_delete_datafeed))
}

#[utoipa::path(
    get,
    path = "/services/datafeed/query",
    params(QueryDatafeedRequest, PageRequest),
    responses((status = 200, description = "Getting for all datafeeds.", body = QueryDatafeedResponse)),
    tag = "Datafeed"
)]
async fn handle_query_datafeeds(
    State(state): State<SigbotState>,
    Query(param): Query<QueryDatafeedRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_datafeed_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryDatafeedResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/datafeed/save",
    request_body = SaveDatafeedRequest,
    responses((status = 200, description = "Save for datafeed.", body = SaveDatafeedResponse)),
    tag = "Datafeed"
)]
async fn handle_save_datafeed(
    State(state): State<SigbotState>,
    ValidatedJson(param): ValidatedJson<SaveDatafeedRequest>,
) -> impl IntoResponse {
    match get_datafeed_handler(&state).save(param).await {
        Ok(result) => Ok(Json(SaveDatafeedResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/datafeed/delete",
    request_body = DeleteDatafeedRequest,
    responses((status = 200, description = "Delete for datafeed.", body = DeleteDatafeedResponse)),
    tag = "Datafeed"
)]
async fn handle_delete_datafeed(
    State(state): State<SigbotState>,
    Json(param): Json<DeleteDatafeedRequest>,
) -> impl IntoResponse {
    match get_datafeed_handler(&state).delete(param).await {
        Ok(result) => Ok(Json(DeleteDatafeedResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_datafeed_handler(state: &SigbotState) -> Box<dyn IDatafeedInfoHandler + '_> {
    Box::new(DatafeedInfoHandler::new(state))
}
