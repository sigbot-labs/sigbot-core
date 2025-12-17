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
use crate::modules::wallet::handler::position_handler::{IPositionInfoHandler, PositionInfoHandler};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use sigbot_types::modules::wallet::position::{QueryPositionRequest, QueryPositionResponse};
use sigbot_types::PageRequest;

pub fn position_routes() -> Router<SigbotState> {
    Router::new().route("/services/wallet/positions/query", get(handle_query_positions))
}

#[utoipa::path(
    get,
    path = "/services/wallet/positions/query",
    params(QueryPositionRequest, PageRequest),
    responses((status = 200, description = "Getting for all positions.", body = QueryPositionResponse)),
    tag = "Wallet"
)]
pub async fn handle_query_positions(
    State(state): State<SigbotState>,
    Query(param): Query<QueryPositionRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_position_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryPositionResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_position_handler(state: &SigbotState) -> Box<dyn IPositionInfoHandler + '_> {
    Box::new(PositionInfoHandler::new(state))
}
