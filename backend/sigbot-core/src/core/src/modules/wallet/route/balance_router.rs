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
use crate::modules::wallet::handler::balance_handler::{BalanceInfoHandler, IBalanceInfoHandler};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use sigbot_types::modules::wallet::balance::{QueryBalanceRequest, QueryBalanceResponse};
use sigbot_types::PageRequest;

pub fn balance_routes() -> Router<SigbotState> {
    Router::new().route("/services/wallet/balances/query", get(handle_query_balances))
}

#[utoipa::path(
    get,
    path = "/services/wallet/balances/query",
    params(QueryBalanceRequest, PageRequest),
    responses((status = 200, description = "Getting for all balances.", body = QueryBalanceResponse)),
    tag = "Wallet"
)]
pub async fn handle_query_balances(
    State(state): State<SigbotState>,
    Query(param): Query<QueryBalanceRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_balance_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryBalanceResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_balance_handler(state: &SigbotState) -> Box<dyn IBalanceInfoHandler + '_> {
    Box::new(BalanceInfoHandler::new(state))
}
