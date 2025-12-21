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

use crate::modules::wallet::handler::ledger_handler::ILedgerInfoHandler;
use crate::{context::state::SigbotState, modules::wallet::handler::ledger_handler::LedgerInfoHandler};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use sigbot_types::modules::wallet::ledger::{QueryLedgerRequest, QueryLedgerResponse};
use sigbot_types::PageRequest;

pub fn ledger_routes() -> Router<SigbotState> {
    Router::new().route("/services/wallet/ledgers/query", get(handle_query_ledgers))
}

#[utoipa::path(
    get,
    path = "/services/wallet/ledgers/query",
    params(QueryLedgerRequest, PageRequest),
    responses((status = 200, description = "Getting for all ledgers.", body = QueryLedgerResponse)),
    tag = "Wallet"
)]
pub async fn handle_query_ledgers(
    State(state): State<SigbotState>,
    Query(param): Query<QueryLedgerRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_ledger_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryLedgerResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_ledger_handler(state: &SigbotState) -> Box<dyn ILedgerInfoHandler + '_> {
    Box::new(LedgerInfoHandler::new(state))
}
