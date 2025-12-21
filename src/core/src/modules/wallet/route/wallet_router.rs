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
use crate::modules::wallet::handler::wallet_handler::{IWalletInfoHandler, WalletInfoHandler};
use crate::modules::wallet::route::{balance_router, ledger_router, position_router};
use crate::util::web::ValidatedJson;
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use sigbot_types::modules::wallet::wallet::{
    DeleteWalletRequest, DeleteWalletResponse, QueryWalletRequest, QueryWalletResponse, SaveWalletRequest,
    SaveWalletResponse,
};
use sigbot_types::PageRequest;

pub fn init() -> Router<SigbotState> {
    Router::new()
        .merge(wallet_routes())
        .merge(ledger_router::ledger_routes())
        .merge(balance_router::balance_routes())
        .merge(position_router::position_routes())
}

fn wallet_routes() -> Router<SigbotState> {
    Router::new()
        .route("/services/wallet/query", get(handle_query_wallets))
        .route("/services/wallet/save", post(handle_save_wallet))
        .route("/services/wallet/delete", post(handle_delete_wallet))
}

#[utoipa::path(
    get,
    path = "/services/wallet/query",
    params(QueryWalletRequest, PageRequest),
    responses((status = 200, description = "Getting for all wallets.", body = QueryWalletResponse)),
    tag = "Wallet"
)]
async fn handle_query_wallets(
    State(state): State<SigbotState>,
    Query(param): Query<QueryWalletRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_wallet_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryWalletResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/wallet/save",
    request_body = SaveWalletRequest,
    responses((status = 200, description = "Save for wallet.", body = SaveWalletResponse)),
    tag = "Wallet"
)]
async fn handle_save_wallet(
    State(state): State<SigbotState>,
    ValidatedJson(param): ValidatedJson<SaveWalletRequest>,
) -> impl IntoResponse {
    match get_wallet_handler(&state).save(param).await {
        Ok(result) => Ok(Json(SaveWalletResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/wallet/delete",
    request_body = DeleteWalletRequest,
    responses((status = 200, description = "Delete for wallet.", body = DeleteWalletResponse)),
    tag = "Wallet"
)]
async fn handle_delete_wallet(
    State(state): State<SigbotState>,
    Json(param): Json<DeleteWalletRequest>,
) -> impl IntoResponse {
    match get_wallet_handler(&state).delete(param).await {
        Ok(result) => Ok(Json(DeleteWalletResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_wallet_handler(state: &SigbotState) -> Box<dyn IWalletInfoHandler + '_> {
    Box::new(WalletInfoHandler::new(state))
}
