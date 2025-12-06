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

use crate::modules::notification::handler::notification_handler::NotificationInfoHandler;
use crate::util::web::ValidatedJson;
use crate::{
    context::state::SigbotState, modules::notification::handler::notification_handler::INotificationInfoHandler,
};
use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use sigbot_types::modules::notification::notification::{
    DeleteNotificationRequest, QueryNotificationRequest, SaveNotificationRequest,
};
use sigbot_types::{
    modules::notification::notification::{
        DeleteNotificationResponse, QueryNotificationResponse, SaveNotificationResponse,
    },
    PageRequest,
};

pub fn init() -> Router<SigbotState> {
    Router::new()
        .route("/services/notification/query", get(handle_query_notifications))
        .route("/services/notification/save", post(handle_save_notification))
        .route("/services/notification/delete", post(handle_delete_notification))
}

#[utoipa::path(
    get,
    path = "/services/notification/query",
    params(QueryNotificationRequest, PageRequest),
    responses((status = 200, description = "Getting for all notifications.", body = QueryNotificationResponse)),
    tag = "Notification"
)]
async fn handle_query_notifications(
    State(state): State<SigbotState>,
    Query(param): Query<QueryNotificationRequest>,
    Query(page): Query<PageRequest>,
) -> impl IntoResponse {
    match get_notification_handler(&state).find(param, page).await {
        Ok((page, data)) => Ok(Json(QueryNotificationResponse::new(page, data))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/notification/save",
    request_body = SaveNotificationRequest,
    responses((status = 200, description = "Save for notification.", body = SaveNotificationResponse)),
    tag = "Notification"
)]
async fn handle_save_notification(
    State(state): State<SigbotState>,
    ValidatedJson(param): ValidatedJson<SaveNotificationRequest>,
) -> impl IntoResponse {
    match get_notification_handler(&state).save(param).await {
        Ok(result) => Ok(Json(SaveNotificationResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[utoipa::path(
    post,
    path = "/services/notification/delete",
    request_body = DeleteNotificationRequest,
    responses((status = 200, description = "Delete for notification.", body = DeleteNotificationResponse)),
    tag = "Notification"
)]
async fn handle_delete_notification(
    State(state): State<SigbotState>,
    Json(param): Json<DeleteNotificationRequest>,
) -> impl IntoResponse {
    match get_notification_handler(&state).delete(param).await {
        Ok(result) => Ok(Json(DeleteNotificationResponse::new(result))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn get_notification_handler(state: &SigbotState) -> Box<dyn INotificationInfoHandler + '_> {
    Box::new(NotificationInfoHandler::new(state))
}
