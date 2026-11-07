// SPDX-License-Identifier: GPL-3.0-only
use crate::internal::resource::ResourceHttpApi;
use axum::Router;
use sigbot_core::context::state::SigbotState;
use sigbot_types::domain::Domain;

pub struct NotifierHttpApi;
impl NotifierHttpApi {
    pub fn routes() -> Router<SigbotState> {
        ResourceHttpApi::routes(Domain::Notifier)
    }
}
