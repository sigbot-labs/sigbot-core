// SPDX-License-Identifier: GPL-3.0-only
use crate::internal::resource::ResourceHttpApi;
use axum::Router;
use sigbot_core::context::state::SigbotState;
use sigbot_types::domain::Domain;

pub struct ExporterHttpApi;
impl ExporterHttpApi {
    pub fn routes() -> Router<SigbotState> {
        ResourceHttpApi::routes(Domain::Exporter)
    }
}
