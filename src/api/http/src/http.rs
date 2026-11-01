// SPDX-License-Identifier: GPL-3.0-only

use ::axum::body::Body;
use ::axum::extract::{Request, State};
use ::axum::middleware::Next;
use ::axum::response::Response;
use ::axum::Router;
use common_context::IProvider;
use sigbot_core::context::state::SigbotState;
use std::future::Future;
use std::pin::Pin;

pub mod axum;

pub type MiddlewareFunction =
    fn(State<SigbotState>, Request<Body>, Next) -> Pin<Box<dyn Future<Output = Response<Body>> + Send + 'static>>;

/// Standard HTTP API protocol boundary.
pub trait IHttpApi: IProvider {
    fn router(
        &self,
        additional_router: Option<Router<SigbotState>>,
        additional_middleware: Option<MiddlewareFunction>,
    ) -> Router;
}
