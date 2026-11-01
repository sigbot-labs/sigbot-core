// SPDX-License-Identifier: GPL-3.0-only

use super::{IHttpApi, MiddlewareFunction};
use ::axum::Router;
use async_trait::async_trait;
use common_context::IProvider;
use sigbot_core::context::middleware::IdentityRouter;
use sigbot_core::context::state::SigbotState;
use sigbot_core::mgmt::health::HealthRouter;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

pub struct AxumHttpApi {
    state: SigbotState,
}

impl AxumHttpApi {
    pub fn new(state: SigbotState) -> Self {
        Self { state }
    }

    fn business_routes(&self, additional: Option<Router<SigbotState>>) -> Router<SigbotState> {
        let routes = Router::new()
            .merge(IdentityRouter::routes())
            .merge(crate::workflow::WorkflowHttpApi::routes())
            .merge(crate::worker::WorkerHttpApi::routes())
            .merge(crate::workflow::observation::WorkflowObservationHttpApi::routes())
            .merge(crate::connection::ConnectionHttpApi::routes())
            .merge(crate::market::MarketHttpApi::routes())
            .merge(crate::knowledge::KnowledgeHttpApi::routes())
            .merge(crate::notifier::NotifierHttpApi::routes());
        additional.map_or(routes.clone(), |router| routes.merge(router))
    }
}

impl IHttpApi for AxumHttpApi {
    fn router(
        &self,
        additional_router: Option<Router<SigbotState>>,
        additional_middleware: Option<MiddlewareFunction>,
    ) -> Router {
        let business = self.business_routes(additional_router);
        let mut router = match self.state.config.server.context_path.as_deref() {
            Some("/") => Router::new()
                .merge(HealthRouter::routes())
                .merge(business)
                .with_state(self.state.clone()),
            Some(path) => Router::new()
                .merge(HealthRouter::routes())
                .nest(path, business)
                .with_state(self.state.clone()),
            None => Router::new()
                .merge(HealthRouter::routes())
                .merge(business)
                .with_state(self.state.clone()),
        };
        if self.state.config.swagger.enabled {
            router = router.merge(crate::swagger::SwaggerHttpApi::routes(&self.state.config));
        }
        router = router.layer(
            ServiceBuilder::new()
                .layer(::axum::middleware::from_fn_with_state(
                    self.state.clone(),
                    IdentityRouter::authenticate,
                ))
                .layer(TraceLayer::new_for_http().make_span_with(|request: &::axum::http::Request<_>| {
                    tracing::info_span!("http_request", method = %request.method(), path = %request.uri().path())
                })),
        );
        if let Some(middleware) = additional_middleware {
            router = router.layer(::axum::middleware::from_fn_with_state(self.state.clone(), middleware));
        }
        router
    }
}

#[async_trait]
impl IProvider for AxumHttpApi {
    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
