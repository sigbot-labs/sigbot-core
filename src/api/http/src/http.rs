// SPDX-License-Identifier: GPL-3.0-only

use ::axum::body::Body;
use ::axum::extract::{Request, State};
use ::axum::middleware::Next;
use ::axum::response::Response;
use ::axum::Router;
use async_trait::async_trait;
use common_context::IProvider;
use sigbot_core::context::middleware::IdentityRouter;
use sigbot_core::context::state::SigbotState;
use sigbot_core::mgmt::health::HealthRouter;
use std::future::Future;
use std::pin::Pin;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

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
            .merge(crate::tenant::tenant::TenantHttpApi::routes())
            .merge(crate::cluster::cluster::ClusterHttpApi::routes())
            .merge(crate::datasource::datasource::DatasourceHttpApi::routes())
            .merge(crate::exchange::exchange::ExchangeHttpApi::routes())
            .merge(crate::strategy::strategy::StrategyHttpApi::routes())
            .merge(crate::order::order::OrderHttpApi::routes())
            .merge(crate::ledger::ledger::LedgerHttpApi::routes())
            .merge(crate::notifier::notifier::NotifierHttpApi::routes())
            .merge(crate::exporter::exporter::ExporterHttpApi::routes())
            .merge(crate::workflow::workflow::WorkflowHttpApi::routes());
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
