// SPDX-License-Identifier: GPL-3.0-only
use crate::domain::resource::ResourceExecutor;
use a2a::{AgentCapabilities, AgentCard, AgentInterface, SecurityScheme, TRANSPORT_PROTOCOL_JSONRPC};
use a2a_server::{
    agent_card::agent_card_router, jsonrpc::jsonrpc_router, AgentExecutor, DefaultRequestHandler, InMemoryTaskStore,
    StaticAgentCard,
};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{OriginalUri, Request, State};
use axum::http::{header::AUTHORIZATION, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use common_context::IProvider;
use serde_json::json;
use sigbot_core::config::config::{A2AProperties, AgentConfig, OAuthDiscoveryProperties};
use sigbot_core::context::{authguard::AuthguardProvider, state::SigbotState};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex},
    task::JoinHandle,
};

pub(crate) const VERIFIED_ACCESS: &str = "x-sigbot-verified-access";
pub trait IA2aApi: IProvider {
    fn router(&self) -> Router;
}

pub struct ResourceA2aApi {
    config: A2AProperties,
    oauth: Option<OAuthDiscoveryProperties>,
    authguard: Arc<AuthguardProvider>,
    state: Arc<SigbotState>,
    process: Mutex<Option<A2aProcess>>,
}
struct A2aProcess {
    stop: oneshot::Sender<()>,
    task: JoinHandle<std::io::Result<()>>,
}

impl ResourceA2aApi {
    pub fn new(config: A2AProperties, state: SigbotState) -> Self {
        Self {
            config,
            oauth: state.config.authguard.oauth.clone(),
            authguard: state.authguard.clone(),
            state: Arc::new(state),
            process: Mutex::new(None),
        }
    }
    fn configured_agent(config: &A2AProperties) -> AgentConfig {
        config.agents.first().cloned().unwrap_or_else(|| AgentConfig {
            name: "Sigbot Domain Agent".into(),
            description: "Manage tenant-scoped Sigbot domain entities".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: Vec::new(),
        })
    }
    fn card(config: &A2AProperties, oauth: Option<&OAuthDiscoveryProperties>) -> Result<AgentCard> {
        let public = OAuthDiscoveryProperties::validate_url(&config.public_url)?;
        ensure!(
            public.query().is_none() && public.path().trim_end_matches('/').ends_with("/a2a"),
            "A2A public_url must identify its /a2a protocol endpoint without a query"
        );
        let agent = Self::configured_agent(config);
        let scheme = if let Some(oauth) = oauth {
            oauth.validate()?;
            SecurityScheme::OpenIdConnect(a2a::OpenIdConnectSecurityScheme {
                open_id_connect_url: oauth.discovery_url.clone(),
                description: Some("Authenticate through the configured Authguard OAuth/OIDC authority".into()),
            })
        } else {
            SecurityScheme::HttpAuth(a2a::HttpAuthSecurityScheme {
                scheme: "Bearer".into(),
                description: Some("Authguard verified context or scope token supplied by the trusted gateway".into()),
                bearer_format: None,
            })
        };
        Ok(AgentCard {
            name: agent.name.clone(),
            description: agent.description.clone(),
            version: agent.version.clone(),
            supported_interfaces: vec![AgentInterface::new(
                config.public_url.clone(),
                TRANSPORT_PROTOCOL_JSONRPC,
            )],
            capabilities: AgentCapabilities {
                streaming: Some(true),
                push_notifications: Some(false),
                extensions: None,
                extended_agent_card: Some(true),
            },
            default_input_modes: vec!["application/json".into(), "text/plain".into()],
            default_output_modes: vec!["application/json".into()],
            skills: ResourceExecutor::skills(&agent),
            provider: None,
            documentation_url: None,
            icon_url: None,
            security_schemes: Some(HashMap::from([("authguard".into(), scheme)])),
            security_requirements: Some(vec![HashMap::from([("authguard".into(), Vec::new())])]),
            signatures: None,
        })
    }
    fn protocol_router<E: AgentExecutor>(
        config: &A2AProperties,
        oauth: Option<&OAuthDiscoveryProperties>,
        authguard: Arc<AuthguardProvider>,
        executor: E,
    ) -> Result<Router> {
        let card = Self::card(config, oauth)?;
        let public = OAuthDiscoveryProperties::validate_url(&config.public_url)?;
        let path = public.path().trim_end_matches('/');
        let handler = Arc::new(
            DefaultRequestHandler::new(executor, InMemoryTaskStore::new())
                .with_capabilities(card.capabilities.clone())
                .with_default_input_modes(vec!["application/json".into(), "text/plain".into()])
                .with_extended_agent_card(card.clone()),
        );
        // nest_service preserves both /a2a and /a2a/ for the SDK's root RPC route.
        let protected = Router::new()
            .nest_service(path, jsonrpc_router(handler))
            .layer(from_fn_with_state(authguard, Self::authenticate));
        Ok(protected.merge(agent_card_router(Arc::new(StaticAgentCard::new(card)))))
    }
    fn build_router(&self) -> Result<Router> {
        Self::protocol_router(
            &self.config,
            self.oauth.as_ref(),
            self.authguard.clone(),
            ResourceExecutor::new(self.state.clone()),
        )
    }
    async fn authenticate(
        State(authguard): State<Arc<AuthguardProvider>>,
        mut request: Request<Body>,
        next: Next,
    ) -> Response {
        let path = request
            .extensions()
            .get::<OriginalUri>()
            .map(|uri| uri.0.path())
            .unwrap_or_else(|| request.uri().path())
            // Authguard URNs require non-empty segments; both RPC routes share one canonical resource.
            .trim_end_matches('/')
            .to_owned();
        let mut identity = authguard.authenticate(request.headers(), request.method(), &path).await;
        if identity.is_err() {
            if let Some(token) = request
                .headers()
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split_once(' '))
                .filter(|(kind, _)| kind.eq_ignore_ascii_case("Bearer"))
                .map(|(_, token)| token.trim())
            {
                identity = authguard.authenticate_bearer(token, request.method(), &path).await;
            }
        }
        let identity = match identity {
            Ok(identity) => identity,
            Err(_) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
                    Json(json!({"error":"authguard_context_required"})),
                )
                    .into_response()
            }
        };
        let access = serde_json::to_string(&identity.to_verified_value())
            .ok()
            .and_then(|value| HeaderValue::from_str(&value).ok());
        match access {
            Some(value) => {
                request
                    .headers_mut()
                    .insert(HeaderName::from_static(VERIFIED_ACCESS), value);
                next.run(request).await
            }
            None => StatusCode::UNAUTHORIZED.into_response(),
        }
    }
    async fn start(&self) -> Result<()> {
        ensure!(self.config.enabled, "A2A API is disabled");
        ensure!(self.config.auth_enabled, "A2A API requires Authguard");
        ensure!(self.authguard.is_configured(), "Authguard verifier is unavailable");
        let router = self.build_router()?;
        let mut process = self.process.lock().await;
        ensure!(process.is_none(), "A2A API is already running");
        let listener = TcpListener::bind(&self.config.bind_address)
            .await
            .context("failed to bind A2A API")?;
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await
        });
        *process = Some(A2aProcess { stop, task });
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        let Some(process) = self.process.lock().await.take() else {
            return Ok(());
        };
        let _ = process.stop.send(());
        process.task.await.context("A2A API task failed")??;
        Ok(())
    }
}
impl IA2aApi for ResourceA2aApi {
    fn router(&self) -> Router {
        self.build_router()
            .expect("invalid A2A protocol discovery configuration")
    }
}
#[async_trait]
impl IProvider for ResourceA2aApi {
    async fn init(&self) -> Result<()> {
        self.start().await
    }
    async fn close(&self) -> Result<()> {
        self.stop().await
    }
}

#[cfg(test)]
#[path = "../tests/a2a/discovery_test.rs"]
mod discovery_tests;
