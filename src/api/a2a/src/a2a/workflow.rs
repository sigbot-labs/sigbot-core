// SPDX-License-Identifier: GPL-3.0-only

use super::IA2aApi;
use a2a::{
    A2AError, AgentCapabilities, AgentCard, AgentInterface, AgentSkill, Message, Part, PartContent, Role,
    SecurityScheme, StreamResponse, Task, TaskState, TaskStatus, TRANSPORT_PROTOCOL_JSONRPC,
};
use a2a_server::{
    agent_card::agent_card_router, jsonrpc::jsonrpc_router, AgentExecutor, DefaultRequestHandler, ExecutorContext,
    InMemoryTaskStore, StaticAgentCard,
};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use common_context::IProvider;
use futures::stream::{self, BoxStream};
use serde::Deserialize;
use serde_json::{json, Value};
use sigbot_core::config::config::{A2AProperties, AgentConfig};
use sigbot_core::context::authguard::AuthguardProvider;
use sigbot_core::context::state::SigbotState;
use sigbot_core::modules::workflow::handler::workflow::IWorkflowHandler;
use sigbot_core::modules::workflow::handler::workflow::WorkflowHandler;
use sigbot_types::Uuid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;

const VERIFIED_TENANT: &str = "x-sigbot-verified-tenant-id";
const VERIFIED_PRINCIPAL: &str = "x-sigbot-verified-principal-id";

pub struct WorkflowA2aApi {
    config: A2AProperties,
    authguard: Arc<AuthguardProvider>,
    state: Arc<SigbotState>,
    process: Mutex<Option<A2aProcess>>,
}

struct A2aProcess {
    stop: oneshot::Sender<()>,
    task: JoinHandle<std::io::Result<()>>,
}

impl WorkflowA2aApi {
    pub fn new(config: A2AProperties, state: SigbotState) -> Self {
        Self {
            config,
            authguard: state.authguard.clone(),
            state: Arc::new(state),
            process: Mutex::new(None),
        }
    }

    fn configured_agent(&self) -> AgentConfig {
        self.config.agents.first().cloned().unwrap_or_else(|| AgentConfig {
            name: "Sigbot Workflow Agent".to_owned(),
            description: "Inspect and control tenant-scoped Sigbot workflows".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            capabilities: Vec::new(),
        })
    }

    fn skills(agent: &AgentConfig) -> Vec<AgentSkill> {
        if agent.capabilities.is_empty() {
            return vec![
                Self::skill(
                    "workflow-observation",
                    "Workflow observation",
                    "Read tenant workflow summaries and ordered events",
                ),
                Self::skill(
                    "workflow-lifecycle",
                    "Workflow lifecycle",
                    "Idempotently start or stop an authorized workflow",
                ),
            ];
        }
        agent
            .capabilities
            .iter()
            .map(|capability| Self::skill(&capability.name, &capability.name, &capability.description))
            .collect()
    }

    fn skill(id: &str, name: &str, description: &str) -> AgentSkill {
        AgentSkill {
            id: id.to_owned(),
            name: name.to_owned(),
            description: description.to_owned(),
            tags: vec!["workflow".to_owned()],
            examples: None,
            input_modes: Some(vec!["application/json".to_owned()]),
            output_modes: Some(vec!["application/json".to_owned()]),
            security_requirements: None,
        }
    }

    fn card(&self) -> AgentCard {
        let agent = self.configured_agent();
        let skills = Self::skills(&agent);
        let security_schemes = HashMap::from([(
            "authguard".to_owned(),
            SecurityScheme::HttpAuth(a2a::HttpAuthSecurityScheme {
                scheme: "authguard".to_owned(),
                description: Some("Trusted request context issued by the Authguard gateway".to_owned()),
                bearer_format: None,
            }),
        )]);
        AgentCard {
            name: agent.name,
            description: agent.description,
            version: agent.version,
            supported_interfaces: vec![AgentInterface::new(
                self.config.public_url.clone(),
                TRANSPORT_PROTOCOL_JSONRPC,
            )],
            capabilities: AgentCapabilities {
                streaming: Some(true),
                push_notifications: Some(false),
                extensions: None,
                extended_agent_card: Some(false),
            },
            default_input_modes: vec!["application/json".to_owned(), "text/plain".to_owned()],
            default_output_modes: vec!["application/json".to_owned()],
            skills,
            provider: None,
            documentation_url: None,
            icon_url: None,
            security_schemes: Some(security_schemes),
            security_requirements: Some(vec![HashMap::from([("authguard".to_owned(), Vec::new())])]),
            signatures: None,
        }
    }

    fn build_router(&self) -> Router {
        let executor = WorkflowExecutor {
            state: self.state.clone(),
        };
        let card = self.card();
        let handler = Arc::new(
            DefaultRequestHandler::new(executor, InMemoryTaskStore::new())
                .with_capabilities(card.capabilities.clone())
                .with_default_input_modes(vec!["application/json".to_owned(), "text/plain".to_owned()]),
        );
        let protocol = jsonrpc_router(handler).layer(from_fn_with_state(self.authguard.clone(), Self::authenticate));
        Router::new()
            .nest("/a2a", protocol)
            .merge(agent_card_router(Arc::new(StaticAgentCard::new(card))))
    }

    async fn authenticate(
        State(authguard): State<Arc<AuthguardProvider>>,
        mut request: Request<Body>,
        next: Next,
    ) -> Response {
        let identity = match authguard
            .authenticate(request.headers(), request.method(), request.uri().path())
            .await
        {
            Ok(identity) => identity,
            Err(_) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"error":"authguard_context_required"})),
                )
                    .into_response();
            }
        };
        let values = (
            HeaderValue::from_str(identity.tenant_id()),
            HeaderValue::from_str(identity.principal_id()),
        );
        match values {
            (Ok(tenant), Ok(principal)) => {
                request
                    .headers_mut()
                    .insert(HeaderName::from_static(VERIFIED_TENANT), tenant);
                request
                    .headers_mut()
                    .insert(HeaderName::from_static(VERIFIED_PRINCIPAL), principal);
                next.run(request).await
            }
            _ => (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"invalid_authguard_identity"})),
            )
                .into_response(),
        }
    }

    async fn start(&self) -> Result<()> {
        ensure!(self.config.enabled, "A2A API is disabled");
        ensure!(self.config.auth_enabled, "A2A API requires Authguard");
        ensure!(self.authguard.is_configured(), "Authguard verifier is unavailable");
        let listener = TcpListener::bind(&self.config.bind_address)
            .await
            .with_context(|| format!("failed to bind A2A API at {}", self.config.bind_address))?;
        let (stop, stopped) = oneshot::channel();
        let router = self.build_router();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await
        });
        let mut process = self.process.lock().await;
        ensure!(process.is_none(), "A2A API is already running");
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

impl IA2aApi for WorkflowA2aApi {
    fn router(&self) -> Router {
        self.build_router()
    }
}

#[async_trait]
impl IProvider for WorkflowA2aApi {
    async fn init(&self) -> Result<()> {
        self.start().await
    }

    async fn close(&self) -> Result<()> {
        self.stop().await
    }
}

struct WorkflowExecutor {
    state: Arc<SigbotState>,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum WorkflowCommand {
    Overview { env: String },
    Events { env: String, workflow_id: Uuid },
    Start { env: String, workflow_id: Uuid },
    Stop { env: String, workflow_id: Uuid },
}

impl WorkflowExecutor {
    fn handler(&self, tenant: Uuid, principal: &str, env: &str) -> Result<WorkflowHandler, A2AError> {
        WorkflowHandler::for_scope(self.state.clone(), tenant, principal, env)
            .map_err(|error| A2AError::invalid_request(error.to_string()))
    }

    fn verified(ctx: &ExecutorContext, header: &str) -> Result<String, A2AError> {
        ctx.service_params
            .get(header)
            .and_then(|values| values.first())
            .cloned()
            .ok_or_else(|| A2AError::invalid_request(format!("missing verified {header}")))
    }

    fn command(ctx: &ExecutorContext) -> Result<WorkflowCommand, A2AError> {
        let message = ctx
            .message
            .as_ref()
            .ok_or_else(|| A2AError::invalid_request("workflow command message is required"))?;
        let value = message
            .parts
            .iter()
            .find_map(|part| match &part.content {
                PartContent::Data(value) => Some(Ok(value.clone())),
                PartContent::Text(value) => {
                    Some(serde_json::from_str(value).map_err(|error| A2AError::invalid_params(error.to_string())))
                }
                _ => None,
            })
            .ok_or_else(|| A2AError::invalid_params("JSON workflow command is required"))??;
        serde_json::from_value(value).map_err(|error| A2AError::invalid_params(error.to_string()))
    }

    async fn execute_command(&self, ctx: &ExecutorContext) -> Result<Value, A2AError> {
        let tenant = Self::verified(ctx, VERIFIED_TENANT)?
            .parse::<Uuid>()
            .map_err(|error| A2AError::invalid_request(error.to_string()))?;
        let principal = Self::verified(ctx, VERIFIED_PRINCIPAL)?;
        let result = match Self::command(ctx)? {
            WorkflowCommand::Overview { env } => self.handler(tenant, &principal, &env)?.overview(&env).await,
            WorkflowCommand::Events { env, workflow_id } => self
                .handler(tenant, &principal, &env)?
                .events(&env, workflow_id)
                .await
                .map(|events| json!({"events": events})),
            WorkflowCommand::Start { env, workflow_id } => self
                .handler(tenant, &principal, &env)?
                .request_run(&env, workflow_id, true)
                .await
                .map(|run_id| json!({"run_id": run_id, "state": "queued"})),
            WorkflowCommand::Stop { env, workflow_id } => self
                .handler(tenant, &principal, &env)?
                .request_run(&env, workflow_id, false)
                .await
                .map(|run_id| json!({"run_id": run_id, "state": "stopping"})),
        };
        result.map_err(|error| {
            tracing::warn!(error = %error, "A2A workflow command failed");
            A2AError::internal("workflow command failed")
        })
    }
}

impl AgentExecutor for WorkflowExecutor {
    fn execute(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        let executor = Self {
            state: self.state.clone(),
        };
        Box::pin(stream::once(async move {
            let value = executor.execute_command(&ctx).await?;
            Ok(StreamResponse::Message(Message::new(
                Role::Agent,
                vec![Part::data(value)],
            )))
        }))
    }

    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        let task = Task {
            id: ctx.task_id,
            context_id: ctx.context_id,
            status: TaskStatus {
                state: TaskState::Canceled,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        Box::pin(stream::once(async move { Ok(StreamResponse::Task(task)) }))
    }
}
