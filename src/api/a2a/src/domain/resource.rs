// SPDX-License-Identifier: GPL-3.0-only
use crate::a2a::VERIFIED_ACCESS;
use a2a::{A2AError, AgentSkill, Message, Part, PartContent, Role, StreamResponse, Task, TaskState, TaskStatus};
use a2a_server::{AgentExecutor, ExecutorContext};
use anyhow::Result;
use futures::stream::{self, BoxStream};
use serde_json::Value;
use sigbot_core::config::config::AgentConfig;
use sigbot_core::context::{identity::RequestIdentity, state::SigbotState};
use sigbot_core::modules::domain::handler::resource::ResourceHandler;
use sigbot_types::domain::{Domain, ResourceCommand};
use std::sync::Arc;

pub(crate) struct ResourceExecutor {
    state: Arc<SigbotState>,
}

impl ResourceExecutor {
    pub(crate) fn new(state: Arc<SigbotState>) -> Self {
        Self { state }
    }
    pub(crate) fn skills(agent: &AgentConfig) -> Vec<AgentSkill> {
        if agent.capabilities.is_empty() {
            return Domain::ALL
                .into_iter()
                .map(|domain| {
                    Self::skill(
                        domain.as_str(),
                        domain.as_str(),
                        "List, get, create, update and delete authorized domain entities",
                    )
                })
                .collect();
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
            tags: vec!["domain".to_owned()],
            examples: None,
            input_modes: Some(vec!["application/json".to_owned()]),
            output_modes: Some(vec!["application/json".to_owned()]),
            security_requirements: None,
        }
    }

    fn command(ctx: &ExecutorContext) -> Result<ResourceCommand, A2AError> {
        let message = ctx
            .message
            .as_ref()
            .ok_or_else(|| A2AError::invalid_request("domain command required"))?;
        let value = message
            .parts
            .iter()
            .find_map(|part| match &part.content {
                PartContent::Data(value) => Some(Ok(value.clone())),
                PartContent::Text(value) => {
                    Some(serde_json::from_str(value).map_err(|_| A2AError::invalid_params("invalid JSON command")))
                }
                _ => None,
            })
            .ok_or_else(|| A2AError::invalid_params("domain command required"))??;
        serde_json::from_value(value).map_err(|_| A2AError::invalid_params("invalid domain command"))
    }
    async fn execute_command(&self, ctx: &ExecutorContext) -> Result<Value, A2AError> {
        let access = ctx
            .service_params
            .get(VERIFIED_ACCESS)
            .and_then(|values| values.first())
            .ok_or_else(|| A2AError::invalid_request("verified identity required"))?;
        let identity = RequestIdentity::from_verified_value(
            serde_json::from_str(access).map_err(|_| A2AError::invalid_request("invalid verified identity"))?,
        )
        .map_err(|_| A2AError::invalid_request("invalid verified identity"))?;
        ResourceHandler::execute(&self.state, &identity, Self::command(ctx)?)
            .await
            .map_err(|error| {
                tracing::warn!(%error,"A2A domain operation failed");
                A2AError::internal("domain operation rejected")
            })
    }
}

impl AgentExecutor for ResourceExecutor {
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
