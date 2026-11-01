// SPDX-License-Identifier: GPL-3.0-only

use async_trait::async_trait;
use rust_mcp_sdk::mcp_server::ServerHandler;
use rust_mcp_sdk::schema::{
    CallToolError, CallToolRequestParams, CallToolResult, ContentBlock, ListToolsResult, PaginatedRequestParams,
    RpcError, TextContent, Tool, ToolAnnotations, ToolInputSchema,
};
use rust_mcp_sdk::McpServer;
use serde_json::{json, Map, Value};
use sigbot_core::context::state::SigbotState;
use sigbot_core::modules::workflow::handler::workflow::IWorkflowHandler;
use sigbot_core::modules::workflow::handler::workflow::WorkflowHandler;
use sigbot_types::Uuid;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct WorkflowMcpRegistry {
    state: Arc<SigbotState>,
}

impl WorkflowMcpRegistry {
    pub fn new(state: Arc<SigbotState>) -> Self {
        Self { state }
    }

    fn property(kind: &str, description: &str) -> Map<String, Value> {
        Map::from_iter([
            ("type".to_owned(), Value::String(kind.to_owned())),
            ("description".to_owned(), Value::String(description.to_owned())),
        ])
    }

    fn tool(name: &str, description: &str, workflow: bool, read_only: bool) -> Tool {
        let mut properties = BTreeMap::from([(
            "env".to_owned(),
            Self::property("string", "Deployment environment such as dev, sit, uat, or prod"),
        )]);
        let mut required = vec!["env".to_owned()];
        if workflow {
            properties.insert(
                "workflow_id".to_owned(),
                Self::property("string", "Authorized UUIDv7 workflow ID"),
            );
            required.push("workflow_id".to_owned());
        }
        Tool {
            annotations: Some(ToolAnnotations {
                destructive_hint: Some(false),
                idempotent_hint: Some(true),
                open_world_hint: Some(false),
                read_only_hint: Some(read_only),
                title: None,
            }),
            description: Some(description.to_owned()),
            execution: None,
            icons: Vec::new(),
            input_schema: ToolInputSchema::new(required, Some(properties), None),
            meta: None,
            name: name.to_owned(),
            output_schema: None,
            title: None,
        }
    }

    fn argument<T: serde::de::DeserializeOwned>(arguments: &Map<String, Value>, name: &str) -> anyhow::Result<T> {
        serde_json::from_value(
            arguments
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing {name}"))?,
        )
        .map_err(Into::into)
    }

    async fn invoke(&self, params: CallToolRequestParams, runtime: Arc<dyn McpServer>) -> anyhow::Result<Value> {
        let auth = runtime
            .auth_info_cloned()
            .await
            .ok_or_else(|| anyhow::anyhow!("authenticated MCP context required"))?;
        let tenant: Uuid = auth
            .extra
            .as_ref()
            .and_then(|extra| extra.get("tenant_id"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("verified tenant missing"))?
            .parse()?;
        let arguments = params.arguments.unwrap_or_default();
        let env: String = Self::argument(&arguments, "env")?;
        let principal = auth
            .user_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("verified principal missing"))?;
        let handler = WorkflowHandler::for_scope(self.state.clone(), tenant, principal, &env)?;
        match params.name.as_str() {
            "workflow_overview" => handler.overview(&env).await,
            "workflow_events" => {
                let workflow_id = Self::argument(&arguments, "workflow_id")?;
                handler
                    .events(&env, workflow_id)
                    .await
                    .map(|events| json!({"events": events}))
            }
            "workflow_start" | "workflow_stop" => {
                let workflow_id = Self::argument(&arguments, "workflow_id")?;
                let start = params.name == "workflow_start";
                handler
                    .request_run(&env, workflow_id, start)
                    .await
                    .map(|run_id| json!({"run_id":run_id,"state":if start {"queued"} else {"stopping"}}))
            }
            _ => anyhow::bail!("unknown workflow tool"),
        }
    }

    fn result(result: anyhow::Result<Value>) -> CallToolResult {
        match result {
            Ok(Value::Object(value)) => CallToolResult {
                content: vec![ContentBlock::from(TextContent::new(
                    Value::Object(value.clone()).to_string(),
                    None,
                    None,
                ))],
                is_error: Some(false),
                meta: None,
                structured_content: Some(value),
            },
            Ok(value) => CallToolResult {
                content: vec![ContentBlock::from(TextContent::new(value.to_string(), None, None))],
                is_error: Some(false),
                meta: None,
                structured_content: Some(Map::from_iter([("value".to_owned(), value)])),
            },
            Err(error) => {
                tracing::warn!(%error, "MCP workflow tool failed");
                CallToolResult {
                    content: vec![ContentBlock::from(TextContent::new(
                        "workflow operation failed".to_owned(),
                        None,
                        None,
                    ))],
                    is_error: Some(true),
                    meta: None,
                    structured_content: None,
                }
            }
        }
    }
}

#[async_trait]
impl ServerHandler for WorkflowMcpRegistry {
    async fn handle_list_tools_request(
        &self,
        _params: Option<PaginatedRequestParams>,
        _runtime: Arc<dyn McpServer>,
    ) -> Result<ListToolsResult, RpcError> {
        Ok(ListToolsResult {
            meta: None,
            next_cursor: None,
            tools: vec![
                Self::tool("workflow_overview", "Read the tenant workflow overview", false, true),
                Self::tool("workflow_events", "Read ordered events for one workflow", true, true),
                Self::tool("workflow_start", "Idempotently start one workflow", true, false),
                Self::tool("workflow_stop", "Idempotently stop one workflow", true, false),
            ],
        })
    }

    async fn handle_call_tool_request(
        &self,
        params: CallToolRequestParams,
        runtime: Arc<dyn McpServer>,
    ) -> Result<CallToolResult, CallToolError> {
        if !matches!(
            params.name.as_str(),
            "workflow_overview" | "workflow_events" | "workflow_start" | "workflow_stop"
        ) {
            return Err(CallToolError::unknown_tool(format!("Unknown tool: {}", params.name)));
        }
        Ok(Self::result(self.invoke(params, runtime).await))
    }
}
