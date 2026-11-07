// SPDX-License-Identifier: GPL-3.0-only
use async_trait::async_trait;
use rust_mcp_sdk::mcp_server::ServerHandler;
use rust_mcp_sdk::schema::{
    CallToolError, CallToolRequestParams, CallToolResult, ContentBlock, ListToolsResult, PaginatedRequestParams,
    RpcError, TextContent, Tool, ToolAnnotations, ToolInputSchema,
};
use rust_mcp_sdk::McpServer;
use serde_json::{json, Map, Value};
use sigbot_core::context::{identity::RequestIdentity, state::SigbotState};
use sigbot_core::modules::domain::handler::resource::ResourceHandler;
use sigbot_types::domain::{Domain, ResourceCommand, ResourceOperation};
use std::{collections::BTreeMap, sync::Arc};

pub struct ResourceMcpRegistry {
    state: Arc<SigbotState>,
}
impl ResourceMcpRegistry {
    pub fn new(state: Arc<SigbotState>) -> Self {
        Self { state }
    }
    fn tool(domain: Domain, operation: ResourceOperation) -> Tool {
        let action = serde_json::to_value(operation).unwrap().as_str().unwrap().to_owned();
        let property = |kind: &str| Map::from_iter([("type".into(), json!(kind))]);
        let mut properties = BTreeMap::from([("env".into(), property("string"))]);
        let mut required = vec!["env".into()];
        if matches!(
            operation,
            ResourceOperation::Get | ResourceOperation::Update | ResourceOperation::Delete
        ) {
            properties.insert("id".into(), property("string"));
            required.push("id".into());
        }
        if matches!(operation, ResourceOperation::Create | ResourceOperation::Update) {
            properties.insert("fields".into(), property("object"));
            required.push("fields".into());
        }
        if operation == ResourceOperation::List {
            properties.insert("limit".into(), property("integer"));
            properties.insert("offset".into(), property("integer"));
        }
        Tool {
            name: format!("{}_{action}", domain.as_str()),
            description: Some(format!("{action} tenant-scoped {} info", domain.as_str())),
            input_schema: ToolInputSchema::new(required, Some(properties), None),
            annotations: Some(ToolAnnotations {
                destructive_hint: Some(operation == ResourceOperation::Delete),
                idempotent_hint: Some(operation != ResourceOperation::Create),
                open_world_hint: Some(false),
                read_only_hint: Some(matches!(operation, ResourceOperation::List | ResourceOperation::Get)),
                title: None,
            }),
            execution: None,
            icons: Vec::new(),
            meta: None,
            output_schema: None,
            title: None,
        }
    }
    pub fn tools() -> Vec<Tool> {
        Domain::ALL
            .into_iter()
            .flat_map(|domain| {
                [
                    ResourceOperation::List,
                    ResourceOperation::Get,
                    ResourceOperation::Create,
                    ResourceOperation::Update,
                    ResourceOperation::Delete,
                ]
                .into_iter()
                .map(move |operation| Self::tool(domain, operation))
            })
            .collect()
    }
    async fn invoke(&self, params: CallToolRequestParams, runtime: Arc<dyn McpServer>) -> anyhow::Result<Value> {
        let auth = runtime
            .auth_info_cloned()
            .await
            .ok_or_else(|| anyhow::anyhow!("authenticated context required"))?;
        let access = auth
            .extra
            .as_ref()
            .and_then(|extra| extra.get("request_access"))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("verified request access required"))?;
        let identity = RequestIdentity::from_verified_value(access)?;
        let (domain, operation) = params
            .name
            .rsplit_once('_')
            .ok_or_else(|| anyhow::anyhow!("unknown resource tool"))?;
        let mut arguments = params.arguments.unwrap_or_default();
        anyhow::ensure!(
            !arguments.contains_key("domain") && !arguments.contains_key("operation"),
            "tool identity cannot be overridden"
        );
        arguments.insert("domain".into(), json!(domain));
        arguments.insert("operation".into(), json!(operation));
        let command: ResourceCommand = serde_json::from_value(Value::Object(arguments))?;
        ResourceHandler::execute(&self.state, &identity, command).await
    }
    fn result(result: anyhow::Result<Value>) -> CallToolResult {
        match result {
            Ok(value) => CallToolResult {
                content: vec![ContentBlock::from(TextContent::new(value.to_string(), None, None))],
                is_error: Some(false),
                meta: None,
                structured_content: value.as_object().cloned(),
            },
            Err(error) => {
                tracing::warn!(%error,"MCP domain operation failed");
                CallToolResult {
                    content: vec![ContentBlock::from(TextContent::new(
                        "domain operation rejected".into(),
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
impl ServerHandler for ResourceMcpRegistry {
    async fn handle_list_tools_request(
        &self,
        _params: Option<PaginatedRequestParams>,
        _runtime: Arc<dyn McpServer>,
    ) -> Result<ListToolsResult, RpcError> {
        Ok(ListToolsResult {
            meta: None,
            next_cursor: None,
            tools: Self::tools(),
        })
    }
    async fn handle_call_tool_request(
        &self,
        params: CallToolRequestParams,
        runtime: Arc<dyn McpServer>,
    ) -> Result<CallToolResult, CallToolError> {
        if !Self::tools().iter().any(|tool| tool.name == params.name) {
            return Err(CallToolError::unknown_tool(format!("Unknown tool: {}", params.name)));
        }
        Ok(Self::result(self.invoke(params, runtime).await))
    }
}
