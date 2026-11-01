// SPDX-License-Identifier: GPL-3.0-only

use crate::notifier::NotificationRead;
use crate::worker::WorkerStatusRequest;
use sigbot_core::config::config::AppConfig;
use sigbot_types::controller::SaveConnection;
use sigbot_types::controller::{
    MiddlewareDeployment, MiddlewareResources, SaveWorker, TenantMiddlewareConfiguration, WorkerAuthConfig,
    WorkerConfiguration, WorkerInfo, WorkerProvider, WorkerStatus,
};
use sigbot_types::llm::knowledge::KnowledgeUploadInfo;
use sigbot_types::workflow::workflow::{
    DeleteWorkflowRequest, DeleteWorkflowResponse, QueryWorkflowResponse, SaveWorkflowRequest, SaveWorkflowResponse,
    WorkflowFlowInfo, WorkflowInfo,
};
use utoipa::openapi::external_docs::ExternalDocsBuilder;
use utoipa::openapi::info::{ContactBuilder, InfoBuilder, LicenseBuilder};
use utoipa::openapi::path::{HttpMethod, Operation, OperationBuilder};
use utoipa::openapi::schema::ComponentsBuilder;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme};
use utoipa::openapi::{OpenApi, OpenApiBuilder, PathItem, Paths, Response, Tag};
use utoipa_swagger_ui::SwaggerUi;

pub struct SwaggerHttpApi;

impl SwaggerHttpApi {
    pub fn routes(config: &AppConfig) -> SwaggerUi {
        let ui_path = Self::join_context_path(config, &config.swagger.ui_path);
        let document_path = Self::join_context_path(config, &config.swagger.openapi_url);
        SwaggerUi::new(ui_path).url(document_path, Self::document(config))
    }

    pub fn document(config: &AppConfig) -> OpenApi {
        let mut paths = Paths::new();
        for (path, method, tag, id, summary) in Self::operations() {
            let path = Self::join_context_path(config, path);
            let item = PathItem::new(method, Self::operation(tag, id, summary));
            paths
                .paths
                .entry(path)
                .and_modify(|current| current.merge_operations(item.clone()))
                .or_insert(item);
        }
        OpenApiBuilder::new()
            .info(Self::info())
            .paths(paths)
            .components(Some(Self::components()))
            .security(Some(vec![SecurityRequirement::new(
                "authguard_bearer",
                Vec::<String>::new(),
            )]))
            .tags(Some(
                [
                    "Connections",
                    "Knowledge",
                    "Market",
                    "Notifications",
                    "Workers",
                    "Workflows",
                ]
                .into_iter()
                .map(Tag::new),
            ))
            .external_docs(Some(
                ExternalDocsBuilder::new()
                    .url("https://github.com/sigbot-labs/sigbot-core/tree/main/docs")
                    .description(Some("Sigbot architecture and operations documentation"))
                    .build(),
            ))
            .build()
    }

    fn info() -> utoipa::openapi::Info {
        InfoBuilder::new()
            .title("Sigbot Core API")
            .version(env!("CARGO_PKG_VERSION"))
            .description(Some(
                "Tenant-scoped APIs for Workflow authoring, compute workers, connections, market observations and notifications. Authentication and authorization are delegated to Authguard.",
            ))
            .terms_of_service(Some("https://github.com/sigbot-labs/sigbot-core"))
            .contact(Some(
                ContactBuilder::new()
                    .name(Some("Sigbot Labs"))
                    .url(Some("https://github.com/sigbot-labs"))
                    .email(Some("opensource@sigbot.org"))
                    .build(),
            ))
            .license(Some(
                LicenseBuilder::new()
                    .name("GNU General Public License v3.0")
                    .identifier(Some("GPL-3.0-only"))
                    .build(),
            ))
            .build()
    }

    fn components() -> utoipa::openapi::Components {
        ComponentsBuilder::new()
            .schema_from::<SaveConnection>()
            .schema_from::<KnowledgeUploadInfo>()
            .schema_from::<NotificationRead>()
            .schema_from::<WorkflowInfo>()
            .schema_from::<WorkflowFlowInfo>()
            .schema_from::<SaveWorkflowRequest>()
            .schema_from::<SaveWorkflowResponse>()
            .schema_from::<QueryWorkflowResponse>()
            .schema_from::<DeleteWorkflowRequest>()
            .schema_from::<DeleteWorkflowResponse>()
            .schema_from::<WorkerProvider>()
            .schema_from::<WorkerStatus>()
            .schema_from::<WorkerAuthConfig>()
            .schema_from::<WorkerConfiguration>()
            .schema_from::<SaveWorker>()
            .schema_from::<WorkerInfo>()
            .schema_from::<WorkerStatusRequest>()
            .schema_from::<MiddlewareResources>()
            .schema_from::<MiddlewareDeployment>()
            .schema_from::<TenantMiddlewareConfiguration>()
            .security_scheme(
                "authguard_bearer",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .description(Some("Authguard-issued bearer token"))
                        .build(),
                ),
            )
            .build()
    }

    fn operation(tag: &'static str, id: &'static str, summary: &'static str) -> Operation {
        OperationBuilder::new()
            .operation_id(Some(id))
            .tag(tag)
            .summary(Some(summary))
            .response("200", Response::new("Operation completed"))
            .response("400", Response::new("Malformed or invalid request"))
            .response("401", Response::new("Missing or invalid Authguard identity"))
            .response("403", Response::new("Resource scope denied"))
            .response("409", Response::new("Resource state conflict"))
            .response("503", Response::new("Required provider unavailable"))
            .build()
    }

    fn operations() -> [(&'static str, HttpMethod, &'static str, &'static str, &'static str); 19] {
        [
            (
                "/api/v1/connections",
                HttpMethod::Get,
                "Connections",
                "listConnections",
                "List scoped connections",
            ),
            (
                "/api/v1/connections",
                HttpMethod::Post,
                "Connections",
                "saveConnection",
                "Create or update a scoped connection",
            ),
            (
                "/api/v1/knowledge/upload",
                HttpMethod::Post,
                "Knowledge",
                "uploadKnowledge",
                "Upload a knowledge document",
            ),
            (
                "/api/v1/market",
                HttpMethod::Get,
                "Market",
                "listMarket",
                "List recent market observations",
            ),
            (
                "/api/v1/notifications",
                HttpMethod::Get,
                "Notifications",
                "listNotifications",
                "List in-app notifications",
            ),
            (
                "/api/v1/notifications/read",
                HttpMethod::Post,
                "Notifications",
                "readNotification",
                "Mark a notification as read",
            ),
            (
                "/api/v1/workers",
                HttpMethod::Get,
                "Workers",
                "listWorkers",
                "List remote compute workers",
            ),
            (
                "/api/v1/workers/defaults",
                HttpMethod::Get,
                "Workers",
                "getWorkerDefaults",
                "Get controller-approved tenant middleware defaults",
            ),
            (
                "/api/v1/workers",
                HttpMethod::Post,
                "Workers",
                "saveWorker",
                "Create or update a remote compute worker",
            ),
            (
                "/api/v1/workers/{id}",
                HttpMethod::Delete,
                "Workers",
                "deleteWorker",
                "Delete an idle compute worker",
            ),
            (
                "/api/v1/workers/{id}/status",
                HttpMethod::Post,
                "Workers",
                "setWorkerStatus",
                "Update worker availability",
            ),
            (
                "/api/v1/workflows",
                HttpMethod::Get,
                "Workflows",
                "listWorkflows",
                "List Workflows",
            ),
            (
                "/api/v1/workflows",
                HttpMethod::Post,
                "Workflows",
                "saveWorkflow",
                "Create or update a Workflow",
            ),
            (
                "/api/v1/workflows/{id}",
                HttpMethod::Get,
                "Workflows",
                "getWorkflow",
                "Get a Workflow",
            ),
            (
                "/api/v1/workflows/{id}",
                HttpMethod::Delete,
                "Workflows",
                "deleteWorkflow",
                "Delete a Workflow",
            ),
            (
                "/api/v1/workflows/{id}/start",
                HttpMethod::Post,
                "Workflows",
                "startWorkflow",
                "Start a Workflow run",
            ),
            (
                "/api/v1/workflows/{id}/stop",
                HttpMethod::Post,
                "Workflows",
                "stopWorkflow",
                "Stop a Workflow run",
            ),
            (
                "/api/v1/overview",
                HttpMethod::Get,
                "Workflows",
                "getOverview",
                "Get recent Workflow metrics",
            ),
            (
                "/api/v1/workflows/{id}/events",
                HttpMethod::Get,
                "Workflows",
                "listWorkflowEvents",
                "List Workflow run events and spans",
            ),
        ]
    }

    fn join_context_path(config: &AppConfig, path: &str) -> String {
        if url::Url::parse(path)
            .map(|uri| matches!(uri.scheme().to_ascii_lowercase().as_str(), "http" | "https"))
            .unwrap_or(false)
        {
            return path.to_owned();
        }
        match config.server.context_path.as_deref() {
            Some("/") | None => path.to_owned(),
            Some(prefix) => format!("{prefix}{path}"),
        }
    }
}
