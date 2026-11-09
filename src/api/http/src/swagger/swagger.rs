// SPDX-License-Identifier: GPL-3.0-only

use sigbot_core::config::config::AppConfig;
use sigbot_types::domain::{Domain, ResourceCommand, ResourceOperation};
use utoipa::openapi::external_docs::ExternalDocsBuilder;
use utoipa::openapi::info::{ContactBuilder, InfoBuilder, LicenseBuilder};
use utoipa::openapi::path::{HttpMethod, Operation, OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::schema::{ComponentsBuilder, ObjectBuilder, Type};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme};
use utoipa::openapi::{ContentBuilder, Required};
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
            let path = Self::join_context_path(config, &path);
            let item = PathItem::new(method, Self::operation(tag, &id, &summary));
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
            .tags(Some(Domain::ALL.into_iter().map(|domain| Tag::new(domain.as_str()))))
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
                "Tenant-scoped CRUD for tenant, cluster, datasource, exchange, strategy, order, ledger, notifier, exporter and workflow entities. Authentication and authorization are delegated to Authguard.",
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
            .schema_from::<ResourceCommand>()
            .schema_from::<Domain>()
            .schema_from::<ResourceOperation>()
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

    fn operation(tag: &str, id: &str, summary: &str) -> Operation {
        let mut operation = OperationBuilder::new()
            .operation_id(Some(id))
            .tag(tag)
            .summary(Some(summary))
            .response("200", Response::new("Operation completed"))
            .response("400", Response::new("Malformed or invalid request"))
            .response("401", Response::new("Missing or invalid Authguard identity"))
            .response("403", Response::new("Resource scope denied"))
            .response("409", Response::new("Resource state conflict"))
            .response("503", Response::new("Required provider unavailable"))
            .parameter(
                ParameterBuilder::new()
                    .name("env")
                    .parameter_in(ParameterIn::Query)
                    .required(Required::True)
                    .schema(Some(ObjectBuilder::new().schema_type(Type::String)))
                    .build(),
            );
        if !id.ends_with("_list") && !id.ends_with("_create") {
            operation = operation.parameter(
                ParameterBuilder::new()
                    .name("id")
                    .parameter_in(ParameterIn::Path)
                    .required(Required::True)
                    .schema(Some(ObjectBuilder::new().schema_type(Type::String)))
                    .build(),
            );
        }
        if id.ends_with("_create") || id.ends_with("_update") {
            operation = operation
                .request_body(Some(
                    RequestBodyBuilder::new()
                        .description(Some(
                            "Entity fields; tenant and audit identity are server-owned. Unknown fields are rejected.",
                        ))
                        .required(Some(Required::True))
                        .content(
                            "application/json",
                            ContentBuilder::new().schema(Some(ObjectBuilder::new())).build(),
                        )
                        .build(),
                ))
                .response("201", Response::new("Entity created"));
        }
        operation.build()
    }

    fn operations() -> Vec<(String, HttpMethod, &'static str, String, String)> {
        let mut operations = Vec::new();
        for domain in Domain::ALL {
            let path = format!("/api/v1/{}", domain.collection());
            for (method, action, item) in [
                (HttpMethod::Get, "list", false),
                (HttpMethod::Post, "create", false),
                (HttpMethod::Get, "get", true),
                (HttpMethod::Put, "update", true),
                (HttpMethod::Delete, "delete", true),
            ] {
                operations.push((
                    if item { format!("{path}/{{id}}") } else { path.clone() },
                    method,
                    domain.as_str(),
                    format!("{}_{}", domain.as_str(), action),
                    format!("{action} {} info", domain.as_str()),
                ));
            }
        }
        operations
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
