// SPDX-License-Identifier: GPL-3.0-only
use crate::config::config::StorageProvider;
use crate::context::{identity::RequestIdentity, state::SigbotState};
use crate::modules::controller::handler::worker::WorkerHandler;
use crate::storage::IGenericRepository;
use crate::storage::{DomainPool, DomainRepository};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sigbot_types::controller::SaveWorker;
use sigbot_types::domain::{Domain, ResourceCommand, ResourceOperation};
use sigbot_types::workflow::{deployment::WorkflowDeployment, workflow::WorkflowFlowInfo};
use uuid::Uuid;

pub struct ResourceHandler;

impl ResourceHandler {
    #[tracing::instrument(skip_all, fields(domain=command.domain.as_str(), operation=?command.operation))]
    pub async fn execute(state: &SigbotState, identity: &RequestIdentity, command: ResourceCommand) -> Result<Value> {
        let tenant: Uuid = identity.tenant_id().parse()?;
        let target = command.id.map(|id| id.to_string()).unwrap_or_else(|| "all".into());
        identity.require_resource(&command.env, command.domain.as_str(), &target)?;
        ensure!(
            tenant.get_version_num() == 7 && command.id.is_none_or(|id| id.get_version_num() == 7),
            "UUIDv7 identity required"
        );
        let pool = match state.config.storage.provider {
            StorageProvider::Postgres => DomainPool::Postgres(state.postgres_pool().await?),
            StorageProvider::SQLite => DomainPool::SQLite(state.sqlite_pool().await?),
        };
        Self::execute_scoped(
            DomainRepository::new(pool, command.domain, tenant, command.env.clone()),
            identity,
            command,
        )
        .await
    }

    pub async fn execute_scoped(
        repository: DomainRepository,
        identity: &RequestIdentity,
        command: ResourceCommand,
    ) -> Result<Value> {
        let tenant: Uuid = identity.tenant_id().parse()?;
        let target = command.id.map(|id| id.to_string()).unwrap_or_else(|| "all".into());
        identity.require_resource(&command.env, command.domain.as_str(), &target)?;
        ensure!(
            command.limit > 0 && command.limit <= 200,
            "limit must be between 1 and 200"
        );
        match command.operation {
            ResourceOperation::List => {
                let mut rows = repository.list(command.limit, command.offset).await?;
                rows.retain(|row| {
                    row["id"].as_str().is_some_and(|id| {
                        identity
                            .require_resource(&command.env, command.domain.as_str(), id)
                            .is_ok()
                    })
                });
                Ok(json!({"data":rows,"limit":command.limit,"offset":command.offset}))
            }
            ResourceOperation::Get => Ok(json!({"data":repository.get(command.id.context("id required")?).await?})),
            ResourceOperation::Delete => {
                ensure!(
                    repository.delete_by_id(command.id.context("id required")?).await? == 1,
                    "entity is missing or immutable"
                );
                Ok(json!({"deleted":true}))
            }
            ResourceOperation::Create | ResourceOperation::Update => {
                let update = command.operation == ResourceOperation::Update;
                ensure!(update || command.id.is_none(), "create must not supply an id");
                ensure!(!command.fields.is_empty(), "entity fields required");
                ensure!(
                    !command.fields.keys().any(|key| matches!(
                        key.as_str(),
                        "id" | "tenant_id"
                            | "env"
                            | "created_at"
                            | "updated_at"
                            | "created_by"
                            | "updated_by"
                            | "del_flag"
                            | "portfolio_version"
                            | "version"
                    )),
                    "identity and audit fields are server-owned"
                );
                let id = if command.domain == Domain::Tenant {
                    ensure!(command.id.is_none_or(|id| id == tenant), "tenant mismatch");
                    tenant
                } else if update {
                    command.id.context("id required")?
                } else {
                    Uuid::now_v7()
                };
                let mut fields = command.fields;
                let mut merged = if update {
                    repository.get(id).await?.context("entity not found")?
                } else {
                    json!({})
                };
                merged
                    .as_object_mut()
                    .context("entity object required")?
                    .extend(fields.clone());
                Self::validate(command.domain, &merged, tenant)?;
                if command.domain == Domain::Order {
                    let intent: sigbot_types::order::intent::TradeIntent =
                        serde_json::from_value(merged["intent"].clone())?;
                    let ledger = repository
                        .for_domain(Domain::Ledger)
                        .get(intent.wallet_id)
                        .await?
                        .context("owned ledger required")?;
                    ensure!(
                        ledger["account_id"] == json!(intent.account_id),
                        "order account/ledger mismatch"
                    );
                }
                if command.domain == Domain::Cluster {
                    let mut input = merged.as_object().unwrap().clone();
                    input.retain(|key, _| {
                        matches!(
                            key.as_str(),
                            "id" | "env"
                                | "workflow_id"
                                | "name"
                                | "provider"
                                | "conn_url"
                                | "auth_config"
                                | "configuration"
                                | "middleware"
                                | "labels"
                                | "priority"
                                | "max_concurrent_workflows"
                        )
                    });
                    input.insert("env".into(), json!(command.env));
                    let worker: SaveWorker = serde_json::from_value(Value::Object(input))?;
                    WorkerHandler::validate(&worker, tenant)?;
                    if !update {
                        fields.insert("created_by".into(), json!(identity.principal_id()));
                    }
                    fields.insert("updated_by".into(), json!(identity.principal_id()));
                }
                fields.insert("id".into(), json!(id));
                if !matches!(command.domain, Domain::Order | Domain::Ledger) {
                    if !update {
                        fields.insert("created_by".into(), json!(identity.principal_id()));
                    }
                    fields.insert("updated_by".into(), json!(identity.principal_id()));
                }
                if command.domain != Domain::Tenant {
                    fields.insert("tenant_id".into(), json!(tenant));
                    fields.insert("env".into(), json!(command.env));
                }
                fields.insert("updated_at".into(), json!(chrono::Utc::now().to_rfc3339()));
                let row = Value::Object(fields);
                if update {
                    repository.update(row).await?;
                } else {
                    repository.insert(row).await?;
                }
                Ok(json!({"id":id}))
            }
        }
    }

    fn validate(domain: Domain, value: &Value, tenant: Uuid) -> Result<()> {
        ensure!(value.to_string().len() <= 2 * 1024 * 1024, "entity exceeds 2MiB");
        WorkflowDeployment::validate_credentials(value)?;
        for field in ["configuration", "default_parameters", "properties", "intent"] {
            ensure!(
                value
                    .get(field)
                    .is_none_or(|value| value.is_null() || value.is_object()),
                "configuration fields must be objects"
            );
        }
        if domain == Domain::Strategy {
            ensure!(
                value.get("code").is_none_or(Value::is_string),
                "strategy code must be a string"
            );
        }
        if !matches!(domain, Domain::Order | Domain::Ledger) {
            ensure!(
                value["name"]
                    .as_str()
                    .is_some_and(|name| !name.trim().is_empty() && name.len() <= 128),
                "name required (1..128 characters)"
            );
        }
        if domain == Domain::Workflow {
            if let Some(properties) = value.get("properties").filter(|v| !v.is_null()) {
                let _: std::collections::HashMap<String, String> = serde_json::from_value(properties.clone())?;
            }
            ensure!(
                value
                    .get("status")
                    .is_none_or(|status| status == "STOPPED" || status == "SUCCESS" || status == "FAILED"),
                "workflow lifecycle state is controller-owned"
            );
            let flow: WorkflowFlowInfo = serde_json::from_value(
                value
                    .get("dag_config")
                    .cloned()
                    .unwrap_or_else(|| json!({"nodes":[],"connections":[]})),
            )?;
            WorkflowDeployment::validate(&flow, false)?;
        }
        if domain == Domain::Order {
            let intent: sigbot_types::order::intent::TradeIntent = serde_json::from_value(value["intent"].clone())?;
            intent.validate(chrono::Utc::now(), i64::MAX)?;
            ensure!(
                value["wallet_id"] == json!(intent.wallet_id)
                    && value["exchange"] == intent.exchange
                    && value["symbol"] == intent.symbol,
                "order fields must match its typed intent"
            );
            ensure!(value["state"] == "pending", "only pending orders may be authored");
            ensure!(
                value.get("prepared_submission").is_none_or(Value::is_null)
                    && value.get("exchange_order_id").is_none_or(Value::is_null),
                "venue execution fields are component-owned"
            );
        }
        if domain == Domain::Tenant {
            ensure!(tenant.get_version_num() == 7, "invalid tenant identity");
        }
        if let Some(secret) = value.get("secret_ref").filter(|v| !v.is_null()) {
            ensure!(
                secret
                    .as_str()
                    .is_some_and(|s| s.starts_with("secret://") && s.len() > 9),
                "secret_ref must reference central secret storage"
            );
        }
        Ok(())
    }
}
