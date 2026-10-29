use crate::{
    context::{
        identity::{RequestIdentity, ScopeQuery},
        state::SigbotState,
    },
    modules::connection::store::postgres::ConnectionPostgresRepository,
};
use anyhow::{ensure, Result};
use serde_json::Value;

pub use sigbot_types::modules::connection::SaveConnection;

pub struct ConnectionHandler;
impl ConnectionHandler {
    pub async fn list(
        state: &SigbotState,
        identity: &RequestIdentity,
        scope: &ScopeQuery,
        kind: &str,
    ) -> Result<Vec<Value>> {
        let tenant = scope.authorize(identity, "connection")?;
        ensure!(matches!(kind, "datafeed" | "exchange"), "invalid connection kind");
        let repository = ConnectionPostgresRepository::new(state.postgres_pool().await?);
        let mut rows = repository.list(tenant, &scope.env, scope.space_id, kind).await?;
        rows.retain(|row| {
            identity
                .require_resource(
                    &scope.env,
                    scope.space_id,
                    "connection",
                    row["id"].as_str().unwrap_or(""),
                )
                .is_ok()
        });
        Ok(rows)
    }
    pub async fn save(state: &SigbotState, identity: &RequestIdentity, request: SaveConnection) -> Result<i64> {
        identity.require_resource(&request.env, request.space_id, "connection", "all")?;
        ensure!(
            !request.name.trim().is_empty() && request.name.len() <= 128,
            "invalid connection name"
        );
        ensure!(
            request.secret_ref.starts_with("secret://") && request.secret_ref.len() > 9,
            "Secret reference required"
        );
        ensure!(
            matches!(request.kind.as_str(), "datafeed" | "exchange"),
            "invalid connection kind"
        );
        if request.kind == "datafeed" {
            ensure!(
                matches!(request.mode.as_str(), "MCP" | "A2A" | "API_KEY"),
                "invalid datafeed transport"
            );
        } else {
            ensure!(
                matches!(request.provider.as_str(), "Binance" | "IBKR" | "Polymarket"),
                "unsupported exchange"
            );
            ensure!(
                request.mode.to_lowercase() == request.env
                    && matches!(request.mode.as_str(), "Live" | "Paper" | "Simulated"),
                "exchange mode must match environment"
            );
        }
        let endpoint = url::Url::parse(&request.endpoint)?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https")
                && endpoint.username().is_empty()
                && endpoint.password().is_none(),
            "invalid endpoint"
        );
        ensure!(
            endpoint.query().is_none(),
            "credentials and query parameters belong in server configuration"
        );
        ConnectionPostgresRepository::new(state.postgres_pool().await?)
            .save(identity.tenant_id().parse()?, &request)
            .await
    }
}
