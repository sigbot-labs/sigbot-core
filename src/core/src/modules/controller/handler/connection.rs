use crate::config::config::StorageProvider;
use crate::context::identity::RequestIdentity;
use crate::context::identity::ScopeQuery;
use crate::context::state::SigbotState;
use crate::modules::controller::storage::postgres::ConnectionPostgresRepository;
use crate::modules::controller::storage::sqlite::ConnectionSQLiteRepository;
use anyhow::ensure;
use anyhow::Result;
use serde_json::Value;
use uuid::Uuid;

pub use sigbot_types::controller::SaveConnection;

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
        let mut rows = match state.config.storage.provider {
            StorageProvider::Postgres => {
                ConnectionPostgresRepository::new(state.postgres_pool().await?)
                    .list(tenant, &scope.env, kind)
                    .await?
            }
            StorageProvider::SQLite => {
                ConnectionSQLiteRepository::new(state.sqlite_pool().await?)
                    .list(tenant, &scope.env, kind)
                    .await?
            }
        };
        rows.retain(|row| {
            identity
                .require_resource(&scope.env, "connection", row["id"].as_str().unwrap_or(""))
                .is_ok()
        });
        Ok(rows)
    }
    pub async fn save(state: &SigbotState, identity: &RequestIdentity, request: SaveConnection) -> Result<Uuid> {
        identity.require_resource(&request.env, "connection", "all")?;
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
                matches!(request.mode.as_str(), "live" | "paper" | "backtest"),
                "exchange mode must be live, paper or backtest"
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
        let tenant_id = identity.tenant_id().parse()?;
        match state.config.storage.provider {
            StorageProvider::Postgres => {
                ConnectionPostgresRepository::new(state.postgres_pool().await?)
                    .save(tenant_id, &request)
                    .await
            }
            StorageProvider::SQLite => {
                ConnectionSQLiteRepository::new(state.sqlite_pool().await?)
                    .save(tenant_id, &request)
                    .await
            }
        }
    }
}
