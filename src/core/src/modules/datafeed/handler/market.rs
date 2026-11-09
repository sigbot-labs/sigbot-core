use crate::config::config::StorageProvider;
use crate::context::identity::RequestIdentity;
use crate::context::identity::ScopeQuery;
use crate::context::state::SigbotState;
use crate::modules::datafeed::storage::postgres::MarketPostgresRepository;
use crate::modules::datafeed::storage::sqlite::MarketSQLiteRepository;
use anyhow::ensure;
use anyhow::Result;
use serde_json::Value;

pub struct MarketHandler;

impl MarketHandler {
    pub async fn recent(
        state: &SigbotState,
        identity: &RequestIdentity,
        scope: &ScopeQuery,
        source: &str,
        category: &str,
    ) -> Result<Vec<Value>> {
        let tenant_id = scope.authorize(identity, "market")?;
        ensure!(
            matches!(
                category,
                "all" | "news" | "liquidation" | "funding" | "positioning" | "trade" | "quote"
            ),
            "unsupported market category"
        );
        match state.config.storage.provider {
            StorageProvider::Postgres => {
                MarketPostgresRepository::new(state.postgres_pool().await?)
                    .recent(tenant_id, &scope.env, source, category)
                    .await
            }
            StorageProvider::SQLite => {
                MarketSQLiteRepository::new(state.sqlite_pool().await?)
                    .recent(tenant_id, &scope.env, source, category)
                    .await
            }
        }
    }
}
