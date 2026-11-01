use anyhow::Result;
use serde_json::Value;
use sqlx::SqlitePool;
use uuid::Uuid;

pub struct MarketSQLiteRepository {
    pool: SqlitePool,
}

impl MarketSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn recent(&self, tenant_id: Uuid, env: &str, source: &str, category: &str) -> Result<Vec<Value>> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT json_object('source',source,'source_event_id',source_event_id,'revision',revision,'category',event_type,'symbol',symbol,'value',json_extract(payload,'$.value'),'title',COALESCE(NULLIF(json_extract(payload,'$.title'),''),NULLIF(json_extract(payload,'$.text'),''),json_extract(payload,'$.content')),'url',json_extract(payload,'$.url'),'event_time',event_at,'available_time',available_at,'sequence',seq,'points',json_extract(payload,'$.points')) FROM mkt_event WHERE tenant_id=?1 AND env=?2 AND (?3='all' OR lower(source)=lower(?3)) AND (?4='all' OR event_type=?4) ORDER BY available_at DESC,source,seq DESC LIMIT 100")
            .bind(tenant_id.to_string()).bind(env).bind(source).bind(category).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(&row).map_err(Into::into))
            .collect()
    }
}
