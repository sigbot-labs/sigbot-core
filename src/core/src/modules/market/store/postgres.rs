use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;

pub struct MarketPostgresRepository {
    pool: PgPool,
}
impl MarketPostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn recent(
        &self,
        tenant_id: i64,
        env: &str,
        space_id: i64,
        source: &str,
        category: &str,
    ) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('source',source,'source_event_id',source_event_id,'revision',revision,'category',event_type,'symbol',symbol,'value',payload->>'value','title',COALESCE(NULLIF(payload->>'title',''),NULLIF(payload->>'text',''),payload->>'content'),'url',payload->>'url','event_time',event_at,'available_time',available_at,'sequence',seq,'points',payload->'points') FROM market_event WHERE tenant_id=$1 AND env=$2 AND space_id=$3 AND ($4='all' OR lower(source)=lower($4)) AND ($5='all' OR event_type=$5) ORDER BY available_at DESC,source,seq DESC LIMIT 100")
            .bind(tenant_id).bind(env).bind(space_id).bind(source).bind(category).fetch_all(&self.pool).await?)
    }
}
