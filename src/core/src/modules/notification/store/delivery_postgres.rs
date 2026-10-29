use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;

pub struct NotificationDeliveryRepository {
    pool: PgPool,
}
impl NotificationDeliveryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn list(&self, tenant: i64, env: &str, space: i64, recipient: &str) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('run_id',d.run_id,'producer_node_id',d.producer_node_id,'seq',d.seq,'channel',d.channel,'title',COALESCE(d.payload->>'title',d.payload->>'event_type','执行通知'),'body',COALESCE(d.payload->>'body',d.payload->>'message',''),'kind',d.payload->>'kind','status',d.state,'event_time',d.available_at,'read',r.read_at IS NOT NULL) FROM notification_delivery d LEFT JOIN notification_read r USING(env,tenant_id,space_id,run_id,producer_node_id,seq,channel,recipient) WHERE d.tenant_id=$1 AND d.env=$2 AND d.space_id=$3 AND d.recipient=$4 AND d.channel='in_app' ORDER BY d.available_at DESC,d.run_id,d.seq DESC LIMIT 100")
            .bind(tenant).bind(env).bind(space).bind(recipient).fetch_all(&self.pool).await?)
    }
    pub async fn mark_read(
        &self,
        tenant: i64,
        env: &str,
        space: i64,
        recipient: &str,
        run: &str,
        seq: i64,
        producer_node_id: i64,
    ) -> Result<bool> {
        let result=sqlx::query("INSERT INTO notification_read(env,tenant_id,space_id,run_id,producer_node_id,seq,channel,recipient) SELECT env,tenant_id,space_id,run_id,producer_node_id,seq,channel,recipient FROM notification_delivery WHERE tenant_id=$1 AND env=$2 AND space_id=$3 AND recipient=$4 AND run_id=$5 AND seq=$6 AND producer_node_id=$7 AND channel='in_app' ON CONFLICT DO NOTHING")
            .bind(tenant).bind(env).bind(space).bind(recipient).bind(run).bind(seq).bind(producer_node_id).execute(&self.pool).await?;
        Ok(result.rows_affected() > 0)
    }
}
