use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

pub struct ConnectionPostgresRepository {
    pool: PgPool,
}
impl ConnectionPostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn list(&self, tenant_id: Uuid, env: &str, kind: &str) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('id',id::text,'name',name,'provider',provider,'mode',transport,'status','configured','kind',configuration->>'kind','endpoint',configuration->>'endpoint','secret_ref',secret_ref,'updated_at',created_at) FROM cfg_connection WHERE tenant_id=$1 AND env=$2 AND configuration->>'kind'=$3 ORDER BY id DESC LIMIT 100")
            .bind(tenant_id).bind(env).bind(kind).fetch_all(&self.pool).await?)
    }
    pub async fn save(&self, tenant_id: Uuid, request: &sigbot_types::controller::SaveConnection) -> Result<Uuid> {
        Ok(sqlx::query_scalar("INSERT INTO cfg_connection(env,tenant_id,name,provider,transport,configuration,secret_ref) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(env,tenant_id,name) DO UPDATE SET provider=excluded.provider,transport=excluded.transport,configuration=excluded.configuration,secret_ref=excluded.secret_ref,version=cfg_connection.version+1 RETURNING id")
            .bind(&request.env).bind(tenant_id).bind(&request.name).bind(&request.provider).bind(&request.mode)
            .bind(serde_json::json!({"kind": request.kind,"endpoint": request.endpoint})).bind(&request.secret_ref).fetch_one(&self.pool).await?)
    }
}
