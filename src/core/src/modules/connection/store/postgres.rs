use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;

pub struct ConnectionPostgresRepository {
    pool: PgPool,
}
impl ConnectionPostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn list(&self, tenant_id: i64, env: &str, space_id: i64, kind: &str) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('id',id::text,'name',name,'provider',provider,'mode',transport,'status','configured','kind',configuration->>'kind','endpoint',configuration->>'endpoint','secret_ref',secret_ref,'updated_at',created_at) FROM connection WHERE tenant_id=$1 AND env=$2 AND space_id=$3 AND configuration->>'kind'=$4 ORDER BY id DESC LIMIT 100")
            .bind(tenant_id).bind(env).bind(space_id).bind(kind).fetch_all(&self.pool).await?)
    }
    pub async fn save(
        &self,
        tenant_id: i64,
        request: &sigbot_types::modules::connection::SaveConnection,
    ) -> Result<i64> {
        Ok(sqlx::query_scalar("INSERT INTO connection(env,tenant_id,space_id,name,provider,transport,configuration,secret_ref) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(env,tenant_id,space_id,name) DO UPDATE SET provider=excluded.provider,transport=excluded.transport,configuration=excluded.configuration,secret_ref=excluded.secret_ref,version=connection.version+1 RETURNING id")
            .bind(&request.env).bind(tenant_id).bind(request.space_id).bind(&request.name).bind(&request.provider).bind(&request.mode)
            .bind(serde_json::json!({"kind": request.kind,"endpoint": request.endpoint})).bind(&request.secret_ref).fetch_one(&self.pool).await?)
    }
}
