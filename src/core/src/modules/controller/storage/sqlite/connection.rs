use anyhow::Result;
use serde_json::Value;
use sigbot_types::controller::SaveConnection;
use sqlx::SqlitePool;
use uuid::Uuid;

pub struct ConnectionSQLiteRepository {
    pool: SqlitePool,
}

impl ConnectionSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn list(&self, tenant_id: Uuid, env: &str, kind: &str) -> Result<Vec<Value>> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT json_object('id',CAST(id AS TEXT),'name',name,'provider',provider,'mode',transport,'status','configured','kind',json_extract(configuration,'$.kind'),'endpoint',json_extract(configuration,'$.endpoint'),'secret_ref',secret_ref,'updated_at',created_at) FROM cfg_connection WHERE tenant_id=?1 AND env=?2 AND json_extract(configuration,'$.kind')=?3 ORDER BY id DESC LIMIT 100")
            .bind(tenant_id.to_string()).bind(env).bind(kind).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(&row).map_err(Into::into))
            .collect()
    }

    pub async fn save(&self, tenant_id: Uuid, request: &SaveConnection) -> Result<Uuid> {
        let id: String = sqlx::query_scalar("INSERT INTO cfg_connection(id,env,tenant_id,name,provider,transport,configuration,secret_ref) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(env,tenant_id,name) DO UPDATE SET provider=excluded.provider,transport=excluded.transport,configuration=excluded.configuration,secret_ref=excluded.secret_ref,version=cfg_connection.version+1 RETURNING id")
            .bind(Uuid::now_v7().to_string()).bind(&request.env).bind(tenant_id.to_string()).bind(&request.name)
            .bind(&request.provider).bind(&request.mode)
            .bind(serde_json::json!({"kind":request.kind,"endpoint":request.endpoint}).to_string())
            .bind(&request.secret_ref).fetch_one(&self.pool).await?;
        Ok(Uuid::parse_str(&id)?)
    }
}
