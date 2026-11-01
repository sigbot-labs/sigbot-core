use anyhow::Result;
use serde_json::Value;
use sigbot_types::controller::{SaveWorker, WorkerInfo, WorkerStatus};
use sqlx::{Row, SqlitePool};
use std::collections::BTreeMap;
use uuid::Uuid;

pub struct WorkerSQLiteRepository {
    pool: SqlitePool,
}

impl WorkerSQLiteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn list(&self, tenant_id: Uuid, env: &str) -> Result<Vec<WorkerInfo>> {
        let rows = sqlx::query("SELECT * FROM ctl_worker WHERE tenant_id=?1 AND env=?2 ORDER BY priority,id")
            .bind(tenant_id.to_string())
            .bind(env)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(Self::decode).collect()
    }

    pub async fn save(&self, tenant_id: Uuid, actor: &str, request: &SaveWorker) -> Result<Uuid> {
        let auth = serde_json::to_string(&request.auth_config)?;
        let config = serde_json::to_string(&request.configuration)?;
        let middleware = serde_json::to_string(&request.middleware)?;
        let labels = serde_json::to_string(&request.labels)?;
        if let Some(id) = request.id {
            let result: String = sqlx::query_scalar("UPDATE ctl_worker SET workflow_id=?4,name=?5,provider=?6,conn_url=?7,auth_config=?8,configuration=?9,middleware=?10,labels=?11,priority=?12,max_concurrent_workflows=?13,updated_at=CURRENT_TIMESTAMP,updated_by=?14 WHERE id=?1 AND tenant_id=?2 AND env=?3 RETURNING id")
                .bind(id.to_string()).bind(tenant_id.to_string()).bind(&request.env).bind(request.workflow_id.map(|value| value.to_string()))
                .bind(&request.name).bind(request.provider.as_str()).bind(&request.conn_url).bind(auth)
                .bind(config).bind(middleware).bind(labels).bind(request.priority).bind(request.max_concurrent_workflows).bind(actor)
                .fetch_one(&self.pool).await?;
            return Ok(Uuid::parse_str(&result)?);
        }
        let result: String = sqlx::query_scalar("INSERT INTO ctl_worker(id,env,tenant_id,workflow_id,name,provider,conn_url,auth_config,configuration,middleware,labels,priority,max_concurrent_workflows,created_by,updated_by) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?14) RETURNING id")
            .bind(Uuid::now_v7().to_string()).bind(&request.env).bind(tenant_id.to_string()).bind(request.workflow_id.map(|value| value.to_string()))
            .bind(&request.name).bind(request.provider.as_str()).bind(&request.conn_url).bind(auth)
            .bind(config).bind(middleware).bind(labels).bind(request.priority).bind(request.max_concurrent_workflows).bind(actor)
            .fetch_one(&self.pool).await?;
        Ok(Uuid::parse_str(&result)?)
    }

    pub async fn delete(&self, tenant_id: Uuid, env: &str, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM ctl_worker WHERE id=?1 AND tenant_id=?2 AND env=?3 AND NOT EXISTS(SELECT 1 FROM wf_run WHERE worker_id=?1 AND state IN ('queued','running'))")
            .bind(id.to_string()).bind(tenant_id.to_string()).bind(env).execute(&self.pool).await?.rows_affected() == 1)
    }

    pub async fn status(
        &self,
        tenant_id: Uuid,
        env: &str,
        id: Uuid,
        status: WorkerStatus,
        actor: &str,
    ) -> Result<bool> {
        Ok(sqlx::query("UPDATE ctl_worker SET status=?4,heartbeat_at=CASE WHEN ?4='online' THEN CURRENT_TIMESTAMP ELSE heartbeat_at END,updated_at=CURRENT_TIMESTAMP,updated_by=?5 WHERE id=?1 AND tenant_id=?2 AND env=?3")
            .bind(id.to_string()).bind(tenant_id.to_string()).bind(env).bind(status.as_str()).bind(actor)
            .execute(&self.pool).await?.rows_affected() == 1)
    }

    fn decode(row: &sqlx::sqlite::SqliteRow) -> Result<WorkerInfo> {
        Ok(WorkerInfo {
            id: Uuid::parse_str(&row.try_get::<String, _>("id")?)?,
            env: row.try_get("env")?,
            tenant_id: Uuid::parse_str(&row.try_get::<String, _>("tenant_id")?)?,
            workflow_id: row
                .try_get::<Option<String>, _>("workflow_id")?
                .map(|value| Uuid::parse_str(&value))
                .transpose()?,
            name: row.try_get("name")?,
            provider: serde_json::from_value(Value::String(row.try_get("provider")?))?,
            conn_url: row.try_get("conn_url")?,
            auth_config: serde_json::from_str(&row.try_get::<String, _>("auth_config")?)?,
            configuration: serde_json::from_str(&row.try_get::<String, _>("configuration")?)?,
            middleware: serde_json::from_str(&row.try_get::<String, _>("middleware")?)?,
            labels: serde_json::from_str::<BTreeMap<String, String>>(&row.try_get::<String, _>("labels")?)?,
            status: serde_json::from_value(Value::String(row.try_get("status")?))?,
            priority: row.try_get("priority")?,
            max_concurrent_workflows: row.try_get("max_concurrent_workflows")?,
            heartbeat_at: row.try_get("heartbeat_at")?,
            created_at: row.try_get("created_at")?,
            created_by: row.try_get("created_by")?,
            updated_at: row.try_get("updated_at")?,
            updated_by: row.try_get("updated_by")?,
        })
    }
}
