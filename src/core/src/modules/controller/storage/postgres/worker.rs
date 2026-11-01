use anyhow::{Context, Result};
use serde_json::Value;
use sigbot_types::controller::{
    SaveWorker, WorkerAuthConfig, WorkerConfiguration, WorkerInfo, WorkerProvider, WorkerStatus,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeMap;
use uuid::Uuid;

pub struct WorkerPostgresRepository {
    pool: PgPool,
}

impl WorkerPostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list(&self, tenant_id: Uuid, env: &str) -> Result<Vec<WorkerInfo>> {
        let rows = sqlx::query("SELECT * FROM ctl_worker WHERE tenant_id=$1 AND env=$2 ORDER BY priority,id")
            .bind(tenant_id)
            .bind(env)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(Self::decode).collect()
    }

    pub async fn save(&self, tenant_id: Uuid, actor: &str, request: &SaveWorker) -> Result<Uuid> {
        let auth = serde_json::to_value(&request.auth_config)?;
        let labels = serde_json::to_value(&request.labels)?;
        if let Some(id) = request.id {
            return sqlx::query_scalar(
                "UPDATE ctl_worker SET workflow_id=$4,name=$5,provider=$6,conn_url=$7,auth_config=$8,configuration=$9,middleware=$10,labels=$11,priority=$12,max_concurrent_workflows=$13,updated_at=now(),updated_by=$14 WHERE id=$1 AND tenant_id=$2 AND env=$3 RETURNING id",
            )
            .bind(id).bind(tenant_id).bind(&request.env)
            .bind(request.workflow_id).bind(&request.name).bind(request.provider.as_str())
            .bind(&request.conn_url).bind(auth).bind(serde_json::to_value(&request.configuration)?)
            .bind(serde_json::to_value(&request.middleware)?).bind(labels)
            .bind(request.priority).bind(request.max_concurrent_workflows).bind(actor)
            .fetch_one(&self.pool).await.map_err(Into::into);
        }
        sqlx::query_scalar(
            "INSERT INTO ctl_worker(env,tenant_id,workflow_id,name,provider,conn_url,auth_config,configuration,middleware,labels,priority,max_concurrent_workflows,created_by,updated_by) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$13) RETURNING id",
        )
        .bind(&request.env).bind(tenant_id).bind(request.workflow_id)
        .bind(&request.name).bind(request.provider.as_str()).bind(&request.conn_url).bind(auth)
        .bind(serde_json::to_value(&request.configuration)?).bind(serde_json::to_value(&request.middleware)?)
        .bind(labels).bind(request.priority)
        .bind(request.max_concurrent_workflows).bind(actor)
        .fetch_one(&self.pool).await.map_err(Into::into)
    }

    pub async fn delete(&self, tenant_id: Uuid, env: &str, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM ctl_worker w WHERE id=$1 AND tenant_id=$2 AND env=$3 AND NOT EXISTS(SELECT 1 FROM wf_run r WHERE r.worker_id=w.id AND r.state IN ('queued','running'))")
            .bind(id).bind(tenant_id).bind(env).execute(&self.pool).await?.rows_affected() == 1)
    }

    pub async fn status(
        &self,
        tenant_id: Uuid,
        env: &str,
        id: Uuid,
        status: WorkerStatus,
        actor: &str,
    ) -> Result<bool> {
        Ok(sqlx::query("UPDATE ctl_worker SET status=$4,heartbeat_at=CASE WHEN $4='online' THEN now() ELSE heartbeat_at END,updated_at=now(),updated_by=$5 WHERE id=$1 AND tenant_id=$2 AND env=$3")
            .bind(id).bind(tenant_id).bind(env).bind(status.as_str()).bind(actor)
            .execute(&self.pool).await?.rows_affected() == 1)
    }

    pub async fn resolve(
        transaction: &mut Transaction<'_, Postgres>,
        tenant_id: Uuid,
        env: &str,
        workflow_id: Uuid,
        assigned_id: Option<Uuid>,
    ) -> Result<WorkerInfo> {
        let row = sqlx::query(
            "SELECT w.* FROM ctl_worker w WHERE w.env=$1 AND w.tenant_id=$2 AND (w.id=$4 OR ($4 IS NULL AND w.status='online' AND (w.workflow_id IS NULL OR w.workflow_id=$3) AND (SELECT count(*) FROM wf_run r WHERE r.worker_id=w.id AND r.state IN ('queued','running')) < w.max_concurrent_workflows)) ORDER BY (w.id=$4) DESC,(w.workflow_id=$3) DESC,w.priority,w.id LIMIT 1 FOR UPDATE OF w SKIP LOCKED",
        )
        .bind(env).bind(tenant_id).bind(workflow_id).bind(assigned_id)
        .fetch_optional(&mut **transaction).await?
        .context("no online worker has capacity for this Workflow")?;
        Self::decode(&row)
    }

    fn decode(row: &sqlx::postgres::PgRow) -> Result<WorkerInfo> {
        let provider = serde_json::from_value::<WorkerProvider>(Value::String(row.try_get("provider")?))?;
        let status = serde_json::from_value::<WorkerStatus>(Value::String(row.try_get("status")?))?;
        let auth_config: WorkerAuthConfig = serde_json::from_value(row.try_get("auth_config")?)?;
        let configuration: WorkerConfiguration = serde_json::from_value(row.try_get("configuration")?)?;
        let labels: BTreeMap<String, String> = serde_json::from_value(row.try_get("labels")?)?;
        Ok(WorkerInfo {
            id: row.try_get("id")?,
            env: row.try_get("env")?,
            tenant_id: row.try_get("tenant_id")?,
            workflow_id: row.try_get("workflow_id")?,
            name: row.try_get("name")?,
            provider,
            conn_url: row.try_get("conn_url")?,
            auth_config,
            configuration,
            middleware: serde_json::from_value(row.try_get("middleware")?)?,
            labels,
            status,
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
