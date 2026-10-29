//! Workflow lifecycle is committed with the immutable run configuration snapshot.
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sqlx::{PgPool, Row};

pub struct WorkflowRunRepository {
    pool: PgPool,
}
impl WorkflowRunRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn request(
        &self,
        tenant: i64,
        env: &str,
        space: i64,
        workflow_id: i64,
        actor: &str,
        start: bool,
    ) -> Result<String> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT status,flow_json,properties FROM s_workflow WHERE id=$1 AND tenant_id=$2 AND env=$3 AND space_id=$4 AND del_flag=0 FOR UPDATE")
            .bind(workflow_id).bind(tenant).bind(env).bind(space).fetch_optional(&mut *tx).await?.context("workflow not found")?;
        let status: String = row.get("status");
        let active: Option<String> = sqlx::query_scalar("SELECT run_id FROM workflow_run WHERE workflow_id=$1 AND tenant_id=$2 AND env=$3 AND space_id=$4 AND state IN ('queued','running') ORDER BY started_at DESC LIMIT 1")
            .bind(workflow_id).bind(tenant).bind(env).bind(space).fetch_optional(&mut *tx).await?;
        if start && matches!(status.as_str(), "PENDING" | "STARTING" | "RUNNING") {
            let run = active.context("active workflow has no run")?;
            tx.commit().await?;
            return Ok(run);
        }
        let run_id = if start {
            ensure!(
                matches!(status.as_str(), "STOPPED" | "SUCCESS" | "FAILED") && active.is_none(),
                "workflow cannot start in current state"
            );
            let flow: Value = row.get("flow_json");
            let definition = serde_json::from_value(flow.clone())?;
            sigbot_types::modules::workflow::deployment::WorkflowDeployment::validate(&definition, true)?;
            ensure!(
                flow.get("nodes")
                    .and_then(Value::as_array)
                    .is_some_and(|nodes| !nodes.is_empty()),
                "workflow has no nodes"
            );
            let run = uuid::Uuid::new_v4().to_string();
            sqlx::query("INSERT INTO workflow_run(run_id,workflow_id,env,tenant_id,space_id,state,params) VALUES($1,$2,$3,$4,$5,'queued',$6)")
                .bind(&run).bind(workflow_id).bind(env).bind(tenant).bind(space)
                .bind(json!({"flow_json":flow,"traceparent":common_telemetry::tracing_context::MessageTrace::child("workflow.start", "").traceparent(),"properties":row.get::<Option<Value>,_>("properties")})).execute(&mut *tx).await?;
            run
        } else {
            ensure!(
                matches!(status.as_str(), "PENDING" | "STARTING" | "RUNNING" | "STOPPING"),
                "workflow is not active"
            );
            active.context("active workflow has no run")?
        };
        sqlx::query("UPDATE s_workflow SET status=$5,updated_by=$6,updated_at=now() WHERE id=$1 AND tenant_id=$2 AND env=$3 AND space_id=$4")
            .bind(workflow_id).bind(tenant).bind(env).bind(space).bind(if start { "PENDING" } else { "STOPPING" }).bind(actor).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(run_id)
    }
}
