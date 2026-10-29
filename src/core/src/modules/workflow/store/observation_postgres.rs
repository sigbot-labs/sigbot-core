//! Read projections only; the runtime remains the writer of runs and execution facts.
use anyhow::Result;
use serde_json::{json, Value};
use sqlx::PgPool;

pub struct WorkflowObservationRepository {
    pool: PgPool,
}
impl WorkflowObservationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn overview(&self, tenant: i64, env: &str, space: i64) -> Result<Value> {
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM s_workflow WHERE tenant_id=$1 AND env=$2 AND space_id=$3 AND del_flag=0",
        )
        .bind(tenant)
        .bind(env)
        .bind(space)
        .fetch_one(&self.pool)
        .await?;
        let active:i64=sqlx::query_scalar("SELECT count(*) FROM s_workflow WHERE tenant_id=$1 AND env=$2 AND space_id=$3 AND del_flag=0 AND status='RUNNING'").bind(tenant).bind(env).bind(space).fetch_one(&self.pool).await?;
        let executions:i64=sqlx::query_scalar("SELECT count(*) FROM order_execution e JOIN s_wallets w ON w.id=e.wallet_id WHERE w.tenant_id=$1 AND w.env=$2 AND w.space_id=$3").bind(tenant.to_string()).bind(env).bind(space).fetch_one(&self.pool).await?;
        let runs:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('run_id',r.run_id,'workflow_id',r.workflow_id::text,'workflow_name',w.name,'status',r.state,'event_time',r.started_at,'attempt_no',r.attempt_no) FROM workflow_run r JOIN s_workflow w ON w.id=r.workflow_id WHERE r.tenant_id=$1 AND r.env=$2 AND r.space_id=$3 AND w.tenant_id=$1 AND w.env=$2 AND w.space_id=$3 ORDER BY r.started_at DESC LIMIT 20").bind(tenant).bind(env).bind(space).fetch_all(&self.pool).await?;
        // Valuation needs a quote asset and synchronized prices. Never sum heterogeneous assets.
        Ok(
            json!({"workflows_total":total,"active_workflows":active,"executions_count":executions,"equity":null,"realized_pnl":null,"equity_series":[],"recent_runs":runs,"updated_at":chrono::Utc::now()}),
        )
    }
    pub async fn events(&self, tenant: i64, env: &str, space: i64, workflow_id: i64) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('run_id',run_id,'workflow_id',$4::text,'status',state,'event_time',received_at,'seq',seq,'attempt_no',attempt_no,'node_id',node_id,'trace_id',NULLIF(split_part(payload->>'traceparent','-',2),''),'span_id',NULLIF(split_part(payload->>'traceparent','-',3),''),'parent_span_id',payload->'payload'->>'parent_span_id','message',topic) FROM messager_inbox WHERE tenant_id=$1 AND env=$2 AND space_id=$3 AND payload->>'workflow_id'=$4::text ORDER BY received_at DESC,node_id,seq DESC LIMIT 200")
            .bind(tenant).bind(env).bind(space).bind(workflow_id.to_string()).fetch_all(&self.pool).await?)
    }
}
