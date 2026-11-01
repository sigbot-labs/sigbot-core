// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::config::config::PostgresStorageProperties;
use crate::storage::postgres::PostgresRepository;
use crate::storage::IGenericRepository;
use anyhow::{ensure, Context, Error, Ok};
use async_trait::async_trait;
use serde_json::{json, Value};
use sigbot_types::workflow::workflow::WorkflowInfo;
use sigbot_types::PageRequest;
use sigbot_types::PageResponse;
use sigbot_utils::types::GenericValue;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use uuid::Uuid;

pub struct WorkflowPostgresRepository {
    inner: PostgresRepository<WorkflowInfo>,
}

impl WorkflowPostgresRepository {
    pub async fn new(config: &PostgresStorageProperties) -> Result<Self, Error> {
        Ok(WorkflowPostgresRepository {
            inner: PostgresRepository::get_or_init(config).await?,
        })
    }

    pub async fn overview(pool: &PgPool, tenant: Uuid, env: &str) -> Result<Value, Error> {
        let total: i64 =
            sqlx::query_scalar("SELECT count(*) FROM wf_workflow WHERE tenant_id=$1 AND env=$2 AND del_flag=0")
                .bind(tenant)
                .bind(env)
                .fetch_one(pool)
                .await?;
        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM wf_workflow WHERE tenant_id=$1 AND env=$2 AND del_flag=0 AND status='RUNNING'",
        )
        .bind(tenant)
        .bind(env)
        .fetch_one(pool)
        .await?;
        let executions:i64=sqlx::query_scalar("SELECT count(*) FROM ord_execution e JOIN wlt_wallet w ON w.id=e.wallet_id WHERE w.tenant_id=$1 AND w.env=$2").bind(tenant).bind(env).fetch_one(pool).await?;
        let runs:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('run_id',r.run_id,'workflow_id',r.workflow_id::text,'workflow_name',w.name,'status',r.state,'event_time',r.started_at,'attempt_no',r.attempt_no) FROM wf_run r JOIN wf_workflow w ON w.id=r.workflow_id WHERE r.tenant_id=$1 AND r.env=$2 AND w.tenant_id=$1 AND w.env=$2 ORDER BY r.started_at DESC LIMIT 20").bind(tenant).bind(env).fetch_all(pool).await?;
        Ok(
            json!({"workflows_total":total,"active_workflows":active,"executions_count":executions,"equity":null,"realized_pnl":null,"equity_series":[],"recent_runs":runs,"updated_at":chrono::Utc::now()}),
        )
    }

    pub async fn events(pool: &PgPool, tenant: Uuid, env: &str, workflow_id: Uuid) -> Result<Vec<Value>, Error> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('run_id',run_id,'workflow_id',$3::text,'status',state,'event_time',received_at,'seq',seq,'attempt_no',attempt_no,'node_id',node_id,'trace_id',NULLIF(split_part(payload->>'traceparent','-',2),''),'span_id',NULLIF(split_part(payload->>'traceparent','-',3),''),'parent_span_id',payload->'payload'->>'parent_span_id','message',topic) FROM msg_inbox WHERE tenant_id=$1 AND env=$2 AND payload->>'workflow_id'=$3::text ORDER BY received_at DESC,node_id,seq DESC LIMIT 200")
            .bind(tenant).bind(env).bind(workflow_id.to_string()).fetch_all(pool).await?)
    }

    pub async fn request_run(
        pool: &PgPool,
        tenant: Uuid,
        env: &str,
        workflow_id: Uuid,
        actor: &str,
        start: bool,
    ) -> Result<Uuid, Error> {
        let mut transaction = pool.begin().await?;
        let row = sqlx::query("SELECT status,flow_json,properties FROM wf_workflow WHERE id=$1 AND tenant_id=$2 AND env=$3 AND del_flag=0 FOR UPDATE")
            .bind(workflow_id).bind(tenant).bind(env).fetch_optional(&mut *transaction).await?.context("workflow not found")?;
        let status: String = row.get("status");
        let active: Option<Uuid> = sqlx::query_scalar("SELECT run_id FROM wf_run WHERE workflow_id=$1 AND tenant_id=$2 AND env=$3 AND state IN ('queued','running') ORDER BY started_at DESC LIMIT 1")
            .bind(workflow_id).bind(tenant).bind(env).fetch_optional(&mut *transaction).await?;
        if start && matches!(status.as_str(), "PENDING" | "STARTING" | "RUNNING") {
            let run = active.context("active workflow has no run")?;
            transaction.commit().await?;
            return Ok(run);
        }
        let run_id = if start {
            ensure!(
                matches!(status.as_str(), "STOPPED" | "SUCCESS" | "FAILED") && active.is_none(),
                "workflow cannot start in current state"
            );
            let flow: Value = row.get("flow_json");
            let definition = serde_json::from_value(flow.clone())?;
            sigbot_types::workflow::deployment::WorkflowDeployment::validate(&definition, true)?;
            let properties: Option<Value> = row.get("properties");
            let mode = sigbot_types::workflow::deployment::WorkflowDeployment::execution_mode(
                &definition,
                properties.as_ref().and_then(|value| value.get("mode")),
            )?;
            ensure!(
                flow.get("nodes")
                    .and_then(Value::as_array)
                    .is_some_and(|nodes| !nodes.is_empty()),
                "workflow has no nodes"
            );
            let run = Uuid::now_v7();
            sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state,params) VALUES($1,$2,$3,$4,'queued',$5)")
                .bind(run).bind(workflow_id).bind(env).bind(tenant)
                .bind(json!({"flow_json":flow,"execution_mode":mode,"traceparent":common_telemetry::tracing_context::MessageTrace::child("workflow.start", "").traceparent(),"properties":properties})).execute(&mut *transaction).await?;
            run
        } else {
            ensure!(
                matches!(status.as_str(), "PENDING" | "STARTING" | "RUNNING" | "STOPPING"),
                "workflow is not active"
            );
            active.context("active workflow has no run")?
        };
        sqlx::query(
            "UPDATE wf_workflow SET status=$4,updated_by=$5,updated_at=now() WHERE id=$1 AND tenant_id=$2 AND env=$3",
        )
        .bind(workflow_id)
        .bind(tenant)
        .bind(env)
        .bind(if start { "PENDING" } else { "STOPPING" })
        .bind(actor)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(run_id)
    }
}

#[async_trait]
impl IGenericRepository<WorkflowInfo> for WorkflowPostgresRepository {
    async fn select(
        &self,
        workflow: WorkflowInfo,
        page: PageRequest,
    ) -> Result<(PageResponse, Vec<WorkflowInfo>), Error> {
        let tenant_id = workflow
            .tenant_id
            .ok_or_else(|| Error::msg("explicit workflow tenant scope required"))?;
        let id = workflow.base.id;
        let env = workflow.env;
        let name = workflow.name;
        let status = workflow.status.map(|value| value.as_str().to_owned());
        let predicate = "tenant_id=$1 AND ($2::uuid IS NULL OR id=$2) AND ($3::text IS NULL OR env=$3) AND ($4::text IS NULL OR name=$4) AND ($5::text IS NULL OR status=$5) AND del_flag=0";
        let count_sql = format!("SELECT count(*) FROM wf_workflow WHERE {predicate}");
        let count = sqlx::query_scalar::<_, i64>(&count_sql)
            .bind(tenant_id)
            .bind(id)
            .bind(&env)
            .bind(&name)
            .bind(&status)
            .fetch_one(self.inner.get_pool())
            .await?;
        let select_sql = format!("SELECT * FROM wf_workflow WHERE {predicate} ORDER BY updated_at LIMIT $6 OFFSET $7");
        let rows = sqlx::query_as::<_, WorkflowInfo>(&select_sql)
            .bind(tenant_id)
            .bind(id)
            .bind(env)
            .bind(name)
            .bind(status)
            .bind(page.get_limit() as i64)
            .bind(page.get_offset() as i64)
            .fetch_all(self.inner.get_pool())
            .await?;
        Ok((
            PageResponse::new(Some(count), Some(page.get_offset()), Some(page.get_limit())),
            rows,
        ))
    }

    async fn select_by(&self, _id: Uuid) -> Result<WorkflowInfo, Error> {
        Err(Error::msg("use select with explicit workflow tenant scope"))
    }

    async fn upsert(&self, mut workflow: WorkflowInfo) -> Result<Uuid, Error> {
        anyhow::ensure!(
            workflow.tenant_id.is_some() && workflow.env.as_deref().is_some_and(|value| !value.is_empty()),
            "bound workflow tenant and environment required"
        );
        let supplied_id = workflow.base.id;
        let actor = workflow.base.created_by.clone();
        if supplied_id.is_none() {
            workflow.base.pre_insert(actor).await;
        }
        let id = workflow.base.id.ok_or_else(|| Error::msg("workflow id required"))?;
        let flow = workflow.flow_info.as_ref().map(serde_json::to_value).transpose()?;
        let result = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO wf_workflow (id, name, status, flow_json, properties, secrets, description, created_at, updated_at, created_by, updated_by, del_flag, tenant_id, env)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
             ON CONFLICT (id) DO UPDATE SET name = excluded.name, status = excluded.status,
             flow_json = excluded.flow_json, properties = excluded.properties, secrets = excluded.secrets,
             description = excluded.description, updated_at = excluded.updated_at, updated_by = excluded.updated_by
             WHERE wf_workflow.del_flag = 0 AND wf_workflow.tenant_id = excluded.tenant_id AND wf_workflow.env = excluded.env RETURNING id")
            .bind(id).bind(&workflow.name).bind(workflow.status.as_ref().map(|value| value.as_str()))
            .bind(flow).bind(workflow.properties.as_ref().map(serde_json::to_value).transpose()?)
            .bind(workflow.secrets.as_ref().map(serde_json::to_value).transpose()?)
            .bind(&workflow.description).bind(workflow.base.created_at).bind(workflow.base.updated_at)
            .bind(&workflow.base.created_by).bind(&workflow.base.updated_by).bind(0_i16)
            .bind(workflow.tenant_id).bind(&workflow.env)
            .fetch_one(self.inner.get_pool()).await?;
        Ok(result)
    }

    async fn update(&self, mut workflow: WorkflowInfo) -> Result<Uuid, Error> {
        let id = workflow.base.id.ok_or_else(|| Error::msg("workflow id required"))?;
        anyhow::ensure!(
            workflow.tenant_id.is_some() && workflow.env.is_some(),
            "explicit workflow scope required"
        );
        let deleted = workflow.base.del_flag.unwrap_or(0);
        let actor = workflow.base.updated_by.clone();
        workflow.base.pre_update(actor).await;
        let flow = workflow.flow_info.as_ref().map(serde_json::to_value).transpose()?;
        let rows = sqlx::query(
            "UPDATE wf_workflow SET name = $1, status = $2, flow_json = $3,
             properties = $4, secrets = $5, description = $6, updated_at = $7, updated_by = $8, del_flag = $9
             WHERE id = $10 AND tenant_id = $11 AND env = $12 AND del_flag = 0 AND (status = $2 OR $2 NOT IN ('STOPPED','SUCCESS','FAILED'))",
        )
        .bind(&workflow.name)
        .bind(workflow.status.as_ref().map(|value| value.as_str()))
        .bind(flow)
        .bind(workflow.properties.as_ref().map(serde_json::to_value).transpose()?)
        .bind(workflow.secrets.as_ref().map(serde_json::to_value).transpose()?)
        .bind(&workflow.description)
        .bind(workflow.base.updated_at)
        .bind(&workflow.base.updated_by)
        .bind(i16::from(deleted))
        .bind(id)
        .bind(workflow.tenant_id)
        .bind(&workflow.env)
        .execute(self.inner.get_pool())
        .await?
        .rows_affected();
        anyhow::ensure!(rows == 1, "workflow missing or deleted");
        Ok(id)
    }

    async fn delete_all(&self) -> Result<u64, Error> {
        Err(Error::msg("unscoped workflow deletion is disabled"))
    }

    async fn delete_by_id(&self, _id: Uuid) -> Result<u64, Error> {
        Err(Error::msg("use a scoped workflow update to mark deletion"))
    }

    async fn select_by_sql(
        &self,
        _sql_template: &str,
        _params: &HashMap<String, GenericValue>,
    ) -> Result<Vec<WorkflowInfo>, Error> {
        Err(Error::msg("unscoped workflow SQL is disabled"))
    }
}
