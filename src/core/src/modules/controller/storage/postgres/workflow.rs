// SPDX-License-Identifier: GPL-3.0-only

use crate::context::state::SigbotState;
use anyhow::Context;
use anyhow::Result;
use serde_json::Value;
use sigbot_types::controller::WorkerInfo;
use sigbot_types::messager::topic::MessageScope;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

#[derive(Clone)]
pub struct ActiveWorkflow {
    pub id: Uuid,
    pub scope: MessageScope,
}

pub struct WorkerClaim<'a> {
    pub run_id: Uuid,
    pub parameters: Value,
    pub stopping: bool,
    pub worker: WorkerInfo,
    workflow_id: Uuid,
    transaction: Transaction<'a, Postgres>,
}

impl WorkerClaim<'_> {
    pub async fn complete(mut self, restarted: bool, ready: bool) -> Result<()> {
        if restarted {
            sqlx::query("UPDATE wf_run SET attempt_no=attempt_no+1 WHERE run_id=$1")
                .bind(&self.run_id)
                .execute(&mut *self.transaction)
                .await?;
        }
        if ready {
            sqlx::query("UPDATE wf_run SET state=$2,ended_at=CASE WHEN $3 THEN now() ELSE NULL END WHERE run_id=$1")
                .bind(&self.run_id)
                .bind(if self.stopping { "stopped" } else { "running" })
                .bind(self.stopping)
                .execute(&mut *self.transaction)
                .await?;
            sqlx::query("UPDATE wf_workflow SET status=$2,updated_at=now() WHERE id=$1")
                .bind(self.workflow_id)
                .bind(if self.stopping { "STOPPED" } else { "RUNNING" })
                .execute(&mut *self.transaction)
                .await?;
        }
        self.transaction.commit().await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ControllerPostgresRepository {
    pool: sqlx::PgPool,
}

impl ControllerPostgresRepository {
    pub async fn from_state(state: &SigbotState) -> Result<Self> {
        Ok(Self {
            pool: state.postgres_pool().await?,
        })
    }

    pub async fn active_workflows(&self) -> Result<Vec<ActiveWorkflow>> {
        let rows = sqlx::query("SELECT id,env,tenant_id FROM wf_workflow WHERE del_flag=0 AND status IN ('PENDING','STARTING','RUNNING','STOPPING','CANCELLING') ORDER BY updated_at,id LIMIT 128")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let scope = MessageScope {
                    env: row.try_get("env")?,
                    tenant_id: row.try_get("tenant_id")?,
                };
                scope.validate()?;
                Ok(ActiveWorkflow {
                    id: row.try_get("id")?,
                    scope,
                })
            })
            .collect()
    }

    pub async fn claim(&self, scope: &MessageScope, workflow_id: Uuid) -> Result<Option<WorkerClaim<'_>>> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT status FROM wf_workflow WHERE id=$1 AND tenant_id=$2 AND env=$3 AND del_flag=0 FOR UPDATE SKIP LOCKED")
            .bind(workflow_id)
            .bind(scope.tenant_id)
            .bind(&scope.env)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status: String = row.get("status");
        if !matches!(
            status.as_str(),
            "PENDING" | "STARTING" | "RUNNING" | "STOPPING" | "CANCELLING"
        ) {
            return Ok(None);
        }
        let run = sqlx::query("SELECT run_id,params,worker_id FROM wf_run WHERE workflow_id=$1 AND tenant_id=$2 AND env=$3 AND state IN ('queued','running') ORDER BY started_at DESC LIMIT 1 FOR UPDATE")
            .bind(workflow_id)
            .bind(scope.tenant_id)
            .bind(&scope.env)
            .fetch_optional(&mut *transaction)
            .await?
            .context("workflow has no active run")?;
        let worker = crate::modules::controller::storage::postgres::WorkerPostgresRepository::resolve(
            &mut transaction,
            scope.tenant_id,
            &scope.env,
            workflow_id,
            run.try_get("worker_id")?,
        )
        .await?;
        sqlx::query("UPDATE wf_run SET worker_id=$2 WHERE run_id=$1 AND worker_id IS NULL")
            .bind(run.get::<Uuid, _>("run_id"))
            .bind(worker.id)
            .execute(&mut *transaction)
            .await?;
        Ok(Some(WorkerClaim {
            run_id: run.get("run_id"),
            parameters: run.get("params"),
            stopping: matches!(status.as_str(), "STOPPING" | "CANCELLING"),
            worker,
            workflow_id,
            transaction,
        }))
    }
}
