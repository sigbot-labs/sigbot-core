// SPDX-License-Identifier: GPL-3.0-only
use anyhow::ensure;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::order::execution_group::GroupFailurePolicy;
use sigbot_types::order::intent::TradeIntent;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

pub struct OrderAdmissionPostgresRepository;
impl OrderAdmissionPostgresRepository {
    pub async fn register(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<TradeIntent>,
        order_node_id: i64,
        coordinators: &[i64],
        now: DateTime<Utc>,
    ) -> Result<()> {
        let Some(group) = &input.payload.execution_group else {
            return Ok(());
        };
        group.validate(input.available_time)?;
        ensure!(
            coordinators.contains(&group.coordinator_node_id),
            "execution group coordinator is outside trusted Workflow edges"
        );
        let owned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM ledger_info WHERE id=$1 AND account_id=$2 AND env=$3 AND tenant_id=$4 AND del_flag=FALSE)")
            .bind(input.payload.wallet_id).bind(input.payload.account_id).bind(&input.scope.env).bind(input.scope.tenant_id)
            .fetch_one(&mut **tx).await?;
        ensure!(owned, "execution group wallet is outside this account scope");
        if Self::already_registered(tx, input, order_node_id).await? {
            return Ok(());
        }
        ensure!(now <= group.deadline, "execution group admission deadline passed");
        let policy = match group.failure_policy {
            GroupFailurePolicy::Hold => "hold",
            GroupFailurePolicy::CompensateFilled => "compensate_filled",
        };
        sqlx::query("INSERT INTO ord_execution_group(env,tenant_id,producer_node_id,run_id,seq,workflow_id,coordinator_node_id,leg_count,deadline,failure_policy,traceparent,state,legs)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending','{}') ON CONFLICT DO NOTHING")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.node_id).bind(&input.run_id)
            .bind(group.group_seq).bind(input.workflow_id).bind(group.coordinator_node_id).bind(i32::from(group.leg_count)).bind(group.deadline).bind(policy)
            .bind(&input.traceparent).execute(&mut **tx).await?;
        let stored = sqlx::query(
            "SELECT workflow_id,coordinator_node_id,leg_count,deadline,failure_policy,state FROM ord_execution_group
            WHERE env=$1 AND tenant_id=$2 AND producer_node_id=$3 AND run_id=$4 AND seq=$5 FOR UPDATE",
        )
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(group.group_seq)
        .fetch_one(&mut **tx)
        .await?;
        ensure!(
            stored.get::<Option<Uuid>, _>("workflow_id") == input.workflow_id
                && stored.get::<i64, _>("coordinator_node_id") == group.coordinator_node_id
                && stored.get::<i32, _>("leg_count") == i32::from(group.leg_count)
                && stored.get::<DateTime<Utc>, _>("deadline") == group.deadline
                && stored.get::<String, _>("failure_policy") == policy,
            "execution group identity reused with different terms"
        );
        if Self::already_registered(tx, input, order_node_id).await? {
            return Ok(());
        }
        ensure!(
            matches!(
                stored.get::<String, _>("state").as_str(),
                "pending" | "executing" | "reconciling"
            ),
            "execution group is closed to new legs"
        );
        sqlx::query("INSERT INTO ord_execution_leg(env,tenant_id,producer_node_id,run_id,group_seq,leg_index,order_node_id,wallet_id,intent_seq,intent)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.node_id).bind(&input.run_id).bind(group.group_seq)
            .bind(i32::from(group.leg_index)).bind(order_node_id).bind(input.payload.wallet_id).bind(input.seq).bind(serde_json::to_value(&input.payload)?)
            .execute(&mut **tx).await?;
        ensure!(
            Self::already_registered(tx, input, order_node_id).await?,
            "execution group leg registration failed"
        );
        Ok(())
    }

    async fn already_registered(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<TradeIntent>,
        order_node_id: i64,
    ) -> Result<bool> {
        let Some(group) = &input.payload.execution_group else {
            return Ok(false);
        };
        let leg=sqlx::query("SELECT l.intent_seq,l.intent,l.order_node_id,g.workflow_id FROM ord_execution_leg l JOIN ord_execution_group g
            ON g.env=l.env AND g.tenant_id=l.tenant_id AND g.producer_node_id=l.producer_node_id AND g.run_id=l.run_id AND g.seq=l.group_seq
            WHERE l.env=$1 AND l.tenant_id=$2 AND l.producer_node_id=$3 AND l.run_id=$4 AND l.group_seq=$5 AND l.leg_index=$6")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.node_id).bind(&input.run_id).bind(group.group_seq)
            .bind(i32::from(group.leg_index)).fetch_optional(&mut **tx).await?;
        if let Some(leg) = leg {
            ensure!(
                leg.get::<Option<Uuid>, _>("workflow_id") == input.workflow_id
                    && leg.get::<i64, _>("intent_seq") == input.seq
                    && leg.get::<i64, _>("order_node_id") == order_node_id
                    && leg.get::<Value, _>("intent") == serde_json::to_value(&input.payload)?,
                "execution group leg identity collision"
            );
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn reject(tx: &mut Transaction<'_, Postgres>, input: &DurableMessage<TradeIntent>) -> Result<()> {
        if let Some(group) = &input.payload.execution_group {
            sqlx::query(
                "UPDATE ord_execution_leg SET rejected=TRUE WHERE env=$1 AND tenant_id=$2
                AND producer_node_id=$3 AND run_id=$4 AND group_seq=$5 AND leg_index=$6 AND intent_seq=$7",
            )
            .bind(&input.scope.env)
            .bind(input.scope.tenant_id)
            .bind(input.node_id)
            .bind(&input.run_id)
            .bind(group.group_seq)
            .bind(i32::from(group.leg_index))
            .bind(input.seq)
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }

    pub async fn is_compensation(
        pool: &PgPool,
        input: &DurableMessage<TradeIntent>,
        order_node_id: i64,
    ) -> Result<bool> {
        let value:Option<Value>=sqlx::query_scalar("SELECT l.compensation_intent FROM ord_execution_leg l JOIN ord_execution_group g
            USING(env,tenant_id,producer_node_id,run_id) WHERE l.group_seq=g.seq AND l.env=$1 AND l.tenant_id=$2
            AND g.coordinator_node_id=$3 AND l.run_id=$4 AND l.compensation_seq=$5 AND l.order_node_id=$6 AND g.workflow_id=$7")
            .bind(&input.scope.env).bind(input.scope.tenant_id).bind(input.node_id).bind(&input.run_id).bind(input.seq)
            .bind(order_node_id).bind(input.workflow_id).fetch_optional(pool).await?;
        Ok(value == Some(serde_json::to_value(&input.payload)?))
    }
}
