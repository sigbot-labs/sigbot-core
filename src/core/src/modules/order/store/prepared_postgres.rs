// SPDX-License-Identifier: GPL-3.0-only
use super::postgres::OrderPostgresRepository;
use anyhow::{ensure, Result};
use serde_json::Value;
use sigbot_types::modules::{messager::envelope::DurableMessage, order::intent::TradeIntent};

impl OrderPostgresRepository {
    pub async fn prepared(&self, input: &DurableMessage<TradeIntent>) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT prepared_submission FROM order_record WHERE env=$1 AND tenant_id=$2 AND space_id=$3
            AND wallet_id=$4 AND producer_node_id=$5 AND run_id=$6 AND seq=$7",
        )
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id.to_string())
        .bind(input.scope.space_id)
        .bind(input.payload.wallet_id)
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .fetch_one(self.pool())
        .await?)
    }

    /// Concurrent SDK preparations may differ (e.g. salts). Only the persisted
    /// winner can be submitted. No loser can overwrite its signature or order ID.
    pub async fn persist_prepared(&self, input: &DurableMessage<TradeIntent>, candidate: &Value) -> Result<Value> {
        ensure!(
            serde_json::to_vec(candidate)?.len() <= 1_048_576,
            "prepared order exceeds storage bound"
        );
        ensure!(
            candidate
                .get("venue_order_id")
                .and_then(Value::as_str)
                .is_some_and(|id| !id.is_empty()),
            "prepared order has no venue identity"
        );
        sqlx::query("UPDATE order_record SET prepared_submission=COALESCE(prepared_submission,$8)
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND wallet_id=$4 AND producer_node_id=$5 AND run_id=$6 AND seq=$7 AND state='reserved'")
            .bind(&input.scope.env).bind(input.scope.tenant_id.to_string()).bind(input.scope.space_id)
            .bind(input.payload.wallet_id).bind(input.node_id).bind(&input.run_id).bind(input.seq).bind(candidate).execute(self.pool()).await?;
        self.prepared(input)
            .await?
            .ok_or_else(|| anyhow::anyhow!("order is no longer reserved for SDK preparation"))
    }
}
