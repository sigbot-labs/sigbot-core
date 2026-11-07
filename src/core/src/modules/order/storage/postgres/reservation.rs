// SPDX-License-Identifier: GPL-3.0-only
//! Order only requests Wallet mutations. No order worker writes balance/reservation tables.
use super::OrderRecordPostgresRepository;
use crate::modules::messager::storage::postgres::MessagerPostgresRepository;
use crate::modules::order::storage::postgres::OrderPostgresRepository;
use anyhow::ensure;
use anyhow::Result;
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::wallet::reservation::ReservationAction;
use sigbot_types::wallet::reservation::ReservationRequest;
use sigbot_types::wallet::reservation::ReservationResult;
use sigbot_types::wallet::reservation::ReservationState;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

pub struct OrderReservationPostgresRepository {
    pool: PgPool,
    scope: MessageScope,
    node_id: i64,
    wallet_node_id: i64,
    producer: String,
    workflow_id: Uuid,
}
impl OrderReservationPostgresRepository {
    pub fn new(
        storage: &OrderPostgresRepository,
        scope: MessageScope,
        node_id: i64,
        wallet_node_id: i64,
        producer: String,
        workflow_id: Uuid,
    ) -> Result<Self> {
        scope.validate()?;
        ensure!(
            wallet_node_id > 0 && wallet_node_id != node_id && workflow_id.get_version_num() == 7,
            "distinct Wallet owner node required"
        );
        ensure!(
            producer == OrderPostgresRepository::owner(&scope, workflow_id, node_id),
            "reservation producer differs from workflow owner"
        );
        Ok(Self {
            pool: storage.pool.clone(),
            scope,
            node_id,
            wallet_node_id,
            producer,
            workflow_id,
        })
    }

    pub async fn request(&self, input: &DurableMessage<TradeIntent>, action: ReservationAction) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        self.request_in(&mut transaction, input, action).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn request_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<TradeIntent>,
        action: ReservationAction,
    ) -> Result<()> {
        ensure!(
            input.scope == self.scope && input.workflow_id == Some(self.workflow_id),
            "reservation workflow mismatch"
        );
        let release = matches!(action, ReservationAction::Release);
        let row = sqlx::query(
            "SELECT wallet_request_seq,wallet_release_seq FROM order_info WHERE env=$1 AND tenant_id=$2
            AND wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6 FOR UPDATE",
        )
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(input.payload.wallet_id)
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .fetch_one(&mut **tx)
        .await?;
        let column = if release {
            "wallet_release_seq"
        } else {
            "wallet_request_seq"
        };
        if row.get::<Option<i64>, _>(column).is_some() {
            return Ok(());
        }
        let seq = MessagerPostgresRepository::next_sequence(tx, &input.scope, self.node_id, &input.run_id).await?;
        // Only a fixed internal column name is interpolated, never caller text.
        sqlx::query(&format!(
            "UPDATE order_info SET {column}=$7 WHERE env=$1 AND tenant_id=$2
            AND wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6"
        ))
        .bind(&input.scope.env)
        .bind(input.scope.tenant_id)
        .bind(input.payload.wallet_id)
        .bind(input.node_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .bind(seq)
        .execute(&mut **tx)
        .await?;
        let span = MessageTrace::child("order.wallet.request", &input.traceparent);
        let message = DurableMessage {
            version: 2,
            scope: input.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: input.account_id,
            node_id: self.node_id,
            target_node_id: Some(self.wallet_node_id),
            run_id: input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Request,
            event_time: input.event_time,
            available_time: input.available_time,
            produced_at: input.produced_at,
            traceparent: span.traceparent(),
            payload: serde_json::to_value(ReservationRequest {
                wallet_id: input.payload.wallet_id,
                account_id: input.payload.account_id,
                reservation_node_id: input.node_id,
                reservation_seq: input.seq,
                action,
            })?,
        };
        MessagerPostgresRepository::enqueue(
            tx,
            &self.producer,
            &AccountTopic::WalletReservationRequest.topic(&input.scope, input.payload.account_id)?,
            &message,
        )
        .await?;
        Ok(())
    }

    pub async fn consume(&self, bytes: &[u8]) -> Result<Option<DurableMessage<TradeIntent>>> {
        let wire: MessageEnvelope<ReservationResult> = serde_json::from_slice(bytes)?;
        wire.validate()?;
        let input = &wire.durable;
        ensure!(
            input.scope == self.scope
                && input.workflow_id == Some(self.workflow_id)
                && input.node_id == self.wallet_node_id
                && input.target_node_id == Some(self.node_id)
                && input.kind == MessageKind::Result
                && input.account_id == Some(input.payload.request.account_id),
            "invalid Wallet result owner/scope"
        );
        let request = &input.payload.request;
        let mut tx = self.pool.begin().await?;
        let value: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        let consumer = format!("{}:reservation", self.producer);
        let topic = AccountTopic::WalletReservationResult.topic(&self.scope, request.account_id)?;
        if !MessagerPostgresRepository::accept(&mut tx, &consumer, &topic, &value).await? {
            tx.commit().await?;
            return Ok(None);
        }
        let stored: Value = sqlx::query_scalar(
            "SELECT payload FROM msg_inbox WHERE env=$1 AND tenant_id=$2
            AND consumer=$3 AND node_id=$4 AND run_id=$5 AND seq=$6",
        )
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(&self.producer)
        .bind(request.reservation_node_id)
        .bind(&input.run_id)
        .bind(request.reservation_seq)
        .fetch_one(&mut *tx)
        .await?;
        let original: DurableMessage<TradeIntent> = serde_json::from_value(stored)?;
        ensure!(
            original.payload.wallet_id == request.wallet_id
                && original.payload.account_id == request.account_id
                && original.workflow_id == input.workflow_id,
            "Wallet result does not match persisted intent"
        );
        if let ReservationAction::Reserve { asset, amount } = &request.action {
            let terms: (String, Decimal) = sqlx::query_as(
                "SELECT reserve_asset,reserve_amount FROM order_info WHERE env=$1 AND tenant_id=$2
                AND wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6 FOR UPDATE",
            )
            .bind(&self.scope.env)
            .bind(self.scope.tenant_id)
            .bind(request.wallet_id)
            .bind(original.node_id)
            .bind(&original.run_id)
            .bind(original.seq)
            .fetch_one(&mut *tx)
            .await?;
            ensure!(
                terms.0 == *asset && terms.1 == *amount,
                "Wallet result changed accepted reserve terms"
            );
            let state = match input.payload.state {
                ReservationState::Reserved => OrderState::Reserved,
                ReservationState::Rejected => OrderState::Rejected,
                _ => anyhow::bail!("invalid reserve response state"),
            };
            if OrderRecordPostgresRepository::record(&mut tx, &original, state.as_str(), None).await? {
                OrderPostgresRepository::enqueue(
                    &mut tx,
                    &self.producer,
                    self.node_id,
                    &original,
                    state,
                    None,
                    input.available_time,
                )
                .await?;
            }
        } else {
            ensure!(
                matches!(
                    input.payload.state,
                    ReservationState::Released | ReservationState::Rejected
                ),
                "invalid release response state"
            );
        }
        MessagerPostgresRepository::complete(&mut tx, &consumer, &value.durable).await?;
        tx.commit().await?;
        Ok(Some(original))
    }
}
