// SPDX-License-Identifier: GPL-3.0-only

use crate::modules::messager::handler::messager::MessagerHandler;
use crate::modules::order::storage::postgres::OrderPostgresRepository;
use crate::modules::order::storage::postgres::{
    OrderExecutionPostgresRepository, OrderGroupPostgresRepository, OrderPublicationPostgresRepository,
    OrderRecordPostgresRepository, OrderReservationPostgresRepository, OrderStatusPostgresRepository,
    OrderVenuePostgresRepository,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_types::exchange::instrument::InstrumentSpec;
use sigbot_types::messager::envelope::{DurableMessage, MessageEnvelope};
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::{ExecutionFact, OrderState, OrderStatusFact};
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::wallet::reservation::ReservationAction;
use uuid::Uuid;

pub use crate::modules::order::storage::postgres::SignalAdmission;
pub use crate::modules::order::storage::postgres::{OrderExposure, PublishedVenueFact, VenueObservation, VenuePoll};

#[derive(Clone)]
pub struct OrderHandler {
    storage: OrderPostgresRepository,
}

#[derive(Clone)]
pub struct OrderRecordHandler {
    repository: OrderRecordPostgresRepository,
}

pub struct OrderReservationHandler {
    repository: OrderReservationPostgresRepository,
}

pub struct OrderExecutionHandler {
    repository: OrderExecutionPostgresRepository,
    status: OrderStatusPostgresRepository,
}

pub struct OrderGroupHandler {
    repository: OrderGroupPostgresRepository,
}

pub struct OrderVenueHandler {
    repository: OrderVenuePostgresRepository,
    publication: OrderPublicationPostgresRepository,
}

pub struct OrderEventHandler;

impl OrderHandler {
    pub async fn connect(database_url: &str) -> Result<Self> {
        Ok(Self {
            storage: OrderPostgresRepository::connect(database_url).await?,
        })
    }

    pub fn messager(&self) -> MessagerHandler {
        self.storage.messager()
    }

    pub async fn virtual_time(&self, scope: &MessageScope, run_id: &Uuid) -> Result<DateTime<Utc>> {
        self.storage.virtual_time(scope, run_id).await
    }

    pub async fn pending(&self, consumer: &str, workflow_id: Uuid) -> Result<Vec<DurableMessage<Value>>> {
        self.storage.pending(consumer, workflow_id).await
    }

    pub async fn owns_account(&self, scope: &MessageScope, account_id: Uuid) -> Result<bool> {
        self.storage.owns_account(scope, account_id).await
    }

    pub async fn is_compensation(&self, input: &DurableMessage<TradeIntent>, order_node_id: i64) -> Result<bool> {
        self.storage.is_compensation(input, order_node_id).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn admit_signal(
        &self,
        consumer: &str,
        topic: &str,
        wire: &MessageEnvelope<Value>,
        input: &DurableMessage<TradeIntent>,
        order_node_id: i64,
        coordinators: &[i64],
        now: DateTime<Utc>,
    ) -> Result<SignalAdmission> {
        self.storage
            .admit_signal(consumer, topic, wire, input, order_node_id, coordinators, now)
            .await
    }

    pub async fn reject_signal(
        &self,
        consumer: &str,
        input: &DurableMessage<TradeIntent>,
        node_id: i64,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.storage.reject_signal(consumer, input, node_id, now).await
    }

    pub async fn complete_signal(&self, consumer: &str, topic: &str, wire: &MessageEnvelope<Value>) -> Result<()> {
        self.storage.complete_signal(consumer, topic, wire).await
    }

    pub async fn quarantine_route(&self, consumer: &str, topic: &str, wire: &MessageEnvelope<Value>) -> Result<()> {
        self.storage.quarantine_route(consumer, topic, wire).await
    }
}

impl OrderRecordHandler {
    pub fn new(storage: &OrderHandler) -> Self {
        Self {
            repository: OrderRecordPostgresRepository::new(&storage.storage),
        }
    }

    pub async fn accept_with<F>(&self, message: &DurableMessage<TradeIntent>, decide: F) -> Result<Option<String>>
    where
        F: FnOnce(&OrderExposure) -> Result<(String, Decimal)>,
    {
        self.repository.accept_with(message, decide).await
    }

    pub async fn find(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<String>> {
        self.repository.find(message).await
    }

    pub async fn wallet_mode(
        &self,
        message: &DurableMessage<TradeIntent>,
    ) -> Result<sigbot_types::exchange::mode::ExecutionMode> {
        self.repository.wallet_mode(message).await
    }

    pub async fn reservation(&self, message: &DurableMessage<TradeIntent>) -> Result<(String, Decimal)> {
        self.repository.reservation(message).await
    }

    pub async fn prepared(&self, message: &DurableMessage<TradeIntent>) -> Result<Option<Value>> {
        self.repository.prepared(message).await
    }

    pub async fn persist_prepared(&self, message: &DurableMessage<TradeIntent>, value: &Value) -> Result<Value> {
        self.repository.persist_prepared(message, value).await
    }

    pub async fn claim_submission(&self, message: &DurableMessage<TradeIntent>) -> Result<bool> {
        self.repository.claim_submission(message).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn persist_transition(
        &self,
        reservation: &OrderReservationHandler,
        producer: &str,
        node_id: i64,
        input: &DurableMessage<TradeIntent>,
        state: OrderState,
        exchange_order_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        self.repository
            .persist_transition(
                &reservation.repository,
                producer,
                node_id,
                input,
                state,
                exchange_order_id,
                now,
            )
            .await
    }
}

impl OrderReservationHandler {
    pub fn new(
        storage: &OrderHandler,
        scope: MessageScope,
        node_id: i64,
        wallet_node_id: i64,
        producer: String,
        workflow_id: Uuid,
    ) -> Result<Self> {
        Ok(Self {
            repository: OrderReservationPostgresRepository::new(
                &storage.storage,
                scope,
                node_id,
                wallet_node_id,
                producer,
                workflow_id,
            )?,
        })
    }

    pub async fn request(&self, input: &DurableMessage<TradeIntent>, action: ReservationAction) -> Result<()> {
        self.repository.request(input, action).await
    }

    pub async fn consume(&self, bytes: &[u8]) -> Result<Option<DurableMessage<TradeIntent>>> {
        self.repository.consume(bytes).await
    }
}

impl OrderExecutionHandler {
    pub fn new(
        storage: &OrderHandler,
        scope: MessageScope,
        node_id: i64,
        sources: Vec<i64>,
        workflow_id: Uuid,
    ) -> Result<Self> {
        let status =
            OrderStatusPostgresRepository::new(&storage.storage, scope.clone(), node_id, sources.clone(), workflow_id)?;
        Ok(Self {
            repository: OrderExecutionPostgresRepository::new(&storage.storage, scope, node_id, sources, workflow_id)?,
            status,
        })
    }

    pub fn with_instrument(mut self, instrument: InstrumentSpec) -> Result<Self> {
        self.repository = self.repository.with_instrument(instrument)?;
        Ok(self)
    }

    pub async fn process(&self, message: &MessageEnvelope<ExecutionFact>) -> Result<bool> {
        self.repository.process(message).await
    }

    pub async fn process_status(&self, message: &MessageEnvelope<OrderStatusFact>) -> Result<bool> {
        self.status.process_status(message).await
    }
}

impl OrderGroupHandler {
    pub fn new(
        storage: &OrderHandler,
        scope: MessageScope,
        workflow_id: Uuid,
        node_id: i64,
        producer: String,
        virtual_clock: bool,
    ) -> Self {
        Self {
            repository: OrderGroupPostgresRepository::new(
                &storage.storage,
                scope,
                workflow_id,
                node_id,
                producer,
                virtual_clock,
            ),
        }
    }

    pub async fn advance(&self, now: DateTime<Utc>) -> Result<()> {
        self.repository.advance(now).await
    }
}

impl OrderVenueHandler {
    pub fn new(
        storage: OrderHandler,
        scope: MessageScope,
        workflow_id: Uuid,
        account_id: Uuid,
        node_id: i64,
        consumer: String,
    ) -> Self {
        let repository =
            OrderVenuePostgresRepository::new(storage.storage, scope, workflow_id, account_id, node_id, consumer);
        let publication = OrderPublicationPostgresRepository::new(repository.clone());
        Self {
            repository,
            publication,
        }
    }

    pub fn producer(&self) -> &str {
        self.repository.producer()
    }

    pub fn messager(&self) -> MessagerHandler {
        self.repository.messager()
    }

    pub async fn claim(&self) -> Result<Option<VenuePoll>> {
        self.repository.claim().await
    }

    pub async fn owns(&self, poll: &VenuePoll) -> Result<bool> {
        let mut transaction = self.repository.storage.pool.begin().await?;
        let owns = OrderVenuePostgresRepository::owns(&mut transaction, poll).await?;
        transaction.commit().await?;
        Ok(owns)
    }

    pub async fn retry(&self, poll: &VenuePoll, after: DateTime<Utc>) -> Result<()> {
        self.repository.retry(poll, after).await
    }

    pub async fn persist(
        &self,
        poll: &VenuePoll,
        observations: Vec<VenueObservation>,
        cursor: Option<&str>,
        has_more: bool,
        terminal: Option<(OrderState, Decimal)>,
    ) -> Result<()> {
        self.publication
            .persist(poll, observations, cursor, has_more, terminal)
            .await
    }

    pub async fn pending_publications(
        &self,
        execution_consumer: &str,
        status_consumer: &str,
    ) -> Result<Vec<PublishedVenueFact>> {
        self.publication
            .pending_publications(execution_consumer, status_consumer)
            .await
    }
}

impl OrderEventHandler {
    pub fn owner(scope: &MessageScope, workflow_id: Uuid, node_id: i64) -> String {
        OrderPostgresRepository::owner(scope, workflow_id, node_id)
    }

    pub fn fact_owner(kind: &str, scope: &MessageScope, workflow_id: Uuid, node_id: i64) -> String {
        OrderPostgresRepository::fact_owner(kind, scope, workflow_id, node_id)
    }
}
