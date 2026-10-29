//! Real PostgreSQL venue observation/cursor/Outbox and execution tracker tests.
//! Only the external exchange observation adapter is replaced. No venue network
//! or MQTT delivery claim is made; persisted Outbox rows feed the actual tracker.
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_core::modules::order::store::{postgres::OrderPostgresRepository, venue_postgres::VenuePostgresRepository};
use sigbot_exchange::client::execution_adapter::{ExecutionMode, ExecutionPage, IExecutionAdapter, PreparedSubmission};
use sigbot_messager::storage::postgres::PostgresMessageStore;
use sigbot_order::manager::{execution_tracker::OrderExecutionTracker, venue_poller::VenueExecutionPoller};
use sigbot_types::modules::{
    exchange::instrument::InstrumentSpec,
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{AccountTopic, MessageScope, WorkflowTopic},
    },
    order::{
        execution::{ExecutionFact, OrderReceipt, OrderState, OrderStatusFact},
        intent::{IntentOrderType, MarketEvidence, OrderSide, TradeIntent},
    },
};
use sqlx::{PgPool, Row};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

struct BoundaryPage {
    expected_cursor: Option<String>,
    result: Result<ExecutionPage>,
}

/// Deliberate boundary stub: it cannot submit/reconcile trading instructions.
struct MockBoundaryAdapter {
    pages: Mutex<VecDeque<BoundaryPage>>,
    seen: Mutex<Vec<Option<String>>>,
}

impl MockBoundaryAdapter {
    fn new(pages: Vec<BoundaryPage>) -> Arc<Self> {
        Arc::new(Self {
            pages: Mutex::new(pages.into()),
            seen: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl IExecutionAdapter for MockBoundaryAdapter {
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Live
    }

    async fn submit(&self, _: &DurableMessage<TradeIntent>) -> Result<OrderReceipt> {
        bail!("observation test must not submit orders")
    }

    async fn reconcile(&self, _: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>> {
        bail!("observation test must not reconcile acceptance")
    }

    async fn read_executions(
        &self,
        input: &DurableMessage<TradeIntent>,
        _: &OrderReceipt,
        _: Option<&PreparedSubmission>,
        instrument: &InstrumentSpec,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<ExecutionPage> {
        ensure!(
            limit == 1000 && input.payload.symbol == instrument.symbol,
            "unexpected observation request"
        );
        self.seen.lock().unwrap().push(cursor.map(str::to_owned));
        let page = self
            .pages
            .lock()
            .unwrap()
            .pop_front()
            .context("unexpected extra venue observation")?;
        ensure!(
            page.expected_cursor.as_deref() == cursor,
            "persisted cursor was not restored"
        );
        page.result
    }
}

struct VenueFixture {
    pool: PgPool,
    input: DurableMessage<TradeIntent>,
    consumer: String,
    instrument: InstrumentSpec,
}

impl VenueFixture {
    async fn new(label: &str) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&std::env::var("MARKETPULSE_DATABASE_URL")?)
            .await?;
        let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
        let scope = MessageScope {
            env: format!("venue-{label}-{}", now.timestamp_micros()),
            tenant_id: 900006,
            space_id: 1,
        };
        let workflow_id: i64 =
            sqlx::query_scalar("INSERT INTO s_workflow(env,tenant_id,space_id,name) VALUES($1,$2,1,$3) RETURNING id")
                .bind(&scope.env)
                .bind(scope.tenant_id)
                .bind(label)
                .fetch_one(&pool)
                .await?;
        let run_id = format!("{}-{workflow_id}", scope.env);
        sqlx::query(
            "INSERT INTO workflow_run(run_id,workflow_id,env,tenant_id,space_id,state) VALUES($1,$2,$3,$4,1,'running')",
        )
        .bind(&run_id)
        .bind(workflow_id)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .execute(&pool)
        .await?;
        let wallet: i64 = sqlx::query_scalar("SELECT nextval(pg_get_serial_sequence('s_wallets','id'))")
            .fetch_one(&pool)
            .await?;
        sqlx::query("INSERT INTO s_wallets(id,account_id,tenant_id,env,space_id,exchange,mode) VALUES($1,$1,$2,$3,1,'binance','live')")
            .bind(wallet).bind(scope.tenant_id.to_string()).bind(&scope.env).execute(&pool).await?;
        let instrument = InstrumentSpec {
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            quantity_step: Decimal::new(1, 3),
            price_tick: Decimal::new(1, 2),
            min_notional: Decimal::ONE,
        };
        let input = DurableMessage {
            version: 2,
            scope: scope.clone(),
            workflow_id: Some(workflow_id),
            account_id: Some(wallet),
            node_id: 5,
            target_node_id: Some(6),
            run_id,
            seq: 0,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: TradeIntent {
                execution_group: None,
                wallet_id: wallet,
                account_id: wallet,
                exchange: instrument.exchange.clone(),
                symbol: instrument.symbol.clone(),
                side: OrderSide::Buy,
                quantity: Decimal::ONE,
                order_type: IntentOrderType::Market,
                limit_price: None,
                reduce_only: false,
                market: MarketEvidence {
                    source: "binance".into(),
                    event_time: now,
                    available_time: now,
                    price: Decimal::from(100),
                },
            },
        };
        let consumer = format!("order:{}:{}:1:{workflow_id}:6", scope.env, scope.tenant_id);
        let fixture = Self {
            pool,
            input,
            consumer,
            instrument,
        };
        fixture.insert_order(&fixture.input, &fixture.consumer).await?;
        Ok(fixture)
    }

    async fn insert_order(&self, input: &DurableMessage<TradeIntent>, consumer: &str) -> Result<()> {
        let wire = MessageEnvelope::from_durable(
            input
                .clone()
                .map_payload(|intent| serde_json::to_value(intent).unwrap()),
        )?;
        let mut tx = self.pool.begin().await?;
        assert!(
            PostgresMessageStore::accept(
                &mut tx,
                consumer,
                &WorkflowTopic::StrategySignal.topic(&input.scope, input.workflow_id.unwrap())?,
                &wire
            )
            .await?
        );
        OrderPostgresRepository::insert(&mut tx, input, "USDT", Decimal::from(101)).await?;
        OrderPostgresRepository::record(
            &mut tx,
            input,
            "acknowledged",
            Some(&format!("accepted-{}", input.node_id)),
        )
        .await?;
        PostgresMessageStore::complete(&mut tx, consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(())
    }

    fn poller(&self, adapter: Arc<dyn IExecutionAdapter>) -> VenueExecutionPoller {
        VenueExecutionPoller::new(
            self.pool.clone(),
            self.input.scope.clone(),
            self.input.workflow_id.unwrap(),
            self.input.payload.account_id,
            6,
            self.consumer.clone(),
            adapter,
            self.instrument.clone(),
        )
    }

    fn execution(&self, trade_id: &str, quantity: Decimal) -> ExecutionFact {
        ExecutionFact {
            reservation_node_id: self.input.node_id,
            reservation_seq: self.input.seq,
            wallet_id: self.input.payload.wallet_id,
            account_id: self.input.payload.account_id,
            exchange: self.instrument.exchange.clone(),
            exchange_order_id: format!("accepted-{}", self.input.node_id),
            exchange_trade_id: trade_id.into(),
            symbol: self.instrument.symbol.clone(),
            base_asset: self.instrument.base_asset.clone(),
            quote_asset: self.instrument.quote_asset.clone(),
            side: self.input.payload.side,
            price: Decimal::from(100),
            quantity,
            quote_quantity: Some(Decimal::from(100) * quantity),
            fee: quantity / Decimal::from(10),
            fee_asset: "USDT".into(),
            order_complete: false,
            executed_at: self.input.event_time,
        }
    }

    fn page(
        &self,
        facts: Vec<ExecutionFact>,
        state: OrderState,
        quantity: Decimal,
        cursor: &str,
        has_more: bool,
    ) -> ExecutionPage {
        ExecutionPage {
            facts,
            next_cursor: Some(cursor.into()),
            has_more,
            status: Some(OrderStatusFact {
                reservation_node_id: self.input.node_id,
                reservation_seq: self.input.seq,
                wallet_id: self.input.payload.wallet_id,
                account_id: self.input.payload.account_id,
                exchange: self.instrument.exchange.clone(),
                receipt: OrderReceipt {
                    exchange_order_id: format!("accepted-{}", self.input.node_id),
                    state,
                },
                executed_quantity: quantity,
                occurred_at: self.input.event_time,
            }),
        }
    }

    async fn due(&self) -> Result<()> {
        sqlx::query("UPDATE order_venue_checkpoint SET next_poll_at=NOW()-interval '1 second' WHERE wallet_id=$1")
            .bind(self.input.payload.wallet_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn checkpoint(&self) -> Result<(Option<String>, bool, i64)> {
        Ok(sqlx::query_as("SELECT cursor,complete,version FROM order_venue_checkpoint WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4")
            .bind(self.input.payload.wallet_id).bind(self.input.node_id).bind(&self.input.run_id).bind(self.input.seq)
            .fetch_one(&self.pool).await?)
    }

    async fn source_counts(&self) -> Result<(i64, i64)> {
        let observations = sqlx::query_scalar("SELECT count(*) FROM order_venue_observation WHERE wallet_id=$1")
            .bind(self.input.payload.wallet_id)
            .fetch_one(&self.pool)
            .await?;
        let outbox = sqlx::query_scalar("SELECT count(*) FROM messager_outbox WHERE producer=$1 AND run_id=$2")
            .bind(format!("{}:venue", self.consumer))
            .bind(&self.input.run_id)
            .fetch_one(&self.pool)
            .await?;
        Ok((observations, outbox))
    }

    async fn track_persisted_facts(&self) -> Result<()> {
        let tracker = OrderExecutionTracker::new(
            self.pool.clone(),
            self.input.scope.clone(),
            6,
            vec![6],
            self.input.workflow_id.unwrap(),
        )?
        .with_instrument(self.instrument.clone())?;
        let rows =
            sqlx::query("SELECT topic,payload FROM messager_outbox WHERE producer=$1 AND run_id=$2 ORDER BY seq")
                .bind(format!("{}:venue", self.consumer))
                .bind(&self.input.run_id)
                .fetch_all(&self.pool)
                .await?;
        for row in rows {
            let payload: Value = row.get("payload");
            let foreign = OrderExecutionTracker::new(
                self.pool.clone(),
                self.input.scope.clone(),
                6,
                vec![6],
                self.input.workflow_id.unwrap() + 1,
            )?;
            let forged_same_workflow_wrong_owner = OrderExecutionTracker::new(
                self.pool.clone(),
                self.input.scope.clone(),
                16,
                vec![6],
                self.input.workflow_id.unwrap(),
            )?;
            if row.get::<String, _>("topic")
                == AccountTopic::ExecutionEvent.topic(&self.input.scope, self.input.payload.account_id)?
            {
                let wire = MessageEnvelope::from_durable(serde_json::from_value(payload)?)?;
                assert!(
                    foreign.process(&wire).await.is_err(),
                    "same node in another Workflow cannot consume a fill"
                );
                assert!(
                    forged_same_workflow_wrong_owner.process(&wire).await.is_err(),
                    "fact must resolve to this owner's original intent Inbox"
                );
                tracker.process(&wire).await?;
                let original = serde_json::to_value(&wire.durable.payload)?;
                let mut legacy = MessageEnvelope::from_durable(
                    wire.durable
                        .clone()
                        .map_payload(|fact| serde_json::to_value(fact).unwrap()),
                )?;
                legacy.durable.workflow_id = None;
                assert!(
                    PostgresMessageStore::new(self.pool.clone())
                        .restore_legacy_workflow(&mut legacy)
                        .await?
                );
                assert_eq!(
                    legacy.durable.payload, original,
                    "migration never changes financial payload"
                );
                let restored: MessageEnvelope<ExecutionFact> = serde_json::from_value(serde_json::to_value(legacy)?)?;
                assert!(
                    !tracker.process(&restored).await?,
                    "legacy inflight duplicate must observe completed Inbox"
                );
            } else {
                let wire = MessageEnvelope::from_durable(serde_json::from_value(payload)?)?;
                assert!(foreign.process_status(&wire).await.is_err());
                assert!(forged_same_workflow_wrong_owner.process_status(&wire).await.is_err());
                tracker.process_status(&wire).await?;
            }
        }
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL migrated through 20261028-12; only exchange observation is mocked"]
async fn restart_restores_cursor_and_repeated_trades_do_not_duplicate_execution_or_fees() -> Result<()> {
    let fixture = VenueFixture::new("restart").await?;
    let first = fixture.execution("fill-a", Decimal::new(4, 1));
    let second = fixture.execution("fill-b", Decimal::new(6, 1));
    let adapter = MockBoundaryAdapter::new(vec![
        BoundaryPage {
            expected_cursor: None,
            result: Ok(fixture.page(
                vec![first.clone()],
                OrderState::PartiallyFilled,
                Decimal::new(4, 1),
                "a",
                true,
            )),
        },
        BoundaryPage {
            expected_cursor: Some("a".into()),
            result: Ok(fixture.page(vec![first, second], OrderState::Filled, Decimal::ONE, "b", false)),
        },
    ]);
    fixture.poller(adapter.clone()).poll().await?;
    assert_eq!(fixture.checkpoint().await?, (Some("a".into()), false, 1));
    assert_eq!(fixture.source_counts().await?, (2, 2));
    fixture.track_persisted_facts().await?;
    fixture.due().await?;
    // A new host instance owns no in-memory cursor; the second page includes the
    // previous trade intentionally, as SDK reconnect snapshots often overlap.
    fixture.poller(adapter.clone()).poll().await?;
    fixture.track_persisted_facts().await?;
    fixture.track_persisted_facts().await?;
    assert_eq!(fixture.checkpoint().await?, (Some("b".into()), true, 2));
    assert_eq!(fixture.source_counts().await?, (4, 4));
    let (count, quantity, fee): (i64, Decimal, Decimal) =
        sqlx::query_as("SELECT count(*),SUM(quantity),SUM(fee) FROM order_execution WHERE wallet_id=$1")
            .bind(fixture.input.payload.wallet_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!((count, quantity, fee), (2, Decimal::ONE, Decimal::new(1, 1)));
    let state: String = sqlx::query_scalar("SELECT state FROM order_record WHERE wallet_id=$1")
        .bind(fixture.input.payload.wallet_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(state, "filled");
    fixture.poller(adapter.clone()).poll().await?;
    assert_eq!(*adapter.seen.lock().unwrap(), vec![None, Some("a".into())]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires PostgreSQL migrated through 20261028-12; only exchange observation is mocked"]
async fn missing_fees_and_invalid_pages_leave_facts_outbox_and_cursor_uncommitted() -> Result<()> {
    let fixture = VenueFixture::new("fees").await?;
    let first = fixture.execution("fill-a", Decimal::new(4, 1));
    let mut missing_fee = fixture.execution("fill-b", Decimal::new(6, 1));
    missing_fee.fee_asset.clear();
    let adapter = MockBoundaryAdapter::new(vec![
        BoundaryPage {
            expected_cursor: None,
            result: Err(anyhow::anyhow!("venue execution commission missing")),
        },
        BoundaryPage {
            expected_cursor: None,
            result: Ok(fixture.page(vec![first, missing_fee], OrderState::Filled, Decimal::ONE, "b", false)),
        },
        BoundaryPage {
            expected_cursor: None,
            result: Ok(fixture.page(vec![], OrderState::Filled, Decimal::ZERO, "invalid-terminal", false)),
        },
    ]);
    let poller = fixture.poller(adapter.clone());
    assert!(poller.poll().await.is_err());
    assert_eq!(fixture.checkpoint().await?, (None, false, 1));
    assert_eq!(fixture.source_counts().await?, (0, 0));
    fixture.due().await?;
    assert!(poller.poll().await.is_err());
    // The first valid trade was written before the malformed second trade; all
    // observations, sequence allocation and Outbox entries must roll back.
    assert_eq!(fixture.checkpoint().await?, (None, false, 2));
    assert_eq!(fixture.source_counts().await?, (0, 0));
    let sequences: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_sequence WHERE env=$1 AND run_id=$2 AND node_id=6")
            .bind(&fixture.input.scope.env)
            .bind(&fixture.input.run_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(sequences, 0);
    fixture.due().await?;
    assert!(
        poller.poll().await.is_err(),
        "Filled without the requested quantity must not complete polling"
    );
    assert_eq!(fixture.checkpoint().await?, (None, false, 3));
    assert_eq!(fixture.source_counts().await?, (0, 0));
    assert_eq!(*adapter.seen.lock().unwrap(), vec![None, None, None]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires PostgreSQL migrated through 20261028-12; actual repository lease fencing"]
async fn leases_fence_expired_owners_and_scope_node_filters_select_only_owned_intents() -> Result<()> {
    let fixture = VenueFixture::new("lease").await?;
    let scope = &fixture.input.scope;
    let workflow_id = fixture.input.workflow_id.unwrap();
    let account = fixture.input.payload.account_id;
    let mut other_scope = scope.clone();
    other_scope.space_id += 1;
    assert!(
        VenuePostgresRepository::claim(&fixture.pool, &other_scope, workflow_id, account, &fixture.consumer)
            .await?
            .is_none()
    );
    assert!(
        VenuePostgresRepository::claim(&fixture.pool, scope, workflow_id + 1, account, &fixture.consumer)
            .await?
            .is_none()
    );
    assert!(
        VenuePostgresRepository::claim(&fixture.pool, scope, workflow_id, account + 1, &fixture.consumer)
            .await?
            .is_none()
    );
    let first = VenuePostgresRepository::claim(&fixture.pool, scope, workflow_id, account, &fixture.consumer)
        .await?
        .context("first lease")?;
    assert!(
        VenuePostgresRepository::claim(&fixture.pool, scope, workflow_id, account, &fixture.consumer)
            .await?
            .is_none()
    );
    let other_consumer = format!("order:{}:{}:1:{workflow_id}:16", scope.env, scope.tenant_id);
    let mut other_input = fixture.input.clone();
    other_input.node_id = 15;
    other_input.target_node_id = Some(16);
    fixture.insert_order(&other_input, &other_consumer).await?;
    let other = VenuePostgresRepository::claim(&fixture.pool, scope, workflow_id, account, &other_consumer)
        .await?
        .context("other node lease")?;
    assert_eq!((other.input.node_id, other.input.seq, other.version), (15, 0, 1));
    fixture.due().await?;
    let next = VenuePostgresRepository::claim(&fixture.pool, scope, workflow_id, account, &fixture.consumer)
        .await?
        .context("replacement lease")?;
    assert_eq!(next.version, first.version + 1);
    let mut tx = fixture.pool.begin().await?;
    assert!(!VenuePostgresRepository::owns(&mut tx, &first).await?);
    tx.commit().await?;
    let mut tx = fixture.pool.begin().await?;
    assert!(VenuePostgresRepository::owns(&mut tx, &next).await?);
    tx.commit().await?;
    VenuePostgresRepository::retry(&fixture.pool, &first, Utc::now()).await?;
    assert!(
        VenuePostgresRepository::claim(&fixture.pool, scope, workflow_id, account, &fixture.consumer)
            .await?
            .is_none(),
        "a stale owner's retry cannot shorten the current lease"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated isolated PostgreSQL; no broker or venue connection is made"]
async fn account_broadcast_other_workflow_commits_quarantine_before_ack() -> Result<()> {
    use serde_json::json;
    use sigbot_order::server::order_runtime::OrderRuntime;
    let fixture = VenueFixture::new("route").await?;
    std::env::set_var("SIGBOT_ORDER_ROUTING_TEST_TOKEN", "test-only-no-network");
    let config = json!({"scope":fixture.input.scope,"workflow_id":fixture.input.workflow_id.unwrap(),
        "node_id":6,"account_id":fixture.input.payload.account_id,"input_node_ids":[5],"execution_node_ids":[7],
        "wallet_node_id":8,"database_url_env":"MARKETPULSE_DATABASE_URL","instrument":fixture.instrument,
        "risk_limits":{"max_order_notional":"1000","max_account_notional":"10000","max_position_quantity":"100","max_market_age_ms":60000,"fee_buffer_bps":"10"},
        "exchange":{"mode":"simulated","endpoint":"http://127.0.0.1:1","token_env":"SIGBOT_ORDER_ROUTING_TEST_TOKEN","timeout_seconds":1}});
    let runtime = OrderRuntime::from_properties(&std::collections::HashMap::from([(
        "runtime_config".into(),
        config.to_string(),
    )]))
    .await?;
    let mut foreign = fixture
        .input
        .clone()
        .map_payload(|_| json!({"reason":"other workflow account broadcast"}));
    foreign.workflow_id = Some(fixture.input.workflow_id.unwrap() + 1);
    foreign.node_id = 7;
    let wire = MessageEnvelope::from_durable(foreign)?;
    let bytes = serde_json::to_vec(&wire)?;
    runtime.consume_account(AccountTopic::ExecutionEvent, &bytes).await?;
    runtime.consume_account(AccountTopic::ExecutionEvent, &bytes).await?;
    let rows: Vec<(String, String, String)> =
        sqlx::query_as("SELECT consumer,state,quarantine_reason FROM messager_inbox WHERE env=$1 AND node_id=7")
            .bind(&fixture.input.scope.env)
            .fetch_all(&fixture.pool)
            .await?;
    assert_eq!(
        rows,
        vec![(
            format!("{}:routing", fixture.consumer),
            "quarantined".into(),
            "outside_order_route".into()
        )]
    );
    let facts: i64 = sqlx::query_scalar("SELECT count(*) FROM order_execution WHERE wallet_id=$1")
        .bind(fixture.input.payload.wallet_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        facts, 0,
        "account routing rejection must never create financial execution facts"
    );
    Ok(())
}
