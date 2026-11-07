//! Real PostgreSQL venue observation/cursor/Outbox and execution tracker tests.
//! Only the external exchange observation adapter is replaced. No venue network
//! or MQTT delivery claim is made; persisted Outbox rows feed the actual tracker.
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common_context::ApplicationContext;
use common_context::IProvider;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_core::modules::order::handler::order::{OrderHandler, OrderRecordHandler, OrderVenueHandler};
use sigbot_exchange::exchange::ExecutionMode;
use sigbot_exchange::exchange::ExecutionPage;
use sigbot_exchange::exchange::IExecutionAdapter;
use sigbot_exchange::exchange::PreparedSubmission;
use sigbot_order::execution::tracker::OrderExecutionTracker;
use sigbot_order::execution::venue::VenueExecutionPoller;
use sigbot_types::exchange::instrument::InstrumentSpec;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::order::execution::ExecutionFact;
use sigbot_types::order::execution::OrderReceipt;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::execution::OrderStatusFact;
use sigbot_types::order::intent::IntentOrderType;
use sigbot_types::order::intent::MarketEvidence;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::Uuid;
use sqlx::PgPool;
use sqlx::Row;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

struct BoundaryPage {
    expected_cursor: Option<String>,
    result: Result<ExecutionPage>,
}

/// Deliberate boundary stub: it cannot submit/reconcile trading instructions.
struct MockBoundaryAdapter {
    mode: ExecutionMode,
    pages: Mutex<VecDeque<BoundaryPage>>,
    seen: Mutex<Vec<Option<String>>>,
}

impl MockBoundaryAdapter {
    fn new(pages: Vec<BoundaryPage>) -> Arc<Self> {
        Arc::new(Self {
            mode: ExecutionMode::Live,
            pages: Mutex::new(pages.into()),
            seen: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl IExecutionAdapter for MockBoundaryAdapter {
    fn mode(&self) -> ExecutionMode {
        self.mode
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
    storage: OrderHandler,
    input: DurableMessage<TradeIntent>,
    consumer: String,
    instrument: InstrumentSpec,
}

impl VenueFixture {
    async fn new(label: &str) -> Result<Self> {
        ApplicationContext::global().init().await?;
        let database_url = std::env::var("MARKETPULSE_DATABASE_URL")?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await?;
        let storage = OrderHandler::connect(&database_url).await?;
        let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
        let tenant_id = Uuid::now_v7();
        let scope = MessageScope {
            env: format!("venue-{label}-{}", now.timestamp_micros()),
            tenant_id,
        };
        let workflow_id = Uuid::now_v7();
        sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name) VALUES($1,$2,$3,$4)")
            .bind(workflow_id)
            .bind(&scope.env)
            .bind(scope.tenant_id)
            .bind(label)
            .execute(&pool)
            .await?;
        let run_id = Uuid::now_v7();
        sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,$3,$4,'running')")
            .bind(&run_id)
            .bind(workflow_id)
            .bind(&scope.env)
            .bind(scope.tenant_id)
            .execute(&pool)
            .await?;
        let wallet = Uuid::now_v7();
        let account = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO ledger_info(id,account_id,tenant_id,env,exchange,mode) VALUES($1,$2,$3,$4,'binance','live')",
        )
        .bind(wallet)
        .bind(account)
        .bind(scope.tenant_id)
        .bind(&scope.env)
        .execute(&pool)
        .await?;
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
            account_id: Some(account),
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
                account_id: account,
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
        sqlx::query(
            "INSERT INTO mkt_event(env,tenant_id,source,source_event_id,symbol,event_type,event_at,available_at,seq,revision,payload,run_id)
             VALUES($1,$2,$3,$4,$5,'market',$6,$7,0,0,$8,$9)",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(&input.payload.market.source)
        .bind(format!("venue-{label}-{run_id}"))
        .bind(&input.payload.symbol)
        .bind(input.payload.market.event_time)
        .bind(input.payload.market.available_time)
        .bind(serde_json::json!({"price": input.payload.market.price.to_string()}))
        .bind(run_id)
        .execute(&pool)
        .await?;
        let consumer = format!("order:{}:{}:{workflow_id}:6", scope.env, scope.tenant_id);
        let fixture = Self {
            pool,
            storage,
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
        let topic = WorkflowTopic::StrategySignal.topic(&input.scope, input.workflow_id.unwrap())?;
        assert!(self.storage.messager().accept(consumer, &topic, &wire).await?);
        OrderRecordHandler::new(&self.storage)
            .accept_with(input, |_| Ok(("USDT".into(), Decimal::from(101))))
            .await?;
        sqlx::query("UPDATE order_info SET state='acknowledged',exchange_order_id=$1 WHERE wallet_id=$2 AND producer_node_id=$3 AND run_id=$4 AND seq=$5")
            .bind(format!("accepted-{}", input.node_id))
            .bind(input.payload.wallet_id)
            .bind(input.node_id)
            .bind(&input.run_id)
            .bind(input.seq)
            .execute(&self.pool)
            .await?;
        self.storage.complete_signal(consumer, &topic, &wire).await?;
        Ok(())
    }

    fn poller(&self, adapter: Arc<dyn IExecutionAdapter>) -> VenueExecutionPoller {
        VenueExecutionPoller::new(
            self.storage.clone(),
            self.input.scope.clone(),
            self.input.workflow_id.unwrap(),
            self.input.payload.account_id,
            6,
            self.consumer.clone(),
            adapter,
            self.instrument.clone(),
        )
    }

    fn venue_handler(
        &self,
        scope: MessageScope,
        workflow_id: Uuid,
        account_id: Uuid,
        node_id: i64,
        consumer: String,
    ) -> OrderVenueHandler {
        OrderVenueHandler::new(self.storage.clone(), scope, workflow_id, account_id, node_id, consumer)
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
        sqlx::query("UPDATE ord_venue_checkpoint SET next_poll_at=NOW()-interval '1 second' WHERE wallet_id=$1")
            .bind(self.input.payload.wallet_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn checkpoint(&self) -> Result<(Option<String>, bool, i64)> {
        Ok(sqlx::query_as("SELECT cursor,complete,version FROM ord_venue_checkpoint WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4")
            .bind(self.input.payload.wallet_id).bind(self.input.node_id).bind(&self.input.run_id).bind(self.input.seq)
            .fetch_one(&self.pool).await?)
    }

    async fn source_counts(&self) -> Result<(i64, i64)> {
        let observations = sqlx::query_scalar("SELECT count(*) FROM ord_venue_observation WHERE wallet_id=$1")
            .bind(self.input.payload.wallet_id)
            .fetch_one(&self.pool)
            .await?;
        let outbox = sqlx::query_scalar("SELECT count(*) FROM msg_outbox WHERE producer=$1 AND run_id=$2")
            .bind(format!("{}:venue", self.consumer))
            .bind(&self.input.run_id)
            .fetch_one(&self.pool)
            .await?;
        Ok((observations, outbox))
    }

    async fn track_persisted_facts(&self) -> Result<()> {
        let tracker = OrderExecutionTracker::new(
            &self.storage,
            self.input.scope.clone(),
            6,
            vec![6],
            self.input.workflow_id.unwrap(),
        )?
        .with_instrument(self.instrument.clone())?;
        let rows = sqlx::query("SELECT topic,payload FROM msg_outbox WHERE producer=$1 AND run_id=$2 ORDER BY seq")
            .bind(format!("{}:venue", self.consumer))
            .bind(&self.input.run_id)
            .fetch_all(&self.pool)
            .await?;
        for row in rows {
            let payload: Value = row.get("payload");
            let foreign =
                OrderExecutionTracker::new(&self.storage, self.input.scope.clone(), 6, vec![6], Uuid::now_v7())?;
            let forged_same_workflow_wrong_owner = OrderExecutionTracker::new(
                &self.storage,
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
                assert!(
                    !tracker.process(&wire).await?,
                    "a repeated execution observes its committed Inbox"
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
        sqlx::query_as("SELECT count(*),SUM(quantity),SUM(fee) FROM ord_execution WHERE wallet_id=$1")
            .bind(fixture.input.payload.wallet_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!((count, quantity, fee), (2, Decimal::ONE, Decimal::new(1, 1)));
    let state: String = sqlx::query_scalar("SELECT state FROM order_info WHERE wallet_id=$1")
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
        sqlx::query_scalar("SELECT count(*) FROM wf_sequence WHERE env=$1 AND run_id=$2 AND node_id=6")
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
#[ignore = "requires migrated isolated PostgreSQL; asserts mode isolation before any venue I/O"]
async fn wallet_mode_mismatch_blocks_venue_observation_without_advancing_facts_or_cursor() -> Result<()> {
    let fixture = VenueFixture::new("mode").await?;
    let adapter = Arc::new(MockBoundaryAdapter {
        mode: ExecutionMode::Paper,
        pages: Mutex::new(VecDeque::new()),
        seen: Mutex::new(Vec::new()),
    });
    let error = fixture.poller(adapter.clone()).poll().await.unwrap_err();
    assert!(error
        .downcast_ref::<sigbot_order::processing::risk::OrderRiskRejection>()
        .is_some());
    assert!(
        adapter.seen.lock().unwrap().is_empty(),
        "mode isolation must precede venue I/O"
    );
    assert_eq!(fixture.checkpoint().await?, (None, false, 1));
    assert_eq!(fixture.source_counts().await?, (0, 0));
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
    other_scope.tenant_id = Uuid::now_v7();
    let outside_scope = fixture.venue_handler(other_scope, workflow_id, account, 6, fixture.consumer.clone());
    assert!(outside_scope.claim().await?.is_none());
    let outside_workflow = fixture.venue_handler(scope.clone(), Uuid::now_v7(), account, 6, fixture.consumer.clone());
    assert!(outside_workflow.claim().await?.is_none());
    let outside_account =
        fixture.venue_handler(scope.clone(), workflow_id, Uuid::now_v7(), 6, fixture.consumer.clone());
    assert!(outside_account.claim().await?.is_none());
    let repository = fixture.venue_handler(scope.clone(), workflow_id, account, 6, fixture.consumer.clone());
    let first = repository.claim().await?.context("first lease")?;
    assert!(repository.claim().await?.is_none());
    let other_consumer = format!("order:{}:{}:{workflow_id}:16", scope.env, scope.tenant_id);
    let mut other_input = fixture.input.clone();
    other_input.node_id = 15;
    other_input.target_node_id = Some(16);
    fixture.insert_order(&other_input, &other_consumer).await?;
    let other_repository = fixture.venue_handler(scope.clone(), workflow_id, account, 16, other_consumer);
    let other = other_repository.claim().await?.context("other node lease")?;
    assert_eq!((other.input.node_id, other.input.seq, other.version), (15, 0, 1));
    fixture.due().await?;
    let next = repository.claim().await?.context("replacement lease")?;
    assert_eq!(next.version, first.version + 1);
    assert!(!repository.owns(&first).await?);
    assert!(repository.owns(&next).await?);
    repository.retry(&first, Utc::now()).await?;
    assert!(
        repository.claim().await?.is_none(),
        "a stale owner's retry cannot shorten the current lease"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated isolated PostgreSQL; no broker or venue connection is made"]
async fn account_broadcast_other_workflow_commits_quarantine_before_ack() -> Result<()> {
    use serde_json::json;
    use sigbot_order::processing::workflow_order::WorkflowOrderProcessor;
    let fixture = VenueFixture::new("route").await?;
    std::env::set_var(
        "SIGBOT__SECRETS__VALUES__ORDER_ROUTING_TEST_TOKEN",
        "test-only-no-network",
    );
    sigbot_core::config::config::refresh_config()?;
    let config = json!({"scope":fixture.input.scope,"workflow_id":fixture.input.workflow_id.unwrap(),
        "node_id":6,"account_id":fixture.input.payload.account_id,"input_node_ids":[5],"execution_node_ids":[7],
        "wallet_node_id":8,"instrument":fixture.instrument,
        "risk_limits":{"max_order_notional":"1000","max_account_notional":"10000","max_position_quantity":"100","max_market_age_ms":60000,"fee_buffer_bps":"10"},
        "exchange":{"mode":"backtest","endpoint":"http://127.0.0.1:1","token_secret":"order_routing_test_token","timeout_seconds":1}});
    let runtime = WorkflowOrderProcessor::from_properties(&std::collections::HashMap::from([(
        "runtime_config".into(),
        config.to_string(),
    )]))
    .await?;
    let mut foreign = fixture
        .input
        .clone()
        .map_payload(|_| json!({"reason":"other workflow account broadcast"}));
    foreign.workflow_id = Some(Uuid::now_v7());
    foreign.node_id = 7;
    let wire = MessageEnvelope::from_durable(foreign)?;
    let bytes = serde_json::to_vec(&wire)?;
    runtime.consume_account(AccountTopic::ExecutionEvent, &bytes).await?;
    runtime.consume_account(AccountTopic::ExecutionEvent, &bytes).await?;
    let rows: Vec<(String, String, String)> =
        sqlx::query_as("SELECT consumer,state,quarantine_reason FROM msg_inbox WHERE env=$1 AND node_id=7")
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
    let facts: i64 = sqlx::query_scalar("SELECT count(*) FROM ord_execution WHERE wallet_id=$1")
        .bind(fixture.input.payload.wallet_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        facts, 0,
        "account routing rejection must never create financial execution facts"
    );
    Ok(())
}

#[async_trait::async_trait]
impl IProvider for MockBoundaryAdapter {
    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
