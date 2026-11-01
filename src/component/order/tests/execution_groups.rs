//! Real PostgreSQL coordinator contracts. Participant states and balanced journals
//! are explicit synthetic fixtures; these tests do not claim live venue execution.
use anyhow::Result;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use serde_json::Value;
use sigbot_core::modules::order::handler::order::{
    OrderEventHandler, OrderHandler, OrderRecordHandler, SignalAdmission,
};
use sigbot_order::execution::groups::ExecutionGroups;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::order::execution_group::ExecutionGroupLeg;
use sigbot_types::order::execution_group::GroupFailurePolicy;
use sigbot_types::order::intent::IntentOrderType;
use sigbot_types::order::intent::MarketEvidence;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::Uuid;
use sqlx::PgPool;

struct GroupFixture {
    pool: PgPool,
    storage: OrderHandler,
    scope: MessageScope,
    workflow_id: Uuid,
    run_id: Uuid,
    now: DateTime<Utc>,
    inputs: Vec<DurableMessage<TradeIntent>>,
}

impl GroupFixture {
    async fn new(label: &str, policy: GroupFailurePolicy) -> Result<Self> {
        let database_url = std::env::var("MARKETPULSE_DATABASE_URL")?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect(&database_url)
            .await?;
        let storage = OrderHandler::connect(&database_url).await?;
        let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
        let scope = MessageScope {
            env: format!("group-{label}-{}", now.timestamp_micros()),
            tenant_id: Uuid::now_v7(),
        };
        let workflow_id = Uuid::now_v7();
        sqlx::query("INSERT INTO wf_workflow(id,env,tenant_id,name) VALUES($1,$2,$3,$4)")
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
        let mut inputs = Vec::new();
        for index in 0..2_i64 {
            let wallet = Uuid::now_v7();
            let account = Uuid::now_v7();
            let exchange = format!("SIM_CONTRACT_{index}");
            sqlx::query(
                "INSERT INTO wlt_wallet(id,tenant_id,env,account_id,exchange,mode) VALUES($1,$2,$3,$4,$5,'backtest')",
            )
            .bind(wallet)
            .bind(scope.tenant_id)
            .bind(&scope.env)
            .bind(account)
            .bind(&exchange)
            .execute(&pool)
            .await?;
            let source = format!("contract-source-{index}");
            sqlx::query("INSERT INTO mkt_event(env,tenant_id,source,source_event_id,symbol,event_type,event_at,available_at,seq,payload,run_id)
                VALUES($1,$2,$3,'fixture','BTCUSDT','market',$4,$4,0,$5,$6)")
                .bind(&scope.env).bind(scope.tenant_id).bind(&source).bind(now).bind(json!({"price":"100","synthetic":true})).bind(&run_id).execute(&pool).await?;
            inputs.push(DurableMessage {
                version: 2,
                scope: scope.clone(),
                workflow_id: Some(workflow_id),
                account_id: Some(account),
                node_id: 5,
                target_node_id: Some(6 + index * 10),
                run_id: run_id.clone(),
                seq: index,
                attempt_no: 0,
                kind: MessageKind::Event,
                event_time: now,
                available_time: now,
                produced_at: now,
                traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
                payload: TradeIntent {
                    execution_group: Some(ExecutionGroupLeg {
                        group_seq: 0,
                        coordinator_node_id: 6,
                        leg_index: index as u16,
                        leg_count: 2,
                        deadline: now + Duration::seconds(5),
                        failure_policy: policy,
                    }),
                    wallet_id: wallet,
                    account_id: account,
                    exchange,
                    symbol: "BTCUSDT".into(),
                    side: OrderSide::Buy,
                    quantity: Decimal::ONE,
                    order_type: IntentOrderType::Market,
                    limit_price: None,
                    reduce_only: false,
                    market: MarketEvidence {
                        source,
                        event_time: now,
                        available_time: now,
                        price: Decimal::from(100),
                    },
                },
            });
        }
        Ok(Self {
            pool,
            storage,
            scope,
            workflow_id,
            run_id,
            now,
            inputs,
        })
    }

    fn coordinator(&self, workflow_id: Uuid) -> ExecutionGroups {
        ExecutionGroups::new(
            &self.storage,
            self.scope.clone(),
            workflow_id,
            6,
            format!("order:{}:{}:{workflow_id}:6", self.scope.env, self.scope.tenant_id),
            false,
        )
    }

    async fn register(&self, input: &DurableMessage<TradeIntent>, at: DateTime<Utc>) -> Result<()> {
        let node_id = input.target_node_id.unwrap();
        let consumer = OrderEventHandler::owner(&input.scope, input.workflow_id.unwrap(), node_id);
        let topic = WorkflowTopic::StrategySignal.topic(&input.scope, input.workflow_id.unwrap())?;
        let wire = MessageEnvelope::from_durable(
            input
                .clone()
                .map_payload(|intent| serde_json::to_value(intent).unwrap()),
        )?;
        match self
            .storage
            .admit_signal(&consumer, &topic, &wire, input, node_id, &[6], at)
            .await?
        {
            SignalAdmission::Accepted | SignalAdmission::Duplicate => Ok(()),
            SignalAdmission::Rejected(reason) => anyhow::bail!(reason),
        }
    }

    async fn register_all(&self) -> Result<()> {
        for input in &self.inputs {
            self.register(input, self.now).await?;
        }
        Ok(())
    }

    async fn order(&self, input: &DurableMessage<TradeIntent>, state: &str) -> Result<()> {
        let orders = OrderRecordHandler::new(&self.storage);
        orders
            .accept_with(input, |_| Ok(("USDT".into(), Decimal::from(100))))
            .await?;
        sqlx::query("UPDATE ord_order SET state=$1,exchange_order_id=$2 WHERE wallet_id=$3 AND producer_node_id=$4 AND run_id=$5 AND seq=$6")
            .bind(state)
            .bind(format!("venue-{}-{}", input.node_id, input.seq))
            .bind(input.payload.wallet_id)
            .bind(input.node_id)
            .bind(&input.run_id)
            .bind(input.seq)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn reject(&self, input: &DurableMessage<TradeIntent>) -> Result<()> {
        let node_id = input.target_node_id.unwrap();
        let consumer = OrderEventHandler::owner(&input.scope, input.workflow_id.unwrap(), node_id);
        self.storage.reject_signal(&consumer, input, node_id, self.now).await
    }

    async fn booked(&self, input: &DurableMessage<TradeIntent>, quantity: Decimal) -> Result<()> {
        self.booked_line(input, quantity, 0).await
    }

    async fn booked_line(&self, input: &DurableMessage<TradeIntent>, quantity: Decimal, offset: i64) -> Result<()> {
        // This fixture is a committed Wallet fact, not a second accounting implementation.
        let seq = input.node_id * 1000 + input.seq + offset;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO wlt_journal(wallet_id,producer_node_id,run_id,seq,source,fact_ref,occurred_at,payload)
            VALUES($1,7,$2,$3,'execution',$4,$5,$6)",
        )
        .bind(input.payload.wallet_id)
        .bind(&self.run_id)
        .bind(seq)
        .bind(format!("booked-{seq}"))
        .bind(self.now)
        .bind(json!({"quantity":quantity,"reservation_node_id":input.node_id,"reservation_seq":input.seq}))
        .execute(&mut *tx)
        .await?;
        for (line, account, amount) in [
            (0, "available", -quantity * Decimal::from(100)),
            (1, "clearing", quantity * Decimal::from(100)),
        ] {
            sqlx::query("INSERT INTO wlt_posting(wallet_id,producer_node_id,run_id,seq,line_no,account,asset,amount) VALUES($1,7,$2,$3,$4,$5,'USDT',$6)")
                .bind(input.payload.wallet_id).bind(&self.run_id).bind(seq).bind(line).bind(account).bind(amount).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn state(&self) -> Result<String> {
        Ok(sqlx::query_scalar("SELECT state FROM ord_execution_group WHERE env=$1 AND tenant_id=$2 AND run_id=$3 AND producer_node_id=5 AND seq=0")
            .bind(&self.scope.env).bind(self.scope.tenant_id).bind(&self.run_id).fetch_one(&self.pool).await?)
    }

    async fn compensation(&self) -> Result<Vec<DurableMessage<TradeIntent>>> {
        let rows:Vec<Value>=sqlx::query_scalar("SELECT payload FROM msg_outbox WHERE env=$1 AND run_id=$2 AND node_id=6 AND topic LIKE '%/strategy/signal' ORDER BY seq")
            .bind(&self.scope.env).bind(&self.run_id).fetch_all(&self.pool).await?;
        rows.into_iter().map(|row| Ok(serde_json::from_value(row)?)).collect()
    }

    async fn cancelled(&self, index: usize, quantity: Decimal) -> Result<()> {
        let input = &self.inputs[index];
        self.order(input, "cancelled").await?;
        let mut status = input.clone().map_payload(|intent| {
            json!({"wallet_id":intent.wallet_id,"reservation_node_id":5,
            "reservation_seq":input.seq,"executed_quantity":quantity,"receipt":{"state":"cancelled"}})
        });
        status.node_id = input.target_node_id.unwrap();
        status.target_node_id = None;
        status.seq = 100;
        self.storage
            .messager()
            .enqueue(
                "external-status-contract",
                &AccountTopic::OrderEvent.topic(&self.scope, input.payload.account_id)?,
                &status,
            )
            .await?;
        Ok(())
    }

    async fn start_compensation(&self) -> Result<DurableMessage<TradeIntent>> {
        self.register_all().await?;
        self.order(&self.inputs[0], "filled").await?;
        self.booked(&self.inputs[0], Decimal::ONE).await?;
        self.reject(&self.inputs[1]).await?;
        self.coordinator(self.workflow_id).advance(self.now).await?;
        assert_eq!(self.state().await?, "compensating");
        let mut compensation = self.compensation().await?;
        assert_eq!(compensation.len(), 1);
        Ok(compensation.remove(0))
    }
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; synthetic participant states, actual group coordinator"]
async fn completed_group_requires_wallet_booking_and_workflow_owner() -> Result<()> {
    let fixture = GroupFixture::new("complete", GroupFailurePolicy::CompensateFilled).await?;
    fixture.register_all().await?;
    for input in &fixture.inputs {
        fixture.order(input, "filled").await?;
    }
    fixture.coordinator(Uuid::now_v7()).advance(fixture.now).await?;
    assert_eq!(fixture.state().await?, "pending");
    fixture.coordinator(fixture.workflow_id).advance(fixture.now).await?;
    assert_eq!(fixture.state().await?, "executing");
    for input in &fixture.inputs {
        fixture.booked(input, Decimal::ONE).await?;
    }
    fixture.coordinator(fixture.workflow_id).advance(fixture.now).await?;
    assert_eq!(fixture.state().await?, "completed");
    assert!(fixture.compensation().await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; synthetic participant states, actual group coordinator"]
async fn compensation_is_reduce_only_once_and_survives_coordinator_restart() -> Result<()> {
    let fixture = GroupFixture::new("compensate", GroupFailurePolicy::CompensateFilled).await?;
    let output = fixture.start_compensation().await?;
    assert_eq!(output.payload.side, OrderSide::Sell);
    assert_eq!(output.payload.quantity, Decimal::ONE);
    assert!(output.payload.reduce_only && output.payload.execution_group.is_none());
    assert!(fixture.storage.is_compensation(&output, 6).await?);
    let mut forged = output.clone();
    forged.payload.quantity = Decimal::from(2);
    assert!(!fixture.storage.is_compensation(&forged, 6).await?);
    fixture
        .register(&fixture.inputs[0], fixture.now + Duration::seconds(10))
        .await?;
    fixture.coordinator(fixture.workflow_id).advance(fixture.now).await?;
    let repeated = fixture.compensation().await?;
    assert_eq!(repeated.len(), 1);
    assert_eq!(repeated[0].seq, output.seq);
    fixture.order(&output, "filled").await?;
    fixture.booked(&output, Decimal::ONE).await?;
    fixture.coordinator(fixture.workflow_id).advance(fixture.now).await?;
    assert_eq!(fixture.state().await?, "compensated");
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; synthetic participant states, actual group coordinator"]
async fn unknown_and_partial_fills_wait_for_terminal_cumulative_quantity() -> Result<()> {
    let fixture = GroupFixture::new("partial", GroupFailurePolicy::CompensateFilled).await?;
    fixture.register_all().await?;
    fixture.order(&fixture.inputs[0], "filled").await?;
    fixture.booked(&fixture.inputs[0], Decimal::ONE).await?;
    fixture.order(&fixture.inputs[1], "unknown").await?;
    let after_deadline = fixture.now + Duration::seconds(6);
    fixture.coordinator(fixture.workflow_id).advance(after_deadline).await?;
    assert_eq!(fixture.state().await?, "reconciling");
    assert!(fixture.compensation().await?.is_empty());
    fixture.order(&fixture.inputs[1], "partially_filled").await?;
    fixture.booked(&fixture.inputs[1], Decimal::new(5, 1)).await?;
    fixture.coordinator(fixture.workflow_id).advance(after_deadline).await?;
    assert!(fixture.compensation().await?.is_empty());
    fixture.cancelled(1, Decimal::new(75, 2)).await?; // A late fill is still outstanding.
    fixture.coordinator(fixture.workflow_id).advance(after_deadline).await?;
    assert_eq!(fixture.state().await?, "reconciling");
    assert!(fixture.compensation().await?.is_empty());
    fixture
        .booked_line(&fixture.inputs[1], Decimal::new(25, 2), 100)
        .await?;
    fixture.coordinator(fixture.workflow_id).advance(after_deadline).await?;
    assert_eq!(fixture.state().await?, "compensating");
    let mut quantities: Vec<_> = fixture
        .compensation()
        .await?
        .into_iter()
        .map(|message| message.payload.quantity)
        .collect();
    quantities.sort();
    assert_eq!(quantities, vec![Decimal::new(75, 2), Decimal::ONE]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; synthetic participant states, actual group coordinator"]
async fn hold_policy_never_emits_compensation() -> Result<()> {
    let fixture = GroupFixture::new("hold", GroupFailurePolicy::Hold).await?;
    fixture.register_all().await?;
    fixture.order(&fixture.inputs[0], "filled").await?;
    fixture.booked(&fixture.inputs[0], Decimal::ONE).await?;
    fixture.reject(&fixture.inputs[1]).await?;
    fixture.coordinator(fixture.workflow_id).advance(fixture.now).await?;
    assert_eq!(fixture.state().await?, "held");
    assert!(fixture.compensation().await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; synthetic participant states, actual group coordinator"]
async fn rejected_compensation_is_a_visible_terminal_failure() -> Result<()> {
    let fixture = GroupFixture::new("rejected", GroupFailurePolicy::CompensateFilled).await?;
    let output = fixture.start_compensation().await?;
    let owner = format!(
        "order:{}:{}:{}:{}",
        fixture.scope.env,
        fixture.scope.tenant_id,
        fixture.workflow_id,
        output.target_node_id.unwrap()
    );
    for (consumer, expected_state) in [
        ("unrelated-consumer", "compensating"),
        (owner.as_str(), "compensation_failed"),
    ] {
        let durable = output
            .clone()
            .map_payload(|intent| serde_json::to_value(intent).unwrap());
        let wire = MessageEnvelope {
            durable,
            request_span_id: None,
        };
        fixture
            .storage
            .messager()
            .accept(
                consumer,
                &WorkflowTopic::StrategySignal.topic(&fixture.scope, fixture.workflow_id)?,
                &wire,
            )
            .await?;
        sqlx::query("UPDATE msg_inbox SET state='quarantined',completed_at=NOW() WHERE env=$1 AND consumer=$2 AND run_id=$3 AND node_id=6 AND seq=$4")
            .bind(&fixture.scope.env).bind(consumer).bind(&fixture.run_id).bind(output.seq).execute(&fixture.pool).await?;
        fixture.coordinator(fixture.workflow_id).advance(fixture.now).await?;
        assert_eq!(fixture.state().await?, expected_state);
    }
    assert_eq!(fixture.compensation().await?.len(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; synthetic participant states, actual group coordinator"]
async fn immutable_leg_identity_and_admission_deadline_are_enforced() -> Result<()> {
    let fixture = GroupFixture::new("identity", GroupFailurePolicy::Hold).await?;
    fixture.register(&fixture.inputs[0], fixture.now).await?;
    let mut changed = fixture.inputs[0].clone();
    changed.payload.quantity = Decimal::from(2);
    assert!(fixture.register(&changed, fixture.now).await.is_err());
    assert!(fixture
        .register(&fixture.inputs[1], fixture.now + Duration::seconds(6))
        .await
        .is_err());
    let mut untrusted = fixture.inputs[1].clone();
    untrusted.payload.execution_group.as_mut().unwrap().coordinator_node_id = 99;
    assert!(fixture.register(&untrusted, fixture.now).await.is_err());
    fixture
        .register(&fixture.inputs[0], fixture.now + Duration::seconds(10))
        .await?;
    assert_eq!(fixture.state().await?, "pending");
    Ok(())
}
