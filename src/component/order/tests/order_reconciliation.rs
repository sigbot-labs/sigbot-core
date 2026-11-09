use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use common_context::IProvider;
use rust_decimal::Decimal;
use serde_json::json;
use sigbot_core::modules::order::handler::order::{OrderHandler, OrderRecordHandler};
use sigbot_exchange::exchange::ExecutionMode;
use sigbot_exchange::exchange::IExecutionAdapter;
use sigbot_order::processing::processor::OrderProcessor;
use sigbot_order::processing::reservation::OrderReservation;
use sigbot_order::processing::risk::InstrumentRules;
use sigbot_order::processing::risk::RiskLimits;
use sigbot_order::processing::risk::RiskPolicy;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::execution::OrderReceipt;
use sigbot_types::order::execution::OrderState;
use sigbot_types::order::intent::IntentOrderType;
use sigbot_types::order::intent::MarketEvidence;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::Uuid;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

struct Venue {
    mode: ExecutionMode,
    known: AtomicBool,
    submissions: AtomicUsize,
    reconciliations: AtomicUsize,
}
impl Default for Venue {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Backtest,
            known: AtomicBool::new(false),
            submissions: AtomicUsize::new(0),
            reconciliations: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl IExecutionAdapter for Venue {
    fn mode(&self) -> ExecutionMode {
        self.mode
    }
    async fn submit(&self, _: &DurableMessage<TradeIntent>) -> Result<OrderReceipt> {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("unexpected duplicate submission")
    }
    async fn reconcile(&self, _: &DurableMessage<TradeIntent>) -> Result<Option<OrderReceipt>> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        Ok(self.known.load(Ordering::SeqCst).then(|| OrderReceipt {
            exchange_order_id: "venue-accepted".into(),
            state: OrderState::Acknowledged,
        }))
    }
}

struct Fixture;
impl Fixture {
    fn policy() -> RiskPolicy {
        RiskPolicy {
            instrument: InstrumentRules {
                exchange: "binance".into(),
                symbol: "BTCUSDT".into(),
                base_asset: "BTC".into(),
                quote_asset: "USDT".into(),
                quantity_step: Decimal::new(1, 3),
                price_tick: Decimal::new(1, 2),
                min_notional: Decimal::ONE,
            },
            limits: RiskLimits {
                max_order_notional: Decimal::from(1000),
                max_account_notional: Decimal::from(2000),
                max_position_quantity: Decimal::from(2),
                max_market_age_ms: 1000,
                fee_buffer_bps: Decimal::ZERO,
            },
        }
    }
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL via MARKETPULSE_DATABASE_URL; venue boundary is deliberately uncertain"]
async fn unknown_submission_only_reconciles_and_preparation_is_immutable() -> Result<()> {
    let database_url = std::env::var("MARKETPULSE_DATABASE_URL")?;
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let storage = OrderHandler::connect(&database_url).await?;
    let scope = MessageScope {
        env: "order-reconcile-test".into(),
        tenant_id: Uuid::now_v7(),
    };
    let now = Utc::now();
    let workflow_id = Uuid::now_v7();
    let run_id = Uuid::now_v7();
    let account = Uuid::now_v7();
    let wallet_id = Uuid::now_v7();
    sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name) VALUES($1,$2,$3,'order reconcile test')")
        .bind(workflow_id)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,$3,$4,'running')")
        .bind(run_id)
        .bind(workflow_id)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO ledger_info(id,tenant_id,exchange,mode,env,account_id) VALUES($1,$2,'binance','backtest',$3,$4)",
    )
    .bind(wallet_id)
    .bind(scope.tenant_id)
    .bind(&scope.env)
    .bind(account)
    .execute(&pool)
    .await?;
    let input = DurableMessage {
        version: 2,
        scope: scope.clone(),
        workflow_id: Some(workflow_id),
        account_id: Some(account),
        node_id: 1,
        target_node_id: Some(2),
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
            wallet_id,
            account_id: account,
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
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
    sqlx::query("INSERT INTO mkt_event(env,tenant_id,run_id,source,source_event_id,revision,symbol,event_type,event_at,available_at,seq,payload) VALUES($1,$2,$3,'binance',$4,0,'BTCUSDT','market',$5,$5,0,$6)")
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(input.run_id)
        .bind(input.run_id.to_string())
        .bind(now)
        .bind(json!({"price":"100"}))
        .execute(&pool)
        .await?;
    let repository = OrderRecordHandler::new(&storage);
    repository
        .accept_with(&input, |_| Ok(("USDT".into(), Decimal::from(100))))
        .await?;
    sqlx::query("UPDATE order_info SET state='unknown' WHERE wallet_id=$1 AND run_id=$2 AND seq=$3")
        .bind(wallet_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .execute(&pool)
        .await?;
    let producer = format!("order:{}:{}:{workflow_id}:2", scope.env, scope.tenant_id);
    let reservation = Arc::new(OrderReservation::new(
        &storage,
        scope.clone(),
        2,
        3,
        producer.clone(),
        workflow_id,
    )?);
    let venue = Arc::new(Venue::default());
    let processor = OrderProcessor::new(
        repository.clone(),
        reservation.clone(),
        venue.clone(),
        Fixture::policy(),
        2,
        scope.clone(),
        workflow_id,
    )?;
    let wrong_mode = Arc::new(Venue {
        mode: ExecutionMode::Paper,
        ..Venue::default()
    });
    let incompatible_processor = OrderProcessor::new(
        repository.clone(),
        reservation,
        wrong_mode.clone(),
        Fixture::policy(),
        2,
        scope.clone(),
        workflow_id,
    )?;
    assert!(
        incompatible_processor.process(&input, now).await.is_err(),
        "a resumed Unknown order must reject a different mode"
    );
    assert!(incompatible_processor.reconcile(&input, now).await.is_err());
    assert_eq!(wrong_mode.reconciliations.load(Ordering::SeqCst), 0);
    let mut incompatible = input.clone();
    incompatible.seq = 99;
    let error = incompatible_processor.process(&incompatible, now).await.unwrap_err();
    assert!(error
        .downcast_ref::<sigbot_order::processing::risk::OrderRiskRejection>()
        .is_some());
    assert!(
        repository.find(&incompatible).await?.is_none(),
        "mode mismatch must fail before accepting an intent"
    );
    assert_eq!(venue.submissions.load(Ordering::SeqCst), 0);
    assert_eq!(processor.process(&input, now).await?, OrderState::Unknown);
    venue.known.store(true, Ordering::SeqCst);
    assert_eq!(processor.process(&input, now).await?, OrderState::Acknowledged);
    assert_eq!(processor.process(&input, now).await?, OrderState::Acknowledged);
    assert_eq!(venue.submissions.load(Ordering::SeqCst), 0);
    assert_eq!(venue.reconciliations.load(Ordering::SeqCst), 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM wlt_journal WHERE wallet_id=$1")
            .bind(wallet_id)
            .fetch_one(&pool)
            .await?,
        0,
        "an acceptance receipt must never become a Wallet execution"
    );
    let mut prepared_input = input.clone();
    prepared_input.seq = 1;
    repository
        .accept_with(&prepared_input, |_| Ok(("USDT".into(), Decimal::from(100))))
        .await?;
    sqlx::query("UPDATE order_info SET state='reserved' WHERE wallet_id=$1 AND run_id=$2 AND seq=$3")
        .bind(wallet_id)
        .bind(&prepared_input.run_id)
        .bind(prepared_input.seq)
        .execute(&pool)
        .await?;
    let signed = json!({"venue_order_id":"prepared-order-test","payload":{"ciphertext":"opaque-test-payload"}});
    assert_eq!(repository.persist_prepared(&prepared_input, &signed).await?, signed);
    let loser = json!({"venue_order_id":"different-preparation","payload":{"ciphertext":"losing-candidate"}});
    assert_eq!(repository.persist_prepared(&prepared_input, &loser).await?, signed);
    assert!(
        sqlx::query("UPDATE order_info SET prepared_submission=$1 WHERE wallet_id=$2 AND seq=1")
            .bind(&loser)
            .bind(wallet_id)
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM msg_outbox WHERE producer=$1 AND run_id=$2")
        .bind(producer)
        .bind(&input.run_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM order_info WHERE wallet_id=$1")
        .bind(wallet_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM ledger_info WHERE id=$1")
        .bind(wallet_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM mkt_event WHERE env=$1 AND tenant_id=$2 AND run_id=$3")
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(&input.run_id)
        .execute(&pool)
        .await?;
    Ok(())
}

#[async_trait::async_trait]
impl IProvider for Venue {
    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
