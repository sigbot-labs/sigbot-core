use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use sigbot_core::modules::order::store::postgres::OrderPostgresRepository;
use sigbot_exchange::client::execution_adapter::{ExecutionMode, IExecutionAdapter};
use sigbot_order::manager::{
    order_processor::OrderProcessor,
    order_reservation::OrderReservation,
    risk::{InstrumentRules, RiskLimits, RiskPolicy},
};
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageKind},
        topic::MessageScope,
    },
    order::{
        execution::{OrderReceipt, OrderState},
        intent::{IntentOrderType, MarketEvidence, OrderSide, TradeIntent},
    },
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
struct Venue {
    known: AtomicBool,
    submissions: AtomicUsize,
    reconciliations: AtomicUsize,
}
#[async_trait]
impl IExecutionAdapter for Venue {
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Simulated
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
    let pool = sqlx::PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
    let scope = MessageScope {
        env: "order-reconcile-test".into(),
        tenant_id: 900002,
        space_id: 1,
    };
    let now = Utc::now();
    let account = now.timestamp_micros();
    let wallet_id:i64=sqlx::query_scalar("INSERT INTO s_wallets(tenant_id,exchange,mode,env,space_id,account_id) VALUES($1,'binance','paper',$2,1,$3) RETURNING id")
        .bind(scope.tenant_id.to_string()).bind(&scope.env).bind(account).fetch_one(&pool).await?;
    let input = DurableMessage {
        version: 2,
        scope: scope.clone(),
        workflow_id: Some(1),
        account_id: Some(account),
        node_id: 1,
        target_node_id: Some(2),
        run_id: format!("order-reconcile-{account}"),
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
    let repository = OrderPostgresRepository::new(pool.clone());
    let mut tx = pool.begin().await?;
    OrderPostgresRepository::insert(&mut tx, &input, "USDT", Decimal::from(100)).await?;
    OrderPostgresRepository::record(&mut tx, &input, "unknown", None).await?;
    tx.commit().await?;
    let producer = format!("order:{}:{}:{}:1:2", scope.env, scope.tenant_id, scope.space_id);
    let reservation = Arc::new(OrderReservation::new(
        pool.clone(),
        scope.clone(),
        2,
        3,
        producer.clone(),
        1,
    )?);
    let venue = Arc::new(Venue::default());
    let processor = OrderProcessor::new(
        repository.clone(),
        reservation,
        venue.clone(),
        Fixture::policy(),
        2,
        scope.clone(),
        1,
    )?;
    assert_eq!(processor.process(&input, now).await?, OrderState::Unknown);
    venue.known.store(true, Ordering::SeqCst);
    assert_eq!(processor.process(&input, now).await?, OrderState::Acknowledged);
    assert_eq!(processor.process(&input, now).await?, OrderState::Acknowledged);
    assert_eq!(venue.submissions.load(Ordering::SeqCst), 0);
    assert_eq!(venue.reconciliations.load(Ordering::SeqCst), 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM wallet_journal WHERE wallet_id=$1")
            .bind(wallet_id)
            .fetch_one(&pool)
            .await?,
        0,
        "an acceptance receipt must never become a Wallet execution"
    );
    let mut prepared_input = input.clone();
    prepared_input.seq = 1;
    let mut tx = pool.begin().await?;
    OrderPostgresRepository::insert(&mut tx, &prepared_input, "USDT", Decimal::from(100)).await?;
    OrderPostgresRepository::record(&mut tx, &prepared_input, "reserved", None).await?;
    tx.commit().await?;
    let signed = json!({"venue_order_id":"prepared-order-test","payload":{"ciphertext":"opaque-test-payload"}});
    assert_eq!(repository.persist_prepared(&prepared_input, &signed).await?, signed);
    let loser = json!({"venue_order_id":"different-preparation","payload":{"ciphertext":"losing-candidate"}});
    assert_eq!(repository.persist_prepared(&prepared_input, &loser).await?, signed);
    assert!(
        sqlx::query("UPDATE order_record SET prepared_submission=$1 WHERE wallet_id=$2 AND seq=1")
            .bind(&loser)
            .bind(wallet_id)
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM messager_outbox WHERE producer=$1 AND run_id=$2")
        .bind(producer)
        .bind(&input.run_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM order_record WHERE wallet_id=$1")
        .bind(wallet_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM s_wallets WHERE id=$1")
        .bind(wallet_id)
        .execute(&pool)
        .await?;
    Ok(())
}
