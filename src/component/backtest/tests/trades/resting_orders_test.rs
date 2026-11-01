//! Synthetic historical ticks exercise the real PostgreSQL matching transaction.
use anyhow::Result;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use sigbot_backtest::trades::trades::BacktestExchange;
use sigbot_backtest::trades::trades::MatchingConfig;
use sigbot_types::datafeed::event::DatafeedEvent;
use sigbot_types::exchange::instrument::InstrumentSpec;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::order::intent::IntentOrderType;
use sigbot_types::order::intent::MarketEvidence;
use sigbot_types::order::intent::OrderSide;
use sigbot_types::order::intent::TradeIntent;
use sigbot_types::Uuid;
use sqlx::PgPool;
use sqlx::Row;

struct Fixture {
    pool: PgPool,
    database_url: String,
    config: MatchingConfig,
    order: DurableMessage<TradeIntent>,
}
impl Fixture {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("MARKETPULSE_DATABASE_URL")?;
        let pool = PgPool::connect(&database_url).await?;
        std::env::set_var("SIGBOT__SECRETS__VALUES__BACKTEST_TEST_TOKEN", "local-matching-test");
        sigbot_core::config::config::refresh_config()?;
        let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
        let tenant_id = Uuid::now_v7();
        let workflow_id = Uuid::now_v7();
        let run_id = Uuid::now_v7();
        let account_id = Uuid::now_v7();
        let wallet_id = Uuid::now_v7();
        let scope = MessageScope {
            env: format!("resting-{}", now.timestamp_micros()),
            tenant_id,
        };
        sqlx::query("INSERT INTO wf_workflow(id,env,tenant_id,name) VALUES($1,$2,$3,'resting order test')")
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
        sqlx::query("INSERT INTO wlt_wallet(id,account_id,tenant_id,env,exchange,mode) VALUES($1,$2,$3,$4,'binance','backtest')")
            .bind(wallet_id).bind(account_id).bind(scope.tenant_id).bind(&scope.env).execute(&pool).await?;
        let config = MatchingConfig {
            scope: scope.clone(),
            node_id: 7,
            fee_bps: Decimal::from(10),
            slippage_bps: Decimal::from(10),
            instrument: InstrumentSpec {
                exchange: "binance".into(),
                symbol: "BTCUSDT".into(),
                base_asset: "BTC".into(),
                quote_asset: "USDT".into(),
                quantity_step: Decimal::new(1, 3),
                price_tick: Decimal::new(1, 2),
                min_notional: Decimal::ONE,
            },
            token_secret: "backtest_test_token".into(),
            virtual_clock: false,
        };
        let order = DurableMessage {
            version: 2,
            scope,
            workflow_id: Some(workflow_id),
            account_id: Some(account_id),
            node_id: 5,
            target_node_id: Some(6),
            run_id,
            seq: 1,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: TradeIntent {
                execution_group: None,
                account_id,
                wallet_id,
                exchange: "binance".into(),
                symbol: "BTCUSDT".into(),
                side: OrderSide::Buy,
                quantity: Decimal::ONE,
                order_type: IntentOrderType::Limit,
                limit_price: Some(Decimal::from(95)),
                reduce_only: false,
                market: MarketEvidence {
                    source: "binance".into(),
                    price: Decimal::from(100),
                    event_time: now,
                    available_time: now,
                },
            },
        };
        Ok(Self {
            pool,
            database_url,
            config,
            order,
        })
    }
    async fn tick(&self, sequence: i64, price: i64, time: DateTime<Utc>) -> Result<MessageEnvelope<DatafeedEvent>> {
        let event = DatafeedEvent {
            source: "binance".into(),
            source_event_id: format!("historical-{sequence}"),
            symbol: "BTCUSDT".into(),
            event_type: "market".into(),
            revision: 0,
            event_time: time,
            available_time: time,
            seq: sequence,
            content: None,
            raw: json!({"price":price.to_string()}),
        };
        sqlx::query("INSERT INTO mkt_event(env,tenant_id,source,source_event_id,symbol,event_type,event_at,available_at,seq,payload,run_id)
            VALUES($1,$2,$3,$4,$5,'market',$6,$6,$7,$8,$9)")
            .bind(&self.config.scope.env).bind(self.config.scope.tenant_id).bind(&event.source).bind(&event.source_event_id)
            .bind(&event.symbol).bind(time).bind(sequence).bind(&event.raw).bind(self.order.run_id).execute(&self.pool).await?;
        MessageEnvelope::from_durable(DurableMessage {
            version: 2,
            scope: self.config.scope.clone(),
            workflow_id: self.order.workflow_id,
            account_id: None,
            node_id: 3,
            target_node_id: None,
            run_id: self.order.run_id.clone(),
            seq: sequence,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: time,
            available_time: time,
            produced_at: time,
            traceparent: self.order.traceparent.clone(),
            payload: event,
        })
    }
}

#[tokio::test]
#[ignore = "requires migrated MARKETPULSE_DATABASE_URL"]
async fn limit_waits_for_a_later_crossing_tick_and_restart_cannot_duplicate_its_fill() -> Result<()> {
    let f = Fixture::new().await?;
    let exchange = BacktestExchange::connect(&f.database_url, f.config.clone()).await?;
    exchange.submit(&f.order, f.order.available_time).await?;
    let first_time = f.order.available_time + Duration::seconds(1);
    let first = f.tick(10, 96, first_time).await?;
    assert_eq!(exchange.match_tick(&first, first_time).await?, 0);
    let crossing_time = first_time + Duration::seconds(1);
    let crossing = f.tick(11, 94, crossing_time).await?;
    assert!(exchange.match_tick(&crossing, first_time).await.is_err());
    assert_eq!(exchange.match_tick(&crossing, crossing_time).await?, 1);
    let restarted = BacktestExchange::connect(&f.database_url, f.config.clone()).await?;
    assert_eq!(restarted.match_tick(&crossing, crossing_time).await?, 0);
    restarted.submit(&f.order, crossing_time).await?;
    let outputs: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT payload FROM msg_outbox WHERE env=$1 AND producer=$2")
            .bind(&f.config.scope.env)
            .bind(restarted.producer())
            .fetch_all(&f.pool)
            .await?;
    assert_eq!(outputs.len(), 1);
    let fact = &outputs[0]["payload"];
    assert_eq!(
        serde_json::from_value::<Decimal>(fact["price"].clone())?,
        Decimal::new(9410, 2)
    );
    assert_eq!(
        serde_json::from_value::<Decimal>(fact["quote_quantity"].clone())?,
        Decimal::new(9410, 2)
    );
    assert_eq!(
        serde_json::from_value::<Decimal>(fact["fee"].clone())?,
        Decimal::new(941, 4)
    );
    let row = sqlx::query("SELECT state,matched_at FROM bt_order WHERE env=$1 AND run_id=$2")
        .bind(&f.config.scope.env)
        .bind(&f.order.run_id)
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(row.get::<String, _>("state"), "filled");
    assert_eq!(row.get::<DateTime<Utc>, _>("matched_at"), crossing_time);
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated MARKETPULSE_DATABASE_URL"]
async fn late_historical_ticks_cannot_fill_an_order_accepted_after_their_event_time() -> Result<()> {
    let f = Fixture::new().await?;
    let exchange = BacktestExchange::connect(&f.database_url, f.config.clone()).await?;
    exchange.submit(&f.order, f.order.available_time).await?;
    let historical = f.tick(1, 90, f.order.available_time - Duration::seconds(1)).await?;
    assert_eq!(exchange.match_tick(&historical, f.order.available_time).await?, 0);
    let mut other = historical;
    other.durable.seq += 1;
    other.durable.workflow_id = Some(Uuid::now_v7());
    assert_eq!(exchange.match_tick(&other, f.order.available_time).await?, 0);
    Ok(())
}
