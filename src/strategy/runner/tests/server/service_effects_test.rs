//! Real PostgreSQL and the restricted Rust/Python worker; no mocked transaction.
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sigbot_strategy_runner::{
    executor::runtime_postgres::PostgresStrategyTransaction,
    server::strategy_node::{StrategyNode, StrategyNodeConfig},
};
use sigbot_strategy_sdk::sdk::runtime::{StrategyCheckpoint, StrategyIdentity, StrategyTransaction};
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{AccountTopic, MessageScope, WorkflowEdge, WorkflowTopic},
    },
    strategy::runtime::Effects,
};
use sqlx::{PgPool, Row};
use std::{collections::BTreeMap, sync::Arc};

struct Fixture {
    pool: PgPool,
    config: StrategyNodeConfig,
    now: DateTime<Utc>,
}
impl Fixture {
    async fn new() -> Result<Self> {
        std::env::set_var(
            "SIGBOT_STRATEGY_WORKER_BINARY",
            env!("CARGO_BIN_EXE_sigbot-strategy-worker"),
        );
        let pool = PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
        let now = Utc::now();
        let scope = MessageScope {
            env: format!("effects-{}", now.timestamp_nanos_opt().unwrap()),
            tenant_id: 910005,
            space_id: 1,
        };
        let workflow_id: i64 = sqlx::query_scalar("INSERT INTO s_workflow(env,tenant_id,space_id,name) VALUES($1,$2,1,'strategy service effects') RETURNING id")
            .bind(&scope.env).bind(scope.tenant_id).fetch_one(&pool).await?;
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
        let account_id: i64 = sqlx::query_scalar("SELECT nextval(pg_get_serial_sequence('s_wallets','id'))")
            .fetch_one(&pool)
            .await?;
        sqlx::query("INSERT INTO s_wallets(id,account_id,tenant_id,env,space_id,exchange,mode) VALUES($1,$1,$2,$3,1,'binance','paper')")
            .bind(account_id).bind(scope.tenant_id.to_string()).bind(&scope.env).execute(&pool).await?;
        let config = StrategyNodeConfig {
            identity: StrategyIdentity { scope, workflow_id, node_id: 5, run_id },
            account_id,
            execution_node_ids: vec![6], order_node_ids: vec![6], wallet_node_ids: vec![7],
            code: r#"
from sigbot_sdk import Effects
class Example:
    def on_data(self, ctx, event):
        kind = event.data.kind
        ctx.state['user'].setdefault('callbacks', []).append(kind)
        ctx.state['user']['portfolio_version'] = ctx.portfolio.version
        if kind == 'execution':
            return Effects(requests=[
                {'kind': 'notification', 'query': {'channel': 'in_app', 'recipient': 'trader', 'template': 'fill', 'variables': {'symbol': event.symbol}}},
                {'kind': 'export', 'query': {'range': 'Trades!A2:B2', 'rows': [[event.symbol, event.data.value.quantity]]}}
            ])
        return Effects()
"#.into(),
            class_name: "Example".into(), configuration: BTreeMap::new(), log_archive: None, virtual_clock: false,
            edges: vec![WorkflowEdge { source_node_id: 8, target_node_id: 5, kind: WorkflowTopic::NotificationResult },
                WorkflowEdge { source_node_id: 9, target_node_id: 5, kind: WorkflowTopic::ExportResult }],
        };
        Ok(Self { pool, config, now })
    }
    fn node(&self) -> Result<StrategyNode> {
        StrategyNode::new(
            self.pool.clone(),
            self.config.clone(),
            Arc::new(|_| Ok("00-11111111111111111111111111111111-3333333333333333-01".into())),
        )
    }
    fn wire(&self, node_id: i64, seq: i64, payload: Value) -> Result<MessageEnvelope<Value>> {
        MessageEnvelope::from_durable(DurableMessage {
            version: 2,
            scope: self.config.identity.scope.clone(),
            workflow_id: Some(self.config.identity.workflow_id),
            account_id: Some(self.config.account_id),
            node_id,
            target_node_id: Some(5),
            run_id: self.config.identity.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: self.now,
            available_time: self.now,
            produced_at: self.now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload,
        })
    }
    fn execution(&self) -> Result<MessageEnvelope<Value>> {
        self.wire(6, 1, json!({"reservation_node_id":5,"reservation_seq":0,"wallet_id":self.config.account_id,
            "account_id":self.config.account_id,"exchange":"binance","exchange_order_id":"order-1","exchange_trade_id":"fill-1",
            "symbol":"BTCUSDT","base_asset":"BTC","quote_asset":"USDT","side":"BUY","price":"100","quantity":"1",
            "quote_quantity":"100","fee":"0.1","fee_asset":"USDT","order_complete":true,"executed_at":self.now}))
    }
    async fn checkpoint(&self) -> Result<Value> {
        Ok(
            sqlx::query_scalar("SELECT state FROM workflow_checkpoint WHERE env=$1 AND node_id=5")
                .bind(&self.config.identity.scope.env)
                .fetch_one(&self.pool)
                .await?,
        )
    }
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL and the actual restricted Rust/Python worker"]
async fn account_callbacks_commit_requests_once_and_service_results_return_through_on_data() -> Result<()> {
    let f = Fixture::new().await?;
    let execution = f.execution()?;
    let bytes = serde_json::to_vec(&execution)?;
    assert!(f.node()?.consume_account(AccountTopic::ExecutionEvent, &bytes).await?);
    assert!(
        !f.node()?.consume_account(AccountTopic::ExecutionEvent, &bytes).await?,
        "restart must not repeat callbacks or requests"
    );
    let outputs = sqlx::query("SELECT topic,payload FROM messager_outbox WHERE env=$1 ORDER BY seq")
        .bind(&f.config.identity.scope.env)
        .fetch_all(&f.pool)
        .await?;
    assert_eq!(outputs.len(), 2);
    for (row, suffix) in outputs.iter().zip(["/notification/request", "/export/request"]) {
        assert!(row.get::<String, _>("topic").ends_with(suffix));
        let payload: Value = row.get("payload");
        assert_eq!(payload["kind"], "request");
        assert!(payload.get("requestSpanId").is_none());
        let wire = MessageEnvelope::from_durable(serde_json::from_value::<DurableMessage<Value>>(payload)?)?;
        assert_eq!(wire.request_span_id.as_deref(), Some("3333333333333333"));
    }
    assert_eq!(
        f.checkpoint().await?["state"]["user"]["callbacks"],
        json!(["execution"])
    );
    let order = f.wire(6, 2, json!({"reservation_node_id":5,"reservation_seq":0,"wallet_id":f.config.account_id,
        "account_id":f.config.account_id,"exchange":"binance","receipt":{"exchange_order_id":"order-1","state":"filled"},
        "executed_quantity":"1","occurred_at":f.now}))?;
    assert!(
        f.node()?
            .consume_account(AccountTopic::OrderEvent, &serde_json::to_vec(&order)?)
            .await?
    );
    let portfolio = f.wire(
        7,
        1,
        json!({"account_id":f.config.account_id,"version":1,"balances":[],"positions":[]}),
    )?;
    assert!(
        f.node()?
            .consume_account(AccountTopic::PortfolioSnapshot, &serde_json::to_vec(&portfolio)?)
            .await?
    );
    for (node, topic) in [(8, WorkflowTopic::NotificationResult), (9, WorkflowTopic::ExportResult)] {
        let mut result = f.wire(node, 1, json!({"status":"delivered"}))?;
        result.durable.kind = MessageKind::Result;
        result.request_span_id = Some("3333333333333333".into());
        assert!(f.node()?.consume(topic, &serde_json::to_vec(&result)?).await?);
    }
    let checkpoint = f.checkpoint().await?;
    assert_eq!(checkpoint["version"], 5);
    assert_eq!(
        checkpoint["state"]["user"]["callbacks"],
        json!(["execution", "order_status", "portfolio", "query_result", "query_result"])
    );
    let mut unauthorized = execution.clone();
    unauthorized.durable.node_id = 99;
    assert!(
        !f.node()?
            .consume_account(AccountTopic::ExecutionEvent, &serde_json::to_vec(&unauthorized)?)
            .await?
    );
    let mut changed = execution;
    changed.durable.payload["quantity"] = json!("2");
    assert!(f
        .node()?
        .consume_account(AccountTopic::ExecutionEvent, &serde_json::to_vec(&changed)?)
        .await
        .is_err());
    assert_eq!(f.checkpoint().await?, checkpoint);
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; real failed checkpoint transaction"]
async fn lost_checkpoint_owner_rolls_back_inbox_and_all_service_requests() -> Result<()> {
    let f = Fixture::new().await?;
    f.node()?
        .consume_account(AccountTopic::ExecutionEvent, &serde_json::to_vec(&f.execution()?)?)
        .await?;
    let store = PostgresStrategyTransaction::new(
        f.pool.clone(),
        f.config.edges.clone(),
        Arc::new(|parent| Ok(parent.to_owned())),
    );
    let input = f
        .wire(
            6,
            99,
            json!({"data":{"kind":"portfolio","value":{}},"source":"wallet","event_time":f.now,"available_time":f.now}),
        )?
        .durable;
    let effects: Effects = serde_json::from_value(
        json!({"intents":[],"timers":[],"requests":[{"kind":"export","query":{"range":"A3:B3","rows":[["BTCUSDT",2]]}}]}),
    )?;
    assert!(store
        .commit(
            &f.config.identity,
            &input,
            0,
            StrategyCheckpoint {
                version: 1,
                ..Default::default()
            },
            effects
        )
        .await
        .is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM messager_inbox WHERE env=$1 AND seq=99")
        .bind(&f.config.identity.scope.env)
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(count, 0);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM messager_outbox WHERE env=$1")
        .bind(&f.config.identity.scope.env)
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(count, 2);
    assert_eq!(f.checkpoint().await?["version"], 1);
    Ok(())
}
