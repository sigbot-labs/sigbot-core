//! Real PostgreSQL and the restricted Rust/Python worker; no mocked transaction.
use anyhow::{Context, Result};
use chrono::DateTime;
use chrono::Utc;
use serde_json::json;
use serde_json::Value;
use sigbot_core::config::config::{PostgresPropertiesBase, PostgresStorageProperties};
use sigbot_strategy_runner::checkpoint::checkpoint::CheckpointStrategyTransaction;
use sigbot_strategy_runner::strategy::StrategyNode;
use sigbot_strategy_runner::strategy::StrategyNodeConfig;
use sigbot_strategy_sdk::sdk::runtime::StrategyCheckpoint;
use sigbot_strategy_sdk::sdk::runtime::StrategyIdentity;
use sigbot_strategy_sdk::sdk::runtime::StrategyTransaction;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::AccountTopic;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowEdge;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::strategy::execution::Effects;
use sigbot_types::Uuid;
use sqlx::PgPool;
use sqlx::Row;
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

struct Fixture {
    pool: PgPool,
    storage: PostgresStorageProperties,
    config: StrategyNodeConfig,
    wallet_id: Uuid,
    now: DateTime<Utc>,
}
impl Fixture {
    async fn new() -> Result<Self> {
        std::env::set_var(
            "SIGBOT_STRATEGY_WORKER_BINARY",
            env!("CARGO_BIN_EXE_sigbot-strategy-worker"),
        );
        let database_url = std::env::var("MARKETPULSE_DATABASE_URL")?;
        let pool = PgPool::connect(&database_url).await?;
        let storage = Self::storage(&database_url)?;
        let now = Utc::now();
        let scope = MessageScope {
            env: format!("effects-{}", now.timestamp_nanos_opt().unwrap()),
            tenant_id: Uuid::now_v7(),
        };
        let workflow_id = Uuid::now_v7();
        sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name) VALUES($1,$2,$3,'strategy service effects')")
            .bind(workflow_id)
            .bind(&scope.env)
            .bind(scope.tenant_id)
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
        let account_id = Uuid::now_v7();
        let wallet_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO ledger_info(id,account_id,tenant_id,env,exchange,mode) VALUES($1,$2,$3,$4,'binance','paper')",
        )
        .bind(wallet_id)
        .bind(account_id)
        .bind(scope.tenant_id)
        .bind(&scope.env)
        .execute(&pool)
        .await?;
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
        Ok(Self {
            pool,
            storage,
            config,
            wallet_id,
            now,
        })
    }
    fn storage(database_url: &str) -> Result<PostgresStorageProperties> {
        let url = Url::parse(database_url)?;
        Ok(PostgresStorageProperties {
            inner: PostgresPropertiesBase {
                url: Some(database_url.to_owned()),
                host: url.host_str().unwrap_or("127.0.0.1").to_owned(),
                port: url.port().unwrap_or(5432),
                database: url.path().trim_start_matches('/').to_owned(),
                schema: "public".to_owned(),
                username: url.username().to_owned(),
                password: url.password().map(str::to_owned),
                min_connections: Some(1),
                max_connections: Some(3),
                use_ssl: false,
            },
        })
    }
    async fn node(&self) -> Result<StrategyNode> {
        StrategyNode::new(
            &self.storage,
            self.config.clone(),
            Arc::new(|_| Ok("00-11111111111111111111111111111111-3333333333333333-01".into())),
        )
        .await
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
        self.wire(6, 1, json!({"reservation_node_id":5,"reservation_seq":0,"wallet_id":self.wallet_id,
            "account_id":self.config.account_id,"exchange":"binance","exchange_order_id":"order-1","exchange_trade_id":"fill-1",
            "symbol":"BTCUSDT","base_asset":"BTC","quote_asset":"USDT","side":"BUY","price":"100","quantity":"1",
            "quote_quantity":"100","fee":"0.1","fee_asset":"USDT","order_complete":true,"executed_at":self.now}))
    }
    async fn checkpoint(&self) -> Result<Value> {
        Ok(
            sqlx::query_scalar("SELECT state FROM wf_checkpoint WHERE env=$1 AND node_id=5")
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
    let node = f.node().await?;
    assert!(node.consume_account(AccountTopic::ExecutionEvent, &bytes).await?);
    assert!(
        !node.consume_account(AccountTopic::ExecutionEvent, &bytes).await?,
        "duplicate delivery must not repeat callbacks or requests"
    );
    let outputs = sqlx::query("SELECT topic,payload FROM msg_outbox WHERE env=$1 ORDER BY seq")
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
        node.consume_account(AccountTopic::OrderEvent, &serde_json::to_vec(&order)?)
            .await?
    );
    let portfolio = f.wire(
        7,
        1,
        json!({"account_id":f.config.account_id,"version":1,"balances":[],"positions":[]}),
    )?;
    assert!(
        node.consume_account(AccountTopic::PortfolioSnapshot, &serde_json::to_vec(&portfolio)?)
            .await?
    );
    for (source_node, topic) in [(8, WorkflowTopic::NotificationResult), (9, WorkflowTopic::ExportResult)] {
        let mut result = f.wire(source_node, 1, json!({"status":"delivered"}))?;
        result.durable.kind = MessageKind::Result;
        result.request_span_id = Some("3333333333333333".into());
        assert!(node.consume(topic, &serde_json::to_vec(&result)?).await?);
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
        !node
            .consume_account(AccountTopic::ExecutionEvent, &serde_json::to_vec(&unauthorized)?)
            .await?
    );
    let mut changed = execution;
    changed.durable.payload["quantity"] = json!("2");
    assert!(node
        .consume_account(AccountTopic::ExecutionEvent, &serde_json::to_vec(&changed)?)
        .await
        .is_err());
    assert_eq!(f.checkpoint().await?, checkpoint);
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; real failed checkpoint transaction"]
async fn lost_checkpoint_owner_rolls_back_inbox_and_all_service_requests() -> Result<()> {
    let f = Fixture::new().await.context("create rollback fixture")?;
    let node = f.node().await.context("create rollback strategy node")?;
    let execution = serde_json::to_vec(&f.execution()?)?;
    node.consume_account(AccountTopic::ExecutionEvent, &execution)
        .await
        .context("commit initial execution callback")?;
    drop(node);
    let store = CheckpointStrategyTransaction::connect(
        &f.storage,
        f.config.edges.clone(),
        Arc::new(|parent| Ok(parent.to_owned())),
        false,
    )
    .await
    .context("connect rollback transaction")?;
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
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM msg_inbox WHERE env=$1 AND seq=99")
        .bind(&f.config.identity.scope.env)
        .fetch_one(&f.pool)
        .await
        .context("read rolled back inbox count")?;
    assert_eq!(count, 0);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM msg_outbox WHERE env=$1")
        .bind(&f.config.identity.scope.env)
        .fetch_one(&f.pool)
        .await
        .context("read preserved outbox count")?;
    assert_eq!(count, 2);
    assert_eq!(f.checkpoint().await?["version"], 1);
    Ok(())
}
