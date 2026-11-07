//! Actual PostgreSQL intake routing. No worker is started for another run.
use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use serde_json::Value;
use sigbot_core::config::config::{PostgresPropertiesBase, PostgresStorageProperties};
use sigbot_strategy_runner::strategy::StrategyNode;
use sigbot_strategy_runner::strategy::StrategyNodeConfig;
use sigbot_strategy_sdk::sdk::runtime::StrategyIdentity;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowEdge;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::Uuid;
use sqlx::PgPool;
use sqlx::Row;
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

struct RoutingFixture {
    pool: PgPool,
    storage: PostgresStorageProperties,
    config: StrategyNodeConfig,
    input: MessageEnvelope<Value>,
    next_run: Uuid,
}

impl RoutingFixture {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("MARKETPULSE_DATABASE_URL")?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect(&database_url)
            .await?;
        let storage = Self::storage(&database_url)?;
        let now = Utc::now();
        let scope = MessageScope {
            env: format!("replay-routing-{}", now.timestamp_micros()),
            tenant_id: Uuid::now_v7(),
        };
        let workflow_id = Uuid::now_v7();
        let current_run = Uuid::now_v7();
        let previous_run = Uuid::now_v7();
        let next_run = Uuid::now_v7();
        sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name) VALUES($1,$2,$3,'strategy routing test')")
            .bind(workflow_id)
            .bind(&scope.env)
            .bind(scope.tenant_id)
            .execute(&pool)
            .await?;
        for run_id in [current_run, previous_run, next_run] {
            sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,$3,$4,'running')")
                .bind(run_id)
                .bind(workflow_id)
                .bind(&scope.env)
                .bind(scope.tenant_id)
                .execute(&pool)
                .await?;
        }
        let config = StrategyNodeConfig {
            identity: StrategyIdentity {
                scope: scope.clone(),
                workflow_id,
                node_id: 5,
                run_id: current_run,
            },
            account_id: Uuid::now_v7(),
            execution_node_ids: vec![],
            order_node_ids: vec![],
            wallet_node_ids: vec![],
            code: "class TestStrategy: pass".into(),
            class_name: "TestStrategy".into(),
            configuration: BTreeMap::new(),
            edges: vec![WorkflowEdge {
                source_node_id: 3,
                target_node_id: 5,
                kind: WorkflowTopic::FeedMarket,
            }],
            log_archive: None,
            virtual_clock: true,
        };
        let input = MessageEnvelope::from_durable(DurableMessage {
            version: 2,
            scope,
            workflow_id: Some(workflow_id),
            account_id: None,
            node_id: 3,
            target_node_id: Some(5),
            run_id: previous_run,
            seq: 7,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: json!({"source":"binance","symbol":"BTCUSDT","source_event_id":"recorded-market-7",
                "event_time":now,"available_time":now,"event_type":"market","seq":7,"raw":{"price":"100"}}),
        })?;
        Ok(Self {
            pool,
            storage,
            config,
            input,
            next_run,
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

    async fn node(&self, config: StrategyNodeConfig) -> Result<StrategyNode> {
        StrategyNode::new(&self.storage, config, Arc::new(|parent| Ok(parent.to_owned()))).await
    }
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; routing completes without Python, Wallet or a replay checkpoint"]
async fn previous_run_is_durably_quarantined_per_instance_before_clock_or_callback() -> Result<()> {
    let fixture = RoutingFixture::new().await?;
    let node = fixture.node(fixture.config.clone()).await?;
    let bytes = serde_json::to_vec(&fixture.input)?;
    assert!(!node.consume(WorkflowTopic::FeedMarket, &bytes).await?);
    assert!(!node.consume(WorkflowTopic::FeedMarket, &bytes).await?);
    // A separate current run keeps its own routing decision. Neither creates or
    // completes the original run's normal strategy.5 Inbox entry.
    let mut next_config = fixture.config.clone();
    next_config.identity.run_id = fixture.next_run;
    assert!(
        !fixture
            .node(next_config)
            .await?
            .consume(WorkflowTopic::FeedMarket, &bytes)
            .await?
    );
    let rows =
        sqlx::query("SELECT consumer,state,payload,quarantine_reason FROM msg_inbox WHERE env=$1 ORDER BY consumer")
            .bind(&fixture.config.identity.scope.env)
            .fetch_all(&fixture.pool)
            .await?;
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert!(row
            .get::<String, _>("consumer")
            .starts_with(&format!("strategy.routing:{}:5:", fixture.config.identity.workflow_id)));
        assert_eq!(row.get::<String, _>("state"), "quarantined");
        assert_eq!(row.get::<String, _>("quarantine_reason"), "inactive_run");
        let payload: Value = row.get("payload");
        assert_eq!(payload["run_id"], fixture.input.durable.run_id.to_string());
        assert!(payload.get("requestSpanId").is_none());
    }
    let mut changed = fixture.input.clone();
    changed.durable.payload["raw"]["price"] = json!("101");
    assert!(
        node.consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&changed)?)
            .await
            .is_err(),
        "quarantine must still reject a reused identity with different facts"
    );
    let mut current = fixture.input.clone();
    current.durable.run_id = fixture.config.identity.run_id;
    assert!(
        node.consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&current)?)
            .await
            .is_err(),
        "this run still requires its own persisted replay clock"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; actual strategy routing authorization"]
async fn routing_quarantine_cannot_bypass_scope_or_workflow() -> Result<()> {
    let fixture = RoutingFixture::new().await?;
    let node = fixture.node(fixture.config.clone()).await?;
    for variant in 0..2 {
        let mut wire = fixture.input.clone();
        match variant {
            0 => wire.durable.scope.tenant_id = Uuid::now_v7(),
            _ => wire.durable.workflow_id = Some(Uuid::now_v7()),
        }
        assert!(node
            .consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&wire)?)
            .await
            .is_err());
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM msg_inbox WHERE env=$1")
        .bind(&fixture.config.identity.scope.env)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(
        count, 0,
        "unauthorized messages must never be admitted as successful quarantine"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires PostgreSQL migrated through 20261028-15; two real strategy routing instances"]
async fn same_topic_other_targets_and_edges_are_durable_without_consuming_the_intended_inbox() -> Result<()> {
    let fixture = RoutingFixture::new().await?;
    let first = fixture.node(fixture.config.clone()).await?;
    let mut second_config = fixture.config.clone();
    second_config.identity.node_id = 6;
    second_config.edges = vec![WorkflowEdge {
        source_node_id: 4,
        target_node_id: 6,
        kind: WorkflowTopic::FeedMarket,
    }];
    let second = fixture.node(second_config).await?;
    let mut to_first = fixture.input.clone();
    to_first.durable.run_id = fixture.config.identity.run_id.clone();
    let mut to_second = to_first.clone();
    to_second.durable.node_id = 4;
    to_second.durable.target_node_id = Some(6);
    assert!(
        !first
            .consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&to_second)?)
            .await?
    );
    assert!(
        !second
            .consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&to_first)?)
            .await?
    );
    // Untargeted fanout is still routed by each instance's explicit input edge.
    to_first.durable.target_node_id = None;
    to_first.durable.seq += 1;
    assert!(
        !second
            .consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&to_first)?)
            .await?
    );
    assert!(
        !second
            .consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&to_first)?)
            .await?
    );
    let rows = sqlx::query(
        "SELECT consumer,node_id,seq,quarantine_reason FROM msg_inbox WHERE env=$1 ORDER BY consumer,node_id,seq",
    )
    .bind(&fixture.config.identity.scope.env)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].get::<String, _>("consumer"),
        format!(
            "strategy.routing:{}:5:{}",
            fixture.config.identity.workflow_id, fixture.config.identity.run_id
        )
    );
    assert_eq!(rows[0].get::<String, _>("quarantine_reason"), "other_target");
    assert_eq!(
        rows[1].get::<String, _>("consumer"),
        format!(
            "strategy.routing:{}:6:{}",
            fixture.config.identity.workflow_id, fixture.config.identity.run_id
        )
    );
    assert_eq!(rows[1].get::<String, _>("quarantine_reason"), "other_target");
    assert_eq!(rows[2].get::<String, _>("quarantine_reason"), "outside_input_edge");
    let normal: i64 =
        sqlx::query_scalar("SELECT count(*) FROM msg_inbox WHERE env=$1 AND consumer IN ('strategy.5','strategy.6')")
            .bind(&fixture.config.identity.scope.env)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        normal, 0,
        "routing cannot mark the intended strategy's normal delivery done"
    );
    // The authorized route passes admission and still demands its own virtual
    // checkpoint. A quarantined copy in the other instance cannot bypass it.
    assert!(first
        .consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&to_first)?)
        .await
        .is_err());
    assert!(second
        .consume(WorkflowTopic::FeedMarket, &serde_json::to_vec(&to_second)?)
        .await
        .is_err());
    Ok(())
}
