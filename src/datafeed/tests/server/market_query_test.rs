use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sigbot_datafeed::server::{market_query::MarketQueryRuntime, persisted_ingestor::PersistedDatafeedIngestor};
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{MessageScope, WorkflowTopic},
    },
    strategy::runtime::{SnapshotMode, SnapshotQuery, StrategyEvent, StrategyEventData},
};
use std::collections::HashMap;

struct QueryFixture {
    pool: sqlx::PgPool,
    properties: HashMap<String, String>,
    scope: MessageScope,
    workflow_id: i64,
    run_id: String,
}

impl QueryFixture {
    async fn create() -> anyhow::Result<Self> {
        let pool = sqlx::PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
        let scope = MessageScope {
            env: format!("e2e-active-query-{}", Utc::now().timestamp_micros()),
            tenant_id: 1,
            space_id: 1,
        };
        let workflow_id: i64 = sqlx::query_scalar(
            "INSERT INTO s_workflow(env,tenant_id,space_id,name) VALUES($1,1,1,'active query contract') RETURNING id",
        )
        .bind(&scope.env)
        .fetch_one(&pool)
        .await?;
        let run_id = format!("{}-{workflow_id}", scope.env);
        sqlx::query(
            "INSERT INTO workflow_run(run_id,workflow_id,env,tenant_id,space_id,state) VALUES($1,$2,$3,1,1,'running')",
        )
        .bind(&run_id)
        .bind(workflow_id)
        .bind(&scope.env)
        .execute(&pool)
        .await?;
        let properties = [
            ("env", scope.env.clone()),
            ("tenant_id", "1".into()),
            ("space_id", "1".into()),
            ("workflow_id", workflow_id.to_string()),
            ("node_id", "3".into()),
            ("run_id", run_id.clone()),
            ("database_url_env", "MARKETPULSE_DATABASE_URL".into()),
            ("snapshot_input_node_ids", "5".into()),
            (
                "traceparent",
                "00-11111111111111111111111111111111-2222222222222222-01".into(),
            ),
            (
                "active_sources",
                json!([
                    {"source":"binance","provider":"binance_spot","symbols":{"HYPEUSDC":"HYPEUSDC"}},
                    {"source":"hyperliquid","provider":"hyperliquid_spot","symbols":{"HYPEUSDC":"@107"}}
                ])
                .to_string(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();
        Ok(Self {
            pool,
            properties,
            scope,
            workflow_id,
            run_id,
        })
    }

    fn request(&self, seq: i64, query: SnapshotQuery, now: DateTime<Utc>) -> anyhow::Result<Vec<u8>> {
        let message = DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: Some(self.workflow_id),
            account_id: None,
            node_id: 5,
            target_node_id: Some(3),
            run_id: self.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Request,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: self.properties["traceparent"].clone(),
            payload: serde_json::to_value(query)?,
        };
        Ok(serde_json::to_vec(&MessageEnvelope::from_durable(message)?)?)
    }

    async fn replies(&self) -> anyhow::Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT payload FROM messager_outbox WHERE env=$1 AND run_id=$2 AND topic LIKE '%/feed/snapshot/result' ORDER BY seq")
            .bind(&self.scope.env).bind(&self.run_id).fetch_all(&self.pool).await?)
    }
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; AS_OF deliberately has no configured active provider for its requested source"]
async fn as_of_uses_only_persisted_facts_and_duplicate_request_keeps_canonical_reply() -> anyhow::Result<()> {
    let fixture = QueryFixture::create().await?;
    let as_of = DateTime::from_timestamp_millis((Utc::now() - Duration::minutes(1)).timestamp_millis()).unwrap();
    let ingestor = PersistedDatafeedIngestor::from_properties(&fixture.properties, "fixture").await?;
    for (id, available, price) in [("known", as_of, "79"), ("future", as_of + Duration::seconds(1), "1000")] {
        ingestor
            .ingest(&serde_json::to_vec(
                &json!({"source":"history-fixture","source_event_id":id,
            "symbol":"HYPEUSDC","event_type":"market","event_time":as_of,"available_time":available,
            "seq":if id=="known" {1} else {2},"raw":{"price":price}}),
            )?)
            .await?;
    }
    let query = SnapshotQuery {
        sources: vec!["history-fixture".into()],
        symbols: vec!["HYPEUSDC".into()],
        mode: SnapshotMode::AsOf(as_of),
        max_age_ms: 5_000,
        max_skew_ms: 5_000,
        max_rows: 1,
    };
    let request = fixture.request(0, query, as_of)?;
    let runtime = MarketQueryRuntime::from_properties(&fixture.properties).await?;
    runtime.consume(WorkflowTopic::FeedSnapshotRequest, &request).await?;
    let replies = fixture.replies().await?;
    assert_eq!(replies.len(), 1);
    let event: StrategyEvent = serde_json::from_value(replies[0]["payload"].clone())?;
    let StrategyEventData::Snapshot(result) = event.data else {
        panic!("AS_OF must not call an active provider")
    };
    assert_eq!(result.points[0].price.to_string(), "79");
    assert!(replies[0].get("requestSpanId").is_none());
    drop(runtime);
    let restarted = MarketQueryRuntime::from_properties(&fixture.properties).await?;
    restarted.consume(WorkflowTopic::FeedSnapshotRequest, &request).await?;
    assert_eq!(fixture.replies().await?, replies);
    let missing = SnapshotQuery {
        sources: vec!["binance".into(), "unconfigured".into()],
        symbols: vec!["HYPEUSDC".into()],
        mode: SnapshotMode::Latest,
        max_age_ms: 5_000,
        max_skew_ms: 5_000,
        max_rows: 2,
    };
    restarted
        .consume(
            WorkflowTopic::FeedSnapshotRequest,
            &fixture.request(1, missing, Utc::now())?,
        )
        .await?;
    let replies = fixture.replies().await?;
    let failure: StrategyEvent = serde_json::from_value(replies[1]["payload"].clone())?;
    assert!(
        matches!(failure.data, StrategyEventData::QueryFailure { .. }),
        "missing source cannot fall back to PG or partial success"
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_event WHERE env=$1")
        .bind(&fixture.scope.env)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(count, 2, "failed active request must not write partial facts");
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL and real read-only Binance + Hyperliquid APIs; no external mocks or funds"]
async fn active_latest_commits_two_real_sources_and_reply_once_across_restart() -> anyhow::Result<()> {
    let fixture = QueryFixture::create().await?;
    let query = SnapshotQuery {
        sources: vec!["binance".into(), "hyperliquid".into()],
        symbols: vec!["HYPEUSDC".into()],
        mode: SnapshotMode::Latest,
        max_age_ms: 300_000,
        max_skew_ms: 300_000,
        max_rows: 2,
    };
    let request = fixture.request(0, query, Utc::now())?;
    let runtime = MarketQueryRuntime::from_properties(&fixture.properties).await?;
    runtime.consume(WorkflowTopic::FeedSnapshotRequest, &request).await?;
    let replies = fixture.replies().await?;
    assert_eq!(replies.len(), 1);
    let event: StrategyEvent = serde_json::from_value(replies[0]["payload"].clone())?;
    let StrategyEventData::Snapshot(result) = event.data else {
        panic!("real public source request failed: {:?}", replies[0])
    };
    assert_eq!(result.points.len(), 2);
    for point in result.points {
        assert!(point.event_time <= point.available_time && point.available_time <= event.available_time);
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_event WHERE env=$1")
        .bind(&fixture.scope.env)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(count, 2);
    let deliveries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_event_delivery WHERE env=$1 AND run_id=$2")
        .bind(&fixture.scope.env)
        .bind(&fixture.run_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(deliveries, 2);
    let state: String =
        sqlx::query_scalar("SELECT state FROM messager_inbox WHERE env=$1 AND run_id=$2 AND node_id=5 AND seq=0")
            .bind(&fixture.scope.env)
            .bind(&fixture.run_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(state, "done");
    drop(runtime);
    let restarted = MarketQueryRuntime::from_properties(&fixture.properties).await?;
    restarted.consume(WorkflowTopic::FeedSnapshotRequest, &request).await?;
    assert_eq!(fixture.replies().await?, replies);
    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_event WHERE env=$1")
        .bind(&fixture.scope.env)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(after, count);
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; verifies symbol-local source sequences and canonical query order"]
async fn independent_symbol_checkpoints_and_reversed_queries_preserve_results() -> anyhow::Result<()> {
    let fixture = QueryFixture::create().await?;
    let as_of = DateTime::from_timestamp_millis((Utc::now() - Duration::minutes(1)).timestamp_millis()).unwrap();
    let ingestor = PersistedDatafeedIngestor::from_properties(&fixture.properties, "fixture").await?;
    for (symbol, seq, price) in [("HYPEUSDC", 100, "79"), ("BTCUSDC", 1, "68000")] {
        ingestor
            .ingest(&serde_json::to_vec(&json!({"source":"history-fixture",
            "source_event_id":format!("{symbol}:trade:{seq}"),"symbol":symbol,"event_type":"market",
            "event_time":as_of,"available_time":as_of,"seq":seq,"raw":{"price":price}}))?)
            .await?;
    }
    // Native trade IDs must not share a cursor with a legacy ticker's timestamp sequence.
    for (symbol, seq, price) in [("HYPEUSDC", 7, "79"), ("BTCUSDC", 2, "68000")] {
        ingestor
            .ingest(&serde_json::to_vec(&json!({"source":"history-fixture",
            "source_event_id":format!("binance_spot:{symbol}:trade:{seq}"),"symbol":symbol,"event_type":"market",
            "event_time":as_of,"available_time":as_of,"seq":seq,
            "raw":{"price":price,"price_type":"last_trade","provider":"binance_spot"}}))?)
            .await?;
    }
    let checkpoints: Vec<(String, i64)> = sqlx::query_as(
        "SELECT partition_key,seq FROM workflow_checkpoint WHERE env=$1 AND run_id=$2 ORDER BY partition_key",
    )
    .bind(&fixture.scope.env)
    .bind(&fixture.run_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        checkpoints,
        vec![
            ("market:history-fixture:BTCUSDC".into(), 1),
            ("market:history-fixture:BTCUSDC:last_trade".into(), 2),
            ("market:history-fixture:HYPEUSDC".into(), 100),
            ("market:history-fixture:HYPEUSDC:last_trade".into(), 7)
        ]
    );
    let runtime = MarketQueryRuntime::from_properties(&fixture.properties).await?;
    let mut query = SnapshotQuery {
        sources: vec!["history-fixture".into()],
        symbols: vec!["HYPEUSDC".into(), "BTCUSDC".into()],
        mode: SnapshotMode::AsOf(as_of),
        max_age_ms: 5_000,
        max_skew_ms: 5_000,
        max_rows: 2,
    };
    runtime
        .consume(
            WorkflowTopic::FeedSnapshotRequest,
            &fixture.request(0, query.clone(), as_of)?,
        )
        .await?;
    query.symbols.reverse();
    runtime
        .consume(WorkflowTopic::FeedSnapshotRequest, &fixture.request(1, query, as_of)?)
        .await?;
    let replies = fixture.replies().await?;
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0]["payload"]["data"], replies[1]["payload"]["data"]);
    assert_eq!(replies[0]["payload"]["data"]["value"]["points"][0]["symbol"], "BTCUSDC");
    Ok(())
}
