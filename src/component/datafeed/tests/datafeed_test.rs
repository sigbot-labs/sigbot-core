use chrono::Utc;
use serde_json::json;
use serde_json::Value;
use sigbot_datafeed::datafeed::PersistedDatafeedIngestor;
use sigbot_types::Uuid;
use std::collections::HashMap;

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; verifies actual source revision and delivery transactions"]
async fn source_revisions_preserve_canonical_delivery_and_allocate_distinct_sequences() -> anyhow::Result<()> {
    let pool = sqlx::PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
    let env = format!("e2e-source-revision-{}", Utc::now().timestamp_micros());
    let tenant_id = Uuid::now_v7();
    let workflow = Uuid::now_v7();
    sqlx::query("INSERT INTO workflow_info(id,env,tenant_id,name) VALUES($1,$2,$3,'revision contract')")
        .bind(workflow)
        .bind(&env)
        .bind(tenant_id)
        .execute(&pool)
        .await?;
    let run = Uuid::now_v7();
    sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,$3,$4,'running')")
        .bind(&run)
        .bind(workflow)
        .bind(&env)
        .bind(tenant_id)
        .execute(&pool)
        .await?;
    let properties: HashMap<String, String> = [
        ("env", env.clone()),
        ("tenant_id", tenant_id.to_string()),
        ("workflow_id", workflow.to_string()),
        ("node_id", "1".into()),
        ("run_id", run.to_string()),
        (
            "traceparent",
            "00-11111111111111111111111111111111-2222222222222222-01".into(),
        ),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect();
    let ingestor = PersistedDatafeedIngestor::from_properties(&properties, "contract").await?;
    let mut event = json!({"source":"contract","source_event_id":"one","symbol":"*","event_type":"news",
        "event_time":"2026-10-27T00:00:00Z","available_time":"2026-10-27T00:00:01Z","seq":7,
        "content":"original","raw":{"content":"original"}});
    ingestor.ingest(&serde_json::to_vec(&event)?).await?;
    event["available_time"] = json!("2026-10-28T00:00:00Z");
    ingestor.ingest(&serde_json::to_vec(&event)?).await?;
    event["revision"] = json!(1);
    event["content"] = json!("explicit correction");
    event["raw"] = json!({"content":"explicit correction"});
    ingestor.ingest(&serde_json::to_vec(&event)?).await?;
    ingestor.ingest(&serde_json::to_vec(&event)?).await?;
    event["source_event_id"] = json!("two");
    event["revision"] = json!(0);
    ingestor.ingest(&serde_json::to_vec(&event)?).await?;
    event["source"] = json!("another-source");
    ingestor.ingest(&serde_json::to_vec(&event)?).await?;
    event["raw"] = json!({"content":"unversioned mutation"});
    assert!(ingestor.ingest(&serde_json::to_vec(&event)?).await.is_err());
    let rows: Vec<Value> =
        sqlx::query_scalar("SELECT payload FROM mkt_delivery WHERE env=$1 AND run_id=$2 ORDER BY message_seq")
            .bind(&env)
            .bind(&run)
            .fetch_all(&pool)
            .await?;
    assert_eq!(rows.len(), 4);
    let sequences: std::collections::HashSet<_> = rows.iter().map(|row| row["seq"].as_i64().unwrap()).collect();
    assert_eq!(sequences.len(), 4);
    assert!(
        rows[0]["payload"].get("revision").is_none(),
        "base revision keeps canonical wire shape"
    );
    assert_eq!(rows[0]["payload"]["raw"]["content"], "original");
    assert_eq!(rows[0]["available_time"], "2026-10-27T00:00:01Z");
    assert_eq!(rows[1]["payload"]["revision"], 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM msg_outbox WHERE env=$1 AND run_id=$2")
        .bind(&env)
        .bind(&run)
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 4);
    Ok(())
}
