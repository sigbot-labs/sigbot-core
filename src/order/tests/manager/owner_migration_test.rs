//! Actual PostgreSQL migration in a transaction-local schema; no live orders.
use anyhow::Result;
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};

struct Fixture;
impl Fixture {
    const MIGRATION: &'static str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tooling/deploy/migrations/postgresql/20261028-17/messager.workflow_owner.ddl.sql"
    ));
    async fn transaction(suffix: &str) -> Result<Transaction<'static, Postgres>> {
        let pool = PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
        let mut tx = pool.begin().await?;
        let schema = format!("owner_migration_{}_{suffix}", chrono::Utc::now().timestamp_micros());
        sqlx::raw_sql(&format!("CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema};
            CREATE TABLE messager_inbox (LIKE public.messager_inbox INCLUDING ALL);
            CREATE TABLE messager_outbox (LIKE public.messager_outbox INCLUDING ALL);
            CREATE TABLE workflow_run(run_id TEXT PRIMARY KEY,workflow_id BIGINT,env TEXT,tenant_id BIGINT,space_id BIGINT);
            INSERT INTO workflow_run VALUES('run-one',101,'paper',1,1),('run-two',202,'paper',1,1);")).execute(&mut *tx).await?;
        Ok(tx)
    }
    async fn inbox(
        tx: &mut Transaction<'_, Postgres>,
        consumer: &str,
        run: &str,
        state: &str,
        workflow: Option<i64>,
        value: i64,
    ) -> Result<()> {
        sqlx::query("INSERT INTO messager_inbox(env,tenant_id,space_id,consumer,node_id,run_id,seq,topic,payload,state,attempt_no) VALUES('paper',1,1,$1,5,$2,0,'topic',$3,$4,2)")
            .bind(consumer).bind(run).bind(json!({"workflow_id":workflow,"payload":{"amount":value}})).bind(state).execute(&mut **tx).await?;
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires migrated isolated PostgreSQL; transaction rolls back its own schema"]
async fn migration_keeps_completed_dedup_splits_same_node_workflows_and_preserves_suffixes() -> Result<()> {
    let mut tx = Fixture::transaction("valid").await?;
    Fixture::inbox(&mut tx, "order:paper:1:1:6", "run-one", "done", Some(101), 1).await?;
    Fixture::inbox(&mut tx, "order:paper:1:1:101:6", "run-one", "pending", Some(101), 1).await?;
    Fixture::inbox(&mut tx, "order:paper:1:1:6", "run-two", "pending", Some(202), 2).await?;
    Fixture::inbox(&mut tx, "order:paper:1:1:6:reservation", "run-one", "done", None, 1).await?;
    Fixture::inbox(&mut tx, "order.execution:paper:1:1:6", "run-one", "done", Some(101), 1).await?;
    sqlx::query("INSERT INTO messager_outbox(env,tenant_id,space_id,producer,node_id,run_id,seq,topic,payload,state) VALUES('paper',1,1,'order:paper:1:1:6:venue',6,'run-one',1,'topic',$1,'sent'),('paper',1,1,'export:paper:1:1:9',9,'run-two',1,'topic',$2,'pending')")
        .bind(json!({"workflow_id":null,"payload":{"fee":"0.1"}}))
        .bind(json!({"workflow_id":202,"payload":{"rows":1}})).execute(&mut *tx).await?;
    sqlx::raw_sql(Fixture::MIGRATION).execute(&mut *tx).await?;
    let inbox: Vec<(String, String, String)> =
        sqlx::query_as("SELECT consumer,run_id,state FROM messager_inbox ORDER BY consumer,run_id")
            .fetch_all(&mut *tx)
            .await?;
    assert_eq!(inbox.len(), 4);
    assert!(inbox.contains(&("order:paper:1:1:101:6".into(), "run-one".into(), "done".into())));
    assert!(inbox.contains(&("order:paper:1:1:202:6".into(), "run-two".into(), "pending".into())));
    assert!(inbox.contains(&(
        "order:paper:1:1:101:6:reservation".into(),
        "run-one".into(),
        "done".into()
    )));
    assert!(inbox.contains(&(
        "order.execution:paper:1:1:101:6".into(),
        "run-one".into(),
        "done".into()
    )));
    let outbox: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT producer,state,(payload->>'workflow_id')::bigint FROM messager_outbox ORDER BY producer",
    )
    .fetch_all(&mut *tx)
    .await?;
    assert!(outbox.contains(&("order:paper:1:1:101:6:venue".into(), "sent".into(), 101)));
    assert!(outbox.contains(&("export:paper:1:1:202:9".into(), "pending".into(), 202)));
    let amount: String =
        sqlx::query_scalar("SELECT payload->'payload'->>'fee' FROM messager_outbox WHERE producer LIKE 'order:%'")
            .fetch_one(&mut *tx)
            .await?;
    assert_eq!(
        amount, "0.1",
        "domain financial facts must not be changed by owner migration"
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated isolated PostgreSQL; transaction rolls back its own schema"]
async fn migration_refuses_conflicting_facts_instead_of_resetting_deduplication() -> Result<()> {
    let mut tx = Fixture::transaction("conflict").await?;
    Fixture::inbox(&mut tx, "order:paper:1:1:6", "run-one", "done", Some(101), 1).await?;
    Fixture::inbox(&mut tx, "order:paper:1:1:101:6", "run-one", "done", Some(101), 2).await?;
    assert!(sqlx::raw_sql(Fixture::MIGRATION).execute(&mut *tx).await.is_err());
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated isolated PostgreSQL; transaction rolls back its own schema"]
async fn migration_refuses_invalid_or_unrecoverable_workflow_binding() -> Result<()> {
    for (label, run, workflow) in [("invalid", "run-one", Some(0)), ("unbound", "unknown-run", None)] {
        let mut tx = Fixture::transaction(label).await?;
        Fixture::inbox(&mut tx, "order:paper:1:1:6", run, "done", workflow, 1).await?;
        assert!(sqlx::raw_sql(Fixture::MIGRATION).execute(&mut *tx).await.is_err());
        tx.rollback().await?;
    }
    Ok(())
}
