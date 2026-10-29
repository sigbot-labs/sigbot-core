//! Requires the isolated dual-extension fixture; ordinary PostgreSQL cannot pass.
use anyhow::{ensure, Result};
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sigbot_datafeed::server::compute_query::ComputeExecutor;
use sigbot_types::modules::{
    messager::topic::MessageScope,
    strategy::runtime::{Aggregation, ComputePlan, StrategyEventData},
};
use sqlx::{PgPool, Postgres, Transaction};

struct ExtensionsFixture;
impl ExtensionsFixture {
    async fn pool() -> Result<PgPool> {
        let pool = PgPool::connect(&std::env::var("MARKETPULSE_DATABASE_URL")?).await?;
        let extensions: Vec<String> = sqlx::query_scalar(
            "SELECT extname FROM pg_extension WHERE extname IN ('timescaledb','vector') ORDER BY extname",
        )
        .fetch_all(&pool)
        .await?;
        ensure!(
            extensions == ["timescaledb", "vector"],
            "both real PostgreSQL extensions must be installed"
        );
        Ok(pool)
    }
    fn plan(as_of: DateTime<Utc>, window_ms: u64) -> ComputePlan {
        ComputePlan {
            history_from: as_of - Duration::days(32),
            as_of,
            sources: vec!["extensions-fixture".into()],
            symbols: vec!["A".into(), "B".into()],
            window_ms,
            group_by: vec!["symbol".into()],
            aggregations: vec![
                Aggregation::Count,
                Aggregation::Sum,
                Aggregation::Mean,
                Aggregation::First,
                Aggregation::Last,
            ],
            predict: None,
            max_rows: 100,
            max_bytes: 65536,
        }
    }
    async fn fallback(
        tx: &mut Transaction<'_, Postgres>,
        scope: &MessageScope,
        plan: &ComputePlan,
    ) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar(&ComputeExecutor::statement(plan, false))
            .bind(&scope.env)
            .bind(scope.tenant_id)
            .bind(scope.space_id)
            .bind(&plan.sources)
            .bind(&plan.symbols)
            .bind(plan.history_from)
            .bind(plan.as_of)
            .bind(plan.window_ms as f64 / 1000.0)
            .bind(i64::from(plan.max_rows) + 1)
            .fetch_all(&mut **tx)
            .await?)
    }
}

#[tokio::test]
#[ignore = "requires actual isolated PostgreSQL with TimescaleDB and pgvector installed"]
async fn actual_timescale_and_pgvector_functions_execute() -> Result<()> {
    let pool = ExtensionsFixture::pool().await?;
    let distance: f64 = sqlx::query_scalar("SELECT '[1,2,3]'::vector <-> '[4,6,3]'::vector")
        .fetch_one(&pool)
        .await?;
    assert_eq!(distance, 5.0);
    let (explicit,postgres,default):(DateTime<Utc>,DateTime<Utc>,DateTime<Utc>)=sqlx::query_as(
        "SELECT time_bucket(INTERVAL '7 days',TIMESTAMPTZ '2026-10-28 12:00:00+00',TIMESTAMPTZ '1970-01-01 00:00:00+00'),
        date_bin(INTERVAL '7 days',TIMESTAMPTZ '2026-10-28 12:00:00+00',TIMESTAMPTZ '1970-01-01 00:00:00+00'),
        time_bucket(INTERVAL '7 days',TIMESTAMPTZ '2026-10-28 12:00:00+00')").fetch_one(&pool).await?;
    assert_eq!(explicit, postgres);
    assert_ne!(
        default, postgres,
        "fixture must expose the previously different weekly origin"
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires migrated isolated PostgreSQL with TimescaleDB and pgvector installed"]
async fn actual_timescale_compute_matches_postgres_for_multiday_windows_and_revisions() -> Result<()> {
    let pool = ExtensionsFixture::pool().await?;
    let mut tx = pool.begin().await?;
    let scope = MessageScope {
        env: format!("extensions-{}", Utc::now().timestamp_micros()),
        tenant_id: 1,
        space_id: 1,
    };
    let as_of: DateTime<Utc> = "2026-10-28T12:00:00Z".parse()?;
    for day in 1..=30_i64 {
        for symbol in ["A", "B"] {
            let event = as_of - Duration::days(day);
            sqlx::query("INSERT INTO market_event(env,tenant_id,space_id,source,source_event_id,symbol,event_type,event_at,available_at,seq,revision,payload,run_id)
                VALUES($1,1,1,'extensions-fixture',$2,$3,'market',$4,$4,$5,0,$6,'extensions-compute')")
                .bind(&scope.env).bind(format!("{symbol}-{day}")).bind(symbol).bind(event).bind(day)
                .bind(json!({"price":day.to_string()})).execute(&mut *tx).await?;
        }
    }
    for (revision, available, price) in [
        (1, as_of - Duration::seconds(1), "100"),
        (2, as_of + Duration::seconds(1), "1000"),
    ] {
        sqlx::query("INSERT INTO market_event(env,tenant_id,space_id,source,source_event_id,symbol,event_type,event_at,available_at,seq,revision,payload,run_id)
            VALUES($1,1,1,'extensions-fixture','A-1','A','market',$2,$3,1,$4,$5,'extensions-compute')")
            .bind(&scope.env).bind(as_of-Duration::days(1)).bind(available).bind(revision).bind(json!({"price":price}))
            .execute(&mut *tx).await?;
    }
    for window_ms in [7 * 86_400_000, 3 * 86_400_000, 25 * 3_600_000, 365 * 86_400_000] {
        let plan = ExtensionsFixture::plan(as_of, window_ms);
        let StrategyEventData::Compute { rows, revision, .. } =
            ComputeExecutor::execute(&mut tx, &scope, &plan, as_of).await?
        else {
            panic!("expected computed rows")
        };
        assert_eq!(revision, 1, "future revision is not available to this plan");
        assert!(!rows.is_empty());
        assert_eq!(
            rows,
            ExtensionsFixture::fallback(&mut tx, &scope, &plan).await?,
            "engine changed windows at {window_ms} ms"
        );
        let count: i64 = rows.iter().map(|row| row["count"].as_i64().unwrap()).sum();
        assert_eq!(count, 60);
        let sum: i64 = rows
            .iter()
            .map(|row| row["sum"].as_str().unwrap().parse::<i64>().unwrap())
            .sum();
        assert_eq!(sum, 1029, "latest available revision replaces one original observation");
    }
    tx.rollback().await?;
    pool.close().await;
    Ok(())
}
