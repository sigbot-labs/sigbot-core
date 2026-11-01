use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rust_decimal::Decimal;
use sigbot_core::modules::datafeed::handler::query::MarketQueryHandler;
use sigbot_datafeed::query::market_query::ComputeExecutor;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::strategy::execution::Aggregation;
use sigbot_types::strategy::execution::ComputePlan;
use sigbot_types::strategy::execution::ModelPartitioning;
use sigbot_types::strategy::execution::Prediction;
use sigbot_types::strategy::execution::StrategyEventData;
use sigbot_types::Uuid;
use std::str::FromStr;

struct Plans;

impl Plans {
    fn base(as_of: DateTime<Utc>) -> ComputePlan {
        ComputePlan {
            history_from: as_of - Duration::hours(1),
            as_of,
            sources: vec!["fixture".to_owned()],
            symbols: vec!["A".to_owned(), "B".to_owned()],
            window_ms: 3_600_000,
            group_by: vec![],
            aggregations: vec![
                Aggregation::Count,
                Aggregation::Sum,
                Aggregation::Mean,
                Aggregation::First,
                Aggregation::Last,
            ],
            predict: Some(Prediction {
                model_version: "mean-price-v1".to_owned(),
                partitioning: ModelPartitioning::SingleOwner,
            }),
            max_rows: 10,
            max_bytes: 8192,
        }
    }
}

#[test]
fn reject_unregistered_model_and_group_expression() {
    let now = Utc::now();
    let mut plan = Plans::base(now);
    plan.group_by = vec!["symbol); DROP TABLE mkt_event;--".to_owned()];
    assert!(ComputeExecutor::validate(&plan, now).is_err());
    plan.group_by.clear();
    plan.predict.as_mut().unwrap().model_version = "unregistered-python".to_owned();
    assert!(ComputeExecutor::validate(&plan, now).is_err());
}

#[tokio::test]
#[ignore = "requires actual PostgreSQL with runtime migrations; market-query runner scenario"]
async fn postgres_late_revision_future_exclusion_and_partition_equivalence() -> anyhow::Result<()> {
    let database_url = std::env::var("MARKETPULSE_DATABASE_URL")?;
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let mut tx = pool.begin().await?;
    let scope = MessageScope {
        env: format!("e2e-compute-contract-{}", Utc::now().timestamp_micros()),
        tenant_id: Uuid::now_v7(),
    };
    let workflow_id = Uuid::now_v7();
    let run_id = Uuid::now_v7();
    sqlx::query("INSERT INTO wf_workflow(id,env,tenant_id,name) VALUES($1,$2,$3,'compute contract')")
        .bind(workflow_id)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,$3,$4,'running')")
        .bind(run_id)
        .bind(workflow_id)
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .execute(&mut *tx)
        .await?;
    let as_of: DateTime<Utc> = "2026-10-27T12:10:00Z".parse()?;
    for (id, symbol, minute, revision, price, available_minute) in [
        ("1", "A", 1, 0, "10", 1),
        ("2", "A", 2, 0, "14", 2),
        ("3", "B", 3, 0, "20", 3),
        ("4", "B", 4, 0, "24", 4),
        ("1", "A", 1, 1, "12", 9),
        ("future", "A", 5, 0, "1000", 11),
    ] {
        sqlx::query("INSERT INTO mkt_event(env,tenant_id,source,source_event_id,symbol,event_type,event_at,available_at,seq,revision,payload,run_id)
            VALUES($1,$2,'fixture',$3,$4,'market',$5,$6,$7,$8,$9,$10)")
            .bind(&scope.env).bind(scope.tenant_id).bind(id).bind(symbol).bind(as_of-Duration::minutes(10-minute))
            .bind(as_of-Duration::minutes(10-available_minute)).bind(minute).bind(revision)
            .bind(serde_json::json!({"price":price})).bind(run_id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    let handler = MarketQueryHandler::from_pool(pool.clone());
    let mut tx = handler.query().await?;
    let plan = Plans::base(as_of);
    let StrategyEventData::Compute {
        rows: whole, revision, ..
    } = ComputeExecutor::execute(&mut tx, &scope, &run_id, &plan, as_of).await?
    else {
        panic!()
    };
    assert_eq!(revision, 1);
    assert_eq!(whole[0]["count"], 4);
    assert_eq!(whole[0]["sum"], "70");
    assert_eq!(whole[0]["first"], "12");
    assert_eq!(whole[0]["last"], "24");
    let mut earlier = plan.clone();
    earlier.as_of = as_of - Duration::minutes(2);
    let StrategyEventData::Compute { rows: earlier, .. } =
        ComputeExecutor::execute(&mut tx, &scope, &run_id, &earlier, as_of).await?
    else {
        panic!()
    };
    assert_eq!(earlier[0]["sum"], "68");
    let mut partitioned = plan.clone();
    partitioned.group_by = vec!["symbol".to_owned()];
    partitioned.predict.as_mut().unwrap().partitioning = ModelPartitioning::IndependentSymbols;
    let StrategyEventData::Compute { rows: parts, .. } =
        ComputeExecutor::execute(&mut tx, &scope, &run_id, &partitioned, as_of).await?
    else {
        panic!()
    };
    let total = parts
        .iter()
        .map(|row| Decimal::from_str(row["sum"].as_str().unwrap()).unwrap())
        .sum::<Decimal>();
    let count = parts.iter().map(|row| row["count"].as_i64().unwrap()).sum::<i64>();
    assert_eq!(
        total / Decimal::from(count),
        Decimal::from_str(whole[0]["mean"].as_str().unwrap())?
    );
    tx.rollback().await?;
    Ok(())
}
