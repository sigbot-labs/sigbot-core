use anyhow::ensure;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Duration;
use chrono::Utc;
use serde_json::json;
use serde_json::Value;
use sigbot_strategy_sdk::sdk::runtime::SerialStrategy;
use sigbot_strategy_sdk::sdk::runtime::Strategy;
use sigbot_strategy_sdk::sdk::runtime::StrategyCheckpoint;
use sigbot_strategy_sdk::sdk::runtime::StrategyContext;
use sigbot_strategy_sdk::sdk::runtime::StrategyIdentity;
use sigbot_strategy_sdk::sdk::runtime::StrategyTransaction;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::strategy::execution::Effects;
use sigbot_types::strategy::execution::HyperparametersUpdate;
use sigbot_types::strategy::execution::SnapshotMode;
use sigbot_types::strategy::execution::SnapshotQuery;
use sigbot_types::strategy::execution::StrategyEvent;
use sigbot_types::strategy::execution::StrategyEventData;
use sigbot_types::strategy::execution::StrategyRequest;
use sigbot_types::Uuid;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Default)]
struct StoreState {
    checkpoint: StrategyCheckpoint,
    processed: HashSet<(i64, i64)>,
    effects: usize,
    requests: Vec<StrategyRequest>,
}
#[derive(Default)]
struct MemoryTransaction(Mutex<StoreState>);
#[async_trait]
impl StrategyTransaction for MemoryTransaction {
    async fn load(&self, _: &StrategyIdentity) -> Result<StrategyCheckpoint> {
        Ok(self.0.lock().await.checkpoint.clone())
    }
    async fn processed(&self, _: &StrategyIdentity, input: &DurableMessage<Value>) -> Result<bool> {
        Ok(self.0.lock().await.processed.contains(&(input.node_id, input.seq)))
    }
    async fn commit(
        &self,
        _: &StrategyIdentity,
        input: &DurableMessage<Value>,
        expected: i64,
        checkpoint: StrategyCheckpoint,
        effects: Effects,
    ) -> Result<bool> {
        let mut state = self.0.lock().await;
        if state.processed.contains(&(input.node_id, input.seq)) {
            return Ok(false);
        }
        ensure!(state.checkpoint.version == expected, "CAS conflict");
        state.checkpoint = checkpoint;
        state.effects += effects.intents.len() + effects.requests.len() + effects.timers.len();
        state.requests.extend(effects.requests);
        state.processed.insert((input.node_id, input.seq));
        Ok(true)
    }
}
struct SnapshotStrategy;
impl Strategy for SnapshotStrategy {
    fn on_data(&mut self, ctx: &mut StrategyContext, _: &StrategyEvent) -> Result<Effects> {
        ctx.market().snapshot(SnapshotQuery {
            sources: vec!["binance".into()],
            symbols: vec!["BTCUSDT".into()],
            mode: SnapshotMode::Latest,
            max_age_ms: 1000,
            max_skew_ms: 1000,
            max_rows: 1,
        })?;
        Ok(Effects::default())
    }
    fn on_hyperparameters(&mut self, _: &mut StrategyContext, _: &HyperparametersUpdate) -> Result<Effects> {
        Ok(Effects::default())
    }
}

#[tokio::test]
async fn virtual_clock_freezes_latest_snapshot_and_rejects_future_delivery() -> Result<()> {
    let store = Arc::new(MemoryTransaction::default());
    let input = Fixture::input(1);
    let identity = StrategyIdentity {
        scope: input.scope.clone(),
        workflow_id: Fixture::workflow(),
        node_id: 5,
        run_id: input.run_id.clone(),
    };
    let runtime = SerialStrategy::new(identity, SnapshotStrategy, store.clone(), BTreeMap::new())?.with_replay_clock();
    let now = input.available_time;
    runtime.on_data(input.clone(), now, Arc::new(Value::Null)).await?;
    let state = store.0.lock().await;
    assert!(
        matches!(&state.requests[0],StrategyRequest::Snapshot(SnapshotQuery {mode:SnapshotMode::AsOf(at),..}) if *at==now)
    );
    drop(state);
    let mut future = input;
    future.seq += 1;
    future.available_time = now + Duration::seconds(1);
    future.produced_at = future.available_time;
    assert!(runtime.on_data(future, now, Arc::new(Value::Null)).await.is_err());
    Ok(())
}
struct CountingStrategy(Arc<AtomicBool>);
impl Strategy for CountingStrategy {
    fn on_data(&mut self, ctx: &mut StrategyContext, _: &StrategyEvent) -> Result<Effects> {
        let count = ctx.state.user.get("count").and_then(Value::as_u64).unwrap_or(0);
        ctx.state.user.insert("count".into(), json!(count + 1));
        ensure!(!self.0.load(Ordering::SeqCst), "callback failed after draft mutation");
        Ok(Effects::default())
    }
    fn on_hyperparameters(&mut self, ctx: &mut StrategyContext, _: &HyperparametersUpdate) -> Result<Effects> {
        let count = ctx
            .state
            .user
            .get("hyperparameter_calls")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        ctx.state.user.insert("hyperparameter_calls".into(), json!(count + 1));
        Ok(Effects::default())
    }
}
struct Fixture;
impl Fixture {
    fn tenant() -> Uuid {
        Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000701").unwrap()
    }
    fn workflow() -> Uuid {
        Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000702").unwrap()
    }
    fn account() -> Uuid {
        Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000703").unwrap()
    }
    fn run() -> Uuid {
        Uuid::parse_str("0192f7b2-5c00-7000-8000-000000000704").unwrap()
    }
    fn input(seq: i64) -> DurableMessage<StrategyEvent> {
        let now = Utc::now() - Duration::seconds(1);
        DurableMessage {
            version: 2,
            scope: MessageScope {
                env: "paper".into(),
                tenant_id: Self::tenant(),
            },
            workflow_id: Some(Self::workflow()),
            account_id: Some(Self::account()),
            node_id: 4,
            target_node_id: Some(5),
            run_id: Self::run(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: now,
            available_time: now,
            produced_at: now,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: StrategyEvent {
                source: "x".into(),
                symbol: None,
                event_time: now,
                available_time: now,
                data: StrategyEventData::News {
                    source_event_id: "1".into(),
                    text: "observed fact".into(),
                    url: None,
                },
            },
        }
    }
    fn runtime(store: Arc<MemoryTransaction>, fail: Arc<AtomicBool>) -> SerialStrategy<CountingStrategy> {
        SerialStrategy::new(
            StrategyIdentity {
                scope: Self::input(0).scope,
                workflow_id: Self::workflow(),
                node_id: 5,
                run_id: Self::run(),
            },
            CountingStrategy(fail),
            store,
            BTreeMap::new(),
        )
        .unwrap()
    }
}

#[tokio::test]
async fn failed_callback_discards_draft_and_retry_commits_exactly_once() {
    let store = Arc::new(MemoryTransaction::default());
    let fail = Arc::new(AtomicBool::new(true));
    let runtime = Fixture::runtime(store.clone(), fail.clone());
    assert!(runtime
        .on_data(Fixture::input(0), Utc::now(), Arc::new(json!({})))
        .await
        .is_err());
    assert!(store.0.lock().await.checkpoint.state.user.is_empty());
    fail.store(false, Ordering::SeqCst);
    assert!(runtime
        .on_data(Fixture::input(0), Utc::now(), Arc::new(json!({})))
        .await
        .unwrap());
    assert!(!runtime
        .on_data(Fixture::input(0), Utc::now(), Arc::new(json!({})))
        .await
        .unwrap());
    assert_eq!(store.0.lock().await.checkpoint.state.user["count"], 1);
}

#[tokio::test]
async fn concurrent_callbacks_are_serialized_and_restore_the_last_committed_state() {
    let store = Arc::new(MemoryTransaction::default());
    let runtime = Fixture::runtime(store.clone(), Arc::new(AtomicBool::new(false)));
    let (first, second) = tokio::join!(
        runtime.on_data(Fixture::input(0), Utc::now(), Arc::new(json!({}))),
        runtime.on_data(Fixture::input(1), Utc::now(), Arc::new(json!({})))
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(store.0.lock().await.checkpoint.state.user["count"], 2);
    let restarted = Fixture::runtime(store.clone(), Arc::new(AtomicBool::new(false)));
    restarted
        .on_data(Fixture::input(2), Utc::now(), Arc::new(json!({})))
        .await
        .unwrap();
    assert_eq!(store.0.lock().await.checkpoint.state.user["count"], 3);
}

#[tokio::test]
async fn expired_update_commits_without_decision_and_does_not_block_new_data() -> Result<()> {
    let store = Arc::new(MemoryTransaction::default());
    let runtime = Fixture::runtime(store.clone(), Arc::new(AtomicBool::new(false)));
    let now = Utc::now();
    let input = Fixture::input(10);
    let update: HyperparametersUpdate = serde_json::from_value(json!({
        "version": 1, "as_of": now - Duration::minutes(2),
        "valid_until": now - Duration::minutes(1),
        "probabilities": {"trend":0.5,"range":0.2,"stabilizing":0.15,"suppression":0.1,"unknown":0.05},
        "parameters": {}, "model_version":"test-v1", "classification_version":"regime-v1", "case_refs":[]
    }))?;
    let expired = input.map_payload(|_| update.clone());
    assert!(
        runtime
            .on_hyperparameters(expired.clone(), now, Arc::new(Value::Null))
            .await?
    );
    assert!(!runtime.on_hyperparameters(expired, now, Arc::new(Value::Null)).await?);
    {
        let checkpoint = store.0.lock().await;
        assert!(checkpoint.checkpoint.hyperparameters.is_none());
        assert!(checkpoint.checkpoint.state.regime.is_none());
        assert_eq!(checkpoint.effects, 0);
        assert!(!checkpoint.checkpoint.state.user.contains_key("hyperparameter_calls"));
    }
    assert!(runtime.on_data(Fixture::input(11), now, Arc::new(Value::Null)).await?);
    assert_eq!(store.0.lock().await.checkpoint.state.user["count"], 1);
    let mut valid = update;
    valid.version = 2;
    valid.as_of = now - Duration::seconds(1);
    valid.valid_until = now + Duration::minutes(1);
    assert!(
        runtime
            .on_hyperparameters(Fixture::input(12).map_payload(|_| valid), now, Arc::new(Value::Null))
            .await?
    );
    assert_eq!(
        store
            .0
            .lock()
            .await
            .checkpoint
            .hyperparameters
            .as_ref()
            .unwrap()
            .version,
        2
    );
    assert_eq!(store.0.lock().await.checkpoint.state.user["hyperparameter_calls"], 1);
    Ok(())
}
