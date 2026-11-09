use anyhow::ensure;
use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use serde_json::json;
use sigbot_backtest::history::virtual_clock::ReplayCursor;
use sigbot_backtest::history::virtual_clock::ReplayDecision;
use sigbot_backtest::history::virtual_clock::ReplayEngine;
use sigbot_backtest::history::virtual_clock::ReplayProgress;
use sigbot_backtest::history::virtual_clock::ReplayRecord;
use sigbot_backtest::history::virtual_clock::ReplaySource;
use sigbot_types::messager::topic::WorkflowTopic;
use std::sync::Arc;
use std::sync::Mutex;

struct Source {
    records: Vec<ReplayRecord>,
    pages: Mutex<Vec<u32>>,
}
#[async_trait]
impl ReplaySource for Source {
    async fn page(&self, after: Option<&ReplayCursor>, limit: u32) -> Result<Vec<ReplayRecord>> {
        self.pages.lock().unwrap().push(limit);
        Ok(self
            .records
            .iter()
            .filter(|event| after.is_none_or(|after| &event.cursor > after))
            .take(limit as usize)
            .cloned()
            .collect())
    }
}
struct Decision {
    timer: Mutex<Option<DateTime<Utc>>>,
    events: Mutex<Vec<String>>,
    unsettled: Mutex<bool>,
}
#[async_trait]
impl ReplayDecision for Decision {
    async fn deliver(&self, record: &ReplayRecord, seq: u64, clock: DateTime<Utc>) -> Result<()> {
        ensure!(
            !*self.unsettled.lock().unwrap(),
            "clock advanced before effects settled"
        );
        assert_eq!(clock, record.cursor.available_at);
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:{seq}", record.cursor.source));
        *self.unsettled.lock().unwrap() = true;
        Ok(())
    }
    async fn next_timer(&self) -> Result<Option<DateTime<Utc>>> {
        Ok(*self.timer.lock().unwrap())
    }
    async fn fire_timers(&self, clock: DateTime<Utc>, limit: u32) -> Result<u32> {
        assert!(limit > 0);
        assert_eq!(self.timer.lock().unwrap().take(), Some(clock));
        self.events.lock().unwrap().push("timer".into());
        *self.unsettled.lock().unwrap() = true;
        Ok(1)
    }
    async fn settle(&self, _: DateTime<Utc>) -> Result<()> {
        *self.unsettled.lock().unwrap() = false;
        Ok(())
    }
    async fn checkpoint(&self, _: &ReplayProgress, _: bool) -> Result<()> {
        ensure!(!*self.unsettled.lock().unwrap(), "checkpoint committed before barrier");
        Ok(())
    }
}
struct Fixture;
impl Fixture {
    fn record(at: DateTime<Utc>, priority: i32, source: &str) -> ReplayRecord {
        ReplayRecord {
            cursor: ReplayCursor {
                available_at: at,
                priority,
                source: source.into(),
                source_event_id: "source-id".into(),
                revision: 0,
            },
            event_time: at,
            topic: if priority == 0 {
                WorkflowTopic::EvaluationSnapshot
            } else {
                WorkflowTopic::FeedMarket
            },
            payload: json!({}),
        }
    }
    fn progress(at: DateTime<Utc>) -> ReplayProgress {
        ReplayProgress {
            cursor: None,
            virtual_time: at,
            steps: 0,
            timer_firings: 0,
        }
    }
}

#[tokio::test]
async fn keyset_pages_and_equal_time_ai_timer_market_have_fixed_order_and_barriers() -> Result<()> {
    let at = Utc::now();
    let source = Arc::new(Source {
        records: vec![Fixture::record(at, 0, "ai"), Fixture::record(at, 2, "market")],
        pages: Mutex::new(vec![]),
    });
    let decision = Arc::new(Decision {
        timer: Mutex::new(Some(at)),
        events: Mutex::new(vec![]),
        unsettled: Mutex::new(false),
    });
    let engine = ReplayEngine::new(source.clone(), decision.clone(), at + TimeDelta::seconds(1), 1, 10)?;
    let progress = engine.run(Fixture::progress(at)).await?;
    assert_eq!(*decision.events.lock().unwrap(), vec!["ai:0", "timer", "market:1"]);
    assert_eq!(progress.steps, 2);
    assert_eq!(progress.timer_firings, 1);
    assert!(source.pages.lock().unwrap().iter().all(|size| *size == 1));
    Ok(())
}

#[tokio::test]
async fn an_out_of_order_historical_source_cannot_advance_the_clock() -> Result<()> {
    let at = Utc::now();
    let source = Arc::new(Source {
        records: vec![Fixture::record(at, 2, "z"), Fixture::record(at, 2, "a")],
        pages: Mutex::new(vec![]),
    });
    let decision = Arc::new(Decision {
        timer: Mutex::new(None),
        events: Mutex::new(vec![]),
        unsettled: Mutex::new(false),
    });
    let engine = ReplayEngine::new(source, decision.clone(), at + TimeDelta::seconds(1), 2, 10)?;
    assert!(engine.run(Fixture::progress(at)).await.is_err());
    assert!(decision.events.lock().unwrap().is_empty());
    Ok(())
}
