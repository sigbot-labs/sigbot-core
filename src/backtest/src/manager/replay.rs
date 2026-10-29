// SPDX-License-Identifier: GPL-3.0-only
//! Finite replay advances one decision owner only after durable effects settle.
use anyhow::{ensure, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sigbot_types::modules::messager::topic::WorkflowTopic;
use std::{collections::VecDeque, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReplayCursor {
    pub available_at: DateTime<Utc>,
    /// Recorded AI output precedes timers (1), market (2), then news (3) at equal times.
    pub priority: i32,
    pub source: String,
    pub source_event_id: String,
    pub revision: i64,
}

#[derive(Clone, Debug)]
pub struct ReplayRecord {
    pub cursor: ReplayCursor,
    pub event_time: DateTime<Utc>,
    pub topic: WorkflowTopic,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayProgress {
    pub cursor: Option<ReplayCursor>,
    pub virtual_time: DateTime<Utc>,
    pub steps: u64,
    #[serde(default)]
    pub timer_firings: u64,
}

#[async_trait]
pub trait ReplaySource: Send + Sync {
    /// Strict keyset ordering and a hard page limit; never load the whole history.
    async fn page(&self, after: Option<&ReplayCursor>, limit: u32) -> Result<Vec<ReplayRecord>>;
}

#[async_trait]
pub trait ReplayDecision: Send + Sync {
    async fn deliver(&self, record: &ReplayRecord, sequence: u64, clock: DateTime<Utc>) -> Result<()>;
    async fn next_timer(&self) -> Result<Option<DateTime<Utc>>>;
    async fn fire_timers(&self, clock: DateTime<Utc>, limit: u32) -> Result<u32>;
    /// Must verify every configured recipient's Inbox, including recursively
    /// produced messages and financial facts. PUBACK alone is not this barrier.
    async fn settle(&self, clock: DateTime<Utc>) -> Result<()>;
    async fn checkpoint(&self, progress: &ReplayProgress, complete: bool) -> Result<()>;
}

pub struct ReplayEngine {
    source: Arc<dyn ReplaySource>,
    decision: Arc<dyn ReplayDecision>,
    until: DateTime<Utc>,
    page_size: u32,
    max_steps: u64,
}

impl ReplayEngine {
    pub fn new(
        source: Arc<dyn ReplaySource>,
        decision: Arc<dyn ReplayDecision>,
        until: DateTime<Utc>,
        page_size: u32,
        max_steps: u64,
    ) -> Result<Self> {
        ensure!(
            (1..=1024).contains(&page_size) && (1..=10_000_000).contains(&max_steps),
            "replay bounds invalid"
        );
        Ok(Self {
            source,
            decision,
            until,
            page_size,
            max_steps,
        })
    }

    pub async fn run(&self, mut progress: ReplayProgress) -> Result<ReplayProgress> {
        ensure!(
            progress.virtual_time <= self.until
                && progress.steps.saturating_add(progress.timer_firings) <= self.max_steps,
            "replay checkpoint exceeds its configured bounds"
        );
        // A crash after decision commit but before this checkpoint must finish its
        // previous barrier before admitting another historical event.
        self.decision.settle(progress.virtual_time).await?;
        let mut buffered = VecDeque::new();
        let mut exhausted = false;
        loop {
            if buffered.is_empty() && !exhausted {
                let page = self.source.page(progress.cursor.as_ref(), self.page_size).await?;
                ensure!(
                    page.len() <= self.page_size as usize,
                    "historical source exceeded page bound"
                );
                let mut last = progress.cursor.as_ref();
                for record in &page {
                    ensure!(
                        last.is_none_or(|last| last < &record.cursor),
                        "historical source is not strictly ordered"
                    );
                    ensure!(
                        record.event_time <= record.cursor.available_at && record.cursor.available_at <= self.until,
                        "historical source includes future or invalid data"
                    );
                    last = Some(&record.cursor);
                }
                exhausted = page.is_empty();
                buffered.extend(page);
            }
            let timer = self.decision.next_timer().await?.filter(|at| *at <= self.until);
            let timer_first = timer.is_some_and(|at| {
                buffered.front().is_none_or(|event| {
                    at < event.cursor.available_at || (at == event.cursor.available_at && event.cursor.priority > 1)
                })
            });
            if timer_first {
                let at = timer.expect("timer_first implies a timer");
                ensure!(at >= progress.virtual_time, "timer would move replay clock backwards");
                let remaining = self
                    .max_steps
                    .saturating_sub(progress.steps.saturating_add(progress.timer_firings));
                ensure!(remaining > 0, "replay exceeded its event limit");
                let count = self.decision.fire_timers(at, remaining.min(64) as u32).await?;
                ensure!((1..=64).contains(&count), "timer source did not make bounded progress");
                progress.timer_firings = progress
                    .timer_firings
                    .checked_add(u64::from(count))
                    .ok_or_else(|| anyhow::anyhow!("replay count overflow"))?;
                progress.virtual_time = at;
            } else if let Some(record) = buffered.pop_front() {
                ensure!(
                    record.cursor.available_at >= progress.virtual_time,
                    "historical clock moved backwards"
                );
                ensure!(
                    progress.steps.saturating_add(progress.timer_firings) < self.max_steps,
                    "replay exceeded its event limit"
                );
                self.decision
                    .deliver(&record, progress.steps, record.cursor.available_at)
                    .await?;
                progress.steps += 1;
                progress.virtual_time = record.cursor.available_at;
                progress.cursor = Some(record.cursor);
            } else {
                break;
            }
            ensure!(
                progress.steps.saturating_add(progress.timer_firings) <= self.max_steps,
                "replay exceeded its event limit"
            );
            self.decision.settle(progress.virtual_time).await?;
            self.decision.checkpoint(&progress, false).await?;
        }
        self.decision.checkpoint(&progress, true).await?;
        Ok(progress)
    }
}
