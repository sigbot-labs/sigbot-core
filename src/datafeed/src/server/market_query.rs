// SPDX-License-Identifier: GPL-3.0-only
//! Bounded, point-in-time reads of persisted market facts and Workflow query replies.
use super::persisted_ingestor::{DatafeedPublication, PersistedDatafeedIngestor};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_exchange::client::public_market::{PublicMarketAdapterFactory, PublicMarketSourceConfig};
use sigbot_messager::{
    client::{messager_factory::ISigbotMessagerClient, outbox_relay::OutboxRelay, reply_pairing::ReplyPairingCache},
    storage::postgres::PostgresMessageStore,
};
use sigbot_types::modules::{
    datafeed::event::DatafeedEvent,
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{MessageScope, WorkflowTopic},
    },
    strategy::runtime::{
        QueryOperation, SnapshotMode, SnapshotPoint, SnapshotQuery, SnapshotResult, StrategyEvent, StrategyEventData,
    },
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};
use tokio::sync::Mutex;

pub struct MarketReader;

impl MarketReader {
    pub async fn snapshot(
        tx: &mut Transaction<'_, Postgres>,
        scope: &MessageScope,
        query: &SnapshotQuery,
        now: DateTime<Utc>,
    ) -> Result<SnapshotResult> {
        Self::validate_query(query, now)?;
        let as_of = match query.mode {
            SnapshotMode::Latest => now,
            SnapshotMode::AsOf(value) => value,
        };
        let rows = sqlx::query("SELECT DISTINCT ON(source,symbol) source,symbol,event_at,available_at,payload FROM market_event
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND event_type='market' AND source=ANY($4) AND symbol=ANY($5)
            AND event_at<=$6 AND available_at<=$6 ORDER BY source,symbol,event_at DESC,available_at DESC,revision DESC,source_event_id DESC LIMIT $7")
            .bind(&scope.env).bind(scope.tenant_id).bind(scope.space_id).bind(&query.sources).bind(&query.symbols)
            .bind(as_of).bind(i64::from(query.max_rows)).fetch_all(&mut **tx).await?;
        let result = SnapshotResult {
            points: rows.into_iter().map(Self::point).collect::<Result<_>>()?,
        };
        Self::validate_result(query, &result, now)?;
        Ok(result)
    }

    fn validate_query(query: &SnapshotQuery, now: DateTime<Utc>) -> Result<()> {
        query.validate(now)?;
        let sources: HashSet<_> = query.sources.iter().collect();
        let symbols: HashSet<_> = query.symbols.iter().collect();
        ensure!(
            sources.len() == query.sources.len() && symbols.len() == query.symbols.len(),
            "duplicate source or symbol"
        );
        let expected = sources.len() * symbols.len();
        ensure!(
            expected <= query.max_rows as usize,
            "snapshot cardinality exceeds result bound"
        );
        Ok(())
    }

    fn validate_result(query: &SnapshotQuery, result: &SnapshotResult, now: DateTime<Utc>) -> Result<()> {
        Self::validate_query(query, now)?;
        let points: HashSet<_> = result
            .points
            .iter()
            .map(|point| (&point.source, &point.symbol))
            .collect();
        ensure!(
            points.len() == result.points.len() && result.points.len() == query.sources.len() * query.symbols.len(),
            "one or more requested market sources have no available fact"
        );
        query.validate_result(result, now)
    }

    fn observed(facts: &[DatafeedEvent]) -> Result<SnapshotResult> {
        Ok(SnapshotResult {
            points: facts
                .iter()
                .map(|fact| {
                    let price = fact
                        .raw
                        .get("price")
                        .and_then(Value::as_str)
                        .context("active market fact has no normalized price")?
                        .parse::<Decimal>()?;
                    Ok(SnapshotPoint {
                        source: fact.source.clone(),
                        symbol: fact.symbol.clone(),
                        event_time: fact.event_time,
                        available_time: fact.available_time,
                        price,
                    })
                })
                .collect::<Result<_>>()?,
        })
    }

    /// Historical reads are bounded by both event time and the time facts became available.
    pub async fn history(
        tx: &mut Transaction<'_, Postgres>,
        scope: &MessageScope,
        query: &SnapshotQuery,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<SnapshotPoint>> {
        query.validate(as_of)?;
        ensure!(
            start <= end && end <= as_of && as_of <= Utc::now(),
            "invalid historical availability boundary"
        );
        let rows = sqlx::query(
            "SELECT source,symbol,event_at,available_at,payload FROM market_event
            WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND event_type='market' AND source=ANY($4) AND symbol=ANY($5)
            AND event_at>=$6 AND event_at<=$7 AND available_at<=$8
            ORDER BY event_at,source,symbol,seq,revision LIMIT $9",
        )
        .bind(&scope.env)
        .bind(scope.tenant_id)
        .bind(scope.space_id)
        .bind(&query.sources)
        .bind(&query.symbols)
        .bind(start)
        .bind(end)
        .bind(as_of)
        .bind(i64::from(query.max_rows))
        .fetch_all(&mut **tx)
        .await?;
        rows.into_iter().map(Self::point).collect()
    }

    fn point(row: sqlx::postgres::PgRow) -> Result<SnapshotPoint> {
        let payload: Value = row.try_get("payload")?;
        let price = payload.get("price").context("market fact has no normalized price")?;
        let price = Decimal::from_str(&price.as_str().map(str::to_owned).unwrap_or_else(|| price.to_string()))?;
        ensure!(price > Decimal::ZERO, "market fact price must be positive");
        Ok(SnapshotPoint {
            source: row.try_get("source")?,
            symbol: row.try_get("symbol")?,
            event_time: row.try_get("event_at")?,
            available_time: row.try_get("available_at")?,
            price,
        })
    }
}

pub struct MarketQueryRuntime {
    pool: PgPool,
    scope: MessageScope,
    workflow_id: i64,
    node_id: i64,
    run_id: String,
    inputs: Vec<i64>,
    compute_inputs: Vec<i64>,
    owner: String,
    virtual_clock: bool,
    active_sources: Option<PublicMarketAdapterFactory>,
    relay: Mutex<Option<OutboxRelay>>,
}

impl MarketQueryRuntime {
    pub async fn from_properties(properties: &HashMap<String, String>) -> Result<Arc<Self>> {
        let get = |key: &str| {
            properties
                .get(key)
                .cloned()
                .with_context(|| format!("missing market query property {key}"))
        };
        let scope = MessageScope {
            env: get("env")?,
            tenant_id: get("tenant_id")?.parse()?,
            space_id: get("space_id")?.parse()?,
        };
        scope.validate()?;
        let workflow_id: i64 = get("workflow_id")?.parse()?;
        let node_id: i64 = get("node_id")?.parse()?;
        let inputs: Vec<i64> = get("snapshot_input_node_ids")?
            .split(',')
            .map(str::parse)
            .collect::<std::result::Result<_, _>>()?;
        ensure!(
            workflow_id > 0 && node_id > 0 && !inputs.is_empty() && inputs.iter().all(|id| *id > 0),
            "explicit query Workflow edges required"
        );
        let owner = format!(
            "datafeed.query:{}:{}:{}:{workflow_id}:{node_id}",
            scope.env, scope.tenant_id, scope.space_id
        );
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&std::env::var(get("database_url_env")?)?)
            .await?;
        let run_id = get("run_id")?;
        let compute_inputs = properties
            .get("compute_input_node_ids")
            .map(|value| {
                value
                    .split(',')
                    .map(str::parse)
                    .collect::<std::result::Result<Vec<i64>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        ensure!(compute_inputs.iter().all(|id| *id > 0), "invalid compute incoming edge");
        ensure!(!run_id.is_empty(), "market query run required");
        let virtual_clock = match properties.get("clock_mode").map(String::as_str).unwrap_or("realtime") {
            "realtime" => false,
            "virtual" => true,
            _ => anyhow::bail!("unknown market query clock mode"),
        };
        ensure!(
            !virtual_clock || matches!(scope.env.as_str(), "replay" | "backtest") || scope.env.starts_with("replay-"),
            "virtual query requires an isolated replay environment"
        );
        let active_config: Vec<PublicMarketSourceConfig> = properties
            .get("active_sources")
            .map(|value| serde_json::from_str(value).context("invalid active_sources configuration"))
            .transpose()?
            .unwrap_or_default();
        for config in &active_config {
            config.validate()?;
        }
        // Replay never initializes network adapters, including a direct LATEST request.
        let active_sources = if virtual_clock || active_config.is_empty() {
            None
        } else {
            Some(PublicMarketAdapterFactory::create(active_config).await?)
        };
        Ok(Arc::new(Self {
            pool,
            scope,
            workflow_id,
            node_id,
            run_id,
            inputs,
            compute_inputs,
            owner,
            virtual_clock,
            active_sources,
            relay: Mutex::new(None),
        }))
    }

    pub async fn start(self: &Arc<Self>, messager: Arc<dyn ISigbotMessagerClient>) -> Result<()> {
        let mut kinds = vec![WorkflowTopic::FeedSnapshotRequest];
        if !self.compute_inputs.is_empty() {
            kinds.push(WorkflowTopic::ComputeRequest);
        }
        for kind in kinds {
            let topic = kind.topic(&self.scope, self.workflow_id)?;
            let instance = self.clone();
            messager
                .subscribe(
                    &topic,
                    Arc::new(move |bytes| {
                        let instance = instance.clone();
                        Box::pin(async move {
                            instance.consume(kind, &bytes).await?;
                            Ok("committed".to_owned())
                        })
                    }),
                )
                .await?;
        }
        let mut relay = self.relay.lock().await;
        ensure!(relay.is_none(), "market query runtime already started");
        *relay = Some(OutboxRelay::start(
            PostgresMessageStore::new(self.pool.clone()),
            self.owner.clone(),
            messager,
        ));
        Ok(())
    }

    pub async fn consume(&self, kind: WorkflowTopic, bytes: &[u8]) -> Result<()> {
        let message: MessageEnvelope<Value> = serde_json::from_slice(bytes)?;
        message.validate()?;
        let input = message.durable.clone();
        ensure!(
            matches!(kind, WorkflowTopic::FeedSnapshotRequest | WorkflowTopic::ComputeRequest),
            "unsupported query topic"
        );
        let topic = kind.topic(&self.scope, self.workflow_id)?;
        input.validate_topic(&topic)?;
        ensure!(
            input.kind == MessageKind::Request,
            "market snapshot requires a request envelope"
        );
        if input.run_id != self.run_id {
            return Ok(());
        }
        let incoming = if kind == WorkflowTopic::ComputeRequest {
            &self.compute_inputs
        } else {
            &self.inputs
        };
        if !incoming.contains(&input.node_id) || input.target_node_id.is_some_and(|id| id != self.node_id) {
            return Ok(());
        }
        let active_query = if kind == WorkflowTopic::FeedSnapshotRequest && self.active_sources.is_some() {
            serde_json::from_value::<SnapshotQuery>(input.payload.clone())
                .ok()
                .filter(|query| matches!(query.mode, SnapshotMode::Latest))
        } else {
            None
        };
        let fetched = if let Some(query) = active_query {
            ensure!(
                input.available_time <= Utc::now(),
                "query request is not available at this clock"
            );
            if !self.admit_active(&topic, &message).await? {
                return Ok(());
            }
            Some(self.fetch_active(query).await)
        } else {
            None
        };
        self.complete_request(kind, message, fetched).await
    }

    /// No database transaction is retained across public SDK I/O. The MQTT request
    /// stays unacknowledged until complete_request commits its final transaction.
    async fn admit_active(&self, topic: &str, message: &MessageEnvelope<Value>) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let accepted = PostgresMessageStore::accept(&mut tx, &self.owner, topic, message).await?;
        tx.commit().await?;
        Ok(accepted)
    }

    async fn complete_request(
        &self,
        kind: WorkflowTopic,
        message: MessageEnvelope<Value>,
        fetched: Option<Result<Vec<DatafeedEvent>>>,
    ) -> Result<()> {
        let input = message.durable.clone();
        let topic = kind.topic(&self.scope, self.workflow_id)?;
        let span = MessageTrace::child("datafeed.query.consume", &input.traceparent);
        let mut tx = self.pool.begin().await?;
        // Aggregates, watermark and revision describe one committed database view.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        if !PostgresMessageStore::accept(&mut tx, &self.owner, &topic, &message).await? {
            tx.commit().await?;
            return Ok(());
        }
        let query = if kind == WorkflowTopic::FeedSnapshotRequest {
            Some(serde_json::from_value::<SnapshotQuery>(input.payload.clone()))
        } else {
            None
        };
        let active_query = query.as_ref().and_then(|parsed| parsed.as_ref().ok()).cloned();
        let now = if self.virtual_clock {
            sqlx::query_scalar("SELECT virtual_time FROM backtest_checkpoint WHERE env=$1 AND tenant_id=$2 AND space_id=$3 AND run_id=$4 AND state='running'")
                .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.scope.space_id).bind(&self.run_id).fetch_one(&mut *tx).await?
        } else {
            Utc::now()
        };
        ensure!(
            input.available_time <= now,
            "query request is not available at this clock"
        );
        let execution = if kind == WorkflowTopic::ComputeRequest {
            match serde_json::from_value(input.payload.clone()) {
                Ok(plan) => super::compute_query::ComputeExecutor::execute(&mut tx, &self.scope, &plan, now).await,
                Err(error) => Err(error.into()),
            }
        } else if let Some(fetched) = fetched {
            match fetched {
                Ok(mut facts) => {
                    // Fixed lock/result order regardless of request pair ordering.
                    facts.sort_by(|left, right| {
                        (&left.source, &left.symbol, &left.source_event_id, left.revision).cmp(&(
                            &right.source,
                            &right.symbol,
                            &right.source_event_id,
                            right.revision,
                        ))
                    });
                    let query = active_query.context("active snapshot query missing")?;
                    let observed = MarketReader::observed(&facts)?;
                    match MarketReader::validate_result(&query, &observed, now) {
                        Ok(()) => {
                            let parent = span.traceparent();
                            let publication = DatafeedPublication {
                                scope: &self.scope,
                                workflow_id: self.workflow_id,
                                node_id: self.node_id,
                                run_id: &self.run_id,
                                producer: &self.owner,
                                traceparent: &parent,
                            };
                            let mut canonical = Vec::with_capacity(facts.len());
                            for fact in facts {
                                // Storage conflicts abort all facts and leave Inbox pending; never
                                // convert a failed SQL transaction into a successful query response.
                                canonical
                                    .push(PersistedDatafeedIngestor::persist_in(&mut tx, &publication, fact).await?);
                            }
                            let result = MarketReader::observed(&canonical)?;
                            MarketReader::validate_result(&query, &result, now)?;
                            Ok(StrategyEventData::Snapshot(result))
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            }
        } else {
            match query.context("snapshot query missing")? {
                Ok(query) => MarketReader::snapshot(&mut tx, &self.scope, &query, now)
                    .await
                    .map(StrategyEventData::Snapshot),
                Err(error) => Err(error.into()),
            }
        };
        let data = match execution {
            Ok(result) => result,
            Err(error) => StrategyEventData::QueryFailure {
                operation: if kind == WorkflowTopic::ComputeRequest {
                    QueryOperation::Compute
                } else {
                    QueryOperation::Snapshot
                },
                reason: error.to_string().chars().take(512).collect(),
                retryable: true,
            },
        };
        let watermark = match &data {
            StrategyEventData::Compute { watermark, .. } => *watermark,
            _ => input.available_time,
        };
        let completed_at = if self.virtual_clock { now } else { Utc::now() };
        let result = DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: Some(self.workflow_id),
            account_id: input.account_id,
            node_id: self.node_id,
            target_node_id: Some(input.node_id),
            run_id: input.run_id.clone(),
            seq: PostgresMessageStore::next_sequence(&mut tx, &self.scope, self.node_id, &input.run_id).await?,
            attempt_no: 0,
            kind: MessageKind::Result,
            event_time: now,
            available_time: completed_at,
            produced_at: completed_at,
            traceparent: MessageTrace::child("datafeed.query.publish", &span.traceparent()).traceparent(),
            payload: serde_json::to_value(StrategyEvent {
                source: "datafeed.query".to_owned(),
                symbol: None,
                event_time: now,
                available_time: completed_at,
                data,
            })?,
        };
        PostgresMessageStore::enqueue(
            &mut tx,
            &self.owner,
            &if kind == WorkflowTopic::ComputeRequest {
                WorkflowTopic::ComputeResult
            } else {
                WorkflowTopic::FeedSnapshotResult
            }
            .topic(&self.scope, self.workflow_id)?,
            &result,
        )
        .await?;
        if kind == WorkflowTopic::ComputeRequest {
            sqlx::query("INSERT INTO workflow_checkpoint(env,tenant_id,space_id,workflow_id,node_id,run_id,partition_key,seq,state,watermark,version)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,1) ON CONFLICT(env,tenant_id,space_id,workflow_id,node_id,run_id,partition_key)
                DO UPDATE SET seq=EXCLUDED.seq,state=EXCLUDED.state,watermark=GREATEST(workflow_checkpoint.watermark,EXCLUDED.watermark),
                    version=workflow_checkpoint.version+1,updated_at=NOW()")
                .bind(&self.scope.env).bind(self.scope.tenant_id).bind(self.scope.space_id).bind(self.workflow_id).bind(self.node_id)
                .bind(&input.run_id).bind(format!("compute:{}", input.node_id)).bind(input.seq)
                .bind(serde_json::json!({"plan":input.payload,"result":result.payload})).bind(watermark).execute(&mut *tx).await?;
        }
        PostgresMessageStore::complete(&mut tx, &self.owner, &input).await?;
        if let Some(pairing) = &message.request_span_id {
            ReplyPairingCache::shared().remember(&result, pairing)?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn fetch_active(&self, query: SnapshotQuery) -> Result<Vec<DatafeedEvent>> {
        MarketReader::validate_query(&query, Utc::now())?;
        let pairs: Vec<_> = query
            .sources
            .iter()
            .flat_map(|source| query.symbols.iter().map(move |symbol| (source.clone(), symbol.clone())))
            .collect();
        self.active_sources
            .as_ref()
            .context("active market sources are disabled")?
            .snapshot(&pairs)
            .await
    }

    pub async fn close(&self) {
        if let Some(relay) = self.relay.lock().await.take() {
            relay.close();
        }
    }
}
