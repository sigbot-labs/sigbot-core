// SPDX-License-Identifier: GPL-3.0-only

use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use common_context::IProvider;
use common_telemetry::tracing_context::MessageTrace;
use rust_decimal::Decimal;
use serde_json::Value;
use sigbot_core::config::config::get_config;
use sigbot_core::modules::datafeed::handler::query::{MarketQueryHandler, MarketQuerySession};
use sigbot_exchange::market::market::MarketExchanges;
use sigbot_exchange::market::market::PublicMarketSourceConfig;
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::OutboxRelay;
use sigbot_messager::messager::ReplyPairingCache;
use sigbot_types::datafeed::event::DatafeedEvent;
use sigbot_types::datafeed::event::DatafeedPublication;
use sigbot_types::exchange::mode::ExecutionMode;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::messager::topic::WorkflowTopic;
use sigbot_types::strategy::execution::Aggregation;
use sigbot_types::strategy::execution::ComputePlan;
use sigbot_types::strategy::execution::ModelPartitioning;
use sigbot_types::strategy::execution::QueryOperation;
use sigbot_types::strategy::execution::SnapshotMode;
use sigbot_types::strategy::execution::SnapshotPoint;
use sigbot_types::strategy::execution::SnapshotQuery;
use sigbot_types::strategy::execution::SnapshotResult;
use sigbot_types::strategy::execution::StrategyEvent;
use sigbot_types::strategy::execution::StrategyEventData;
use sigbot_types::Uuid;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MarketReader;

impl MarketReader {
    pub async fn snapshot(
        transaction: &mut MarketQuerySession,
        scope: &MessageScope,
        run_id: &Uuid,
        query: &SnapshotQuery,
        now: DateTime<Utc>,
    ) -> Result<SnapshotResult> {
        Self::validate_query(query, now)?;
        let result = transaction.snapshot(scope, run_id, query, now).await?;
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
        transaction: &mut MarketQuerySession,
        scope: &MessageScope,
        run_id: &Uuid,
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
        transaction.history(scope, run_id, query, start, end, as_of).await
    }
}

pub struct MarketQueryService {
    handler: MarketQueryHandler,
    scope: MessageScope,
    workflow_id: Uuid,
    node_id: i64,
    run_id: Uuid,
    inputs: Vec<i64>,
    compute_inputs: Vec<i64>,
    owner: String,
    virtual_clock: bool,
    active_sources: Option<MarketExchanges>,
    relay: Mutex<Option<OutboxRelay>>,
}

impl MarketQueryService {
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
        };
        scope.validate()?;
        let workflow_id: Uuid = get("workflow_id")?.parse()?;
        let node_id: i64 = get("node_id")?.parse()?;
        let inputs: Vec<i64> = get("snapshot_input_node_ids")?
            .split(',')
            .map(str::parse)
            .collect::<std::result::Result<_, _>>()?;
        ensure!(
            workflow_id.get_version_num() == 7 && node_id > 0 && !inputs.is_empty() && inputs.iter().all(|id| *id > 0),
            "explicit query Workflow edges required"
        );
        let owner = format!(
            "datafeed.query:{}:{}:{workflow_id}:{node_id}",
            scope.env, scope.tenant_id
        );
        let database_url = get_config().storage.postgres.connection_url()?;
        let handler = MarketQueryHandler::connect(&database_url).await?;
        let run_id: Uuid = get("run_id")?.parse()?;
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
        ensure!(run_id.get_version_num() == 7, "market query run must be UUIDv7");
        let virtual_clock = match properties.get("clock_mode").map(String::as_str).unwrap_or("realtime") {
            "realtime" => false,
            "virtual" => true,
            _ => anyhow::bail!("unknown market query clock mode"),
        };
        let mode = properties
            .get("execution_mode")
            .map(|value| serde_json::from_value::<ExecutionMode>(serde_json::json!(value)))
            .transpose()?;
        ensure!(
            !virtual_clock || mode.is_none_or(|mode| mode == ExecutionMode::Backtest),
            "virtual query requires backtest mode"
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
            Some(MarketExchanges::create(active_config).await?)
        };
        Ok(Arc::new(Self {
            handler,
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

    pub async fn start(self: &Arc<Self>, messager: Arc<dyn IMessager>) -> Result<()> {
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
            self.handler.messager(),
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
        self.handler.admit(&self.owner, topic, message).await
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
        let Some(mut transaction) = self.handler.begin(&self.owner, &topic, &message).await? else {
            return Ok(());
        };
        let query = if kind == WorkflowTopic::FeedSnapshotRequest {
            Some(serde_json::from_value::<SnapshotQuery>(input.payload.clone()))
        } else {
            None
        };
        let active_query = query.as_ref().and_then(|parsed| parsed.as_ref().ok()).cloned();
        let now = if self.virtual_clock {
            transaction.virtual_time(&self.scope, &self.run_id).await?
        } else {
            Utc::now()
        };
        ensure!(
            input.available_time <= now,
            "query request is not available at this clock"
        );
        let execution = if kind == WorkflowTopic::ComputeRequest {
            match serde_json::from_value(input.payload.clone()) {
                Ok(plan) => {
                    crate::query::market_query::ComputeExecutor::execute(
                        &mut transaction,
                        &self.scope,
                        &self.run_id,
                        &plan,
                        now,
                    )
                    .await
                }
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
                                canonical.push(transaction.persist(&publication, fact).await?);
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
                Ok(query) => MarketReader::snapshot(&mut transaction, &self.scope, &self.run_id, &query, now)
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
            seq: transaction
                .next_sequence(&self.scope, self.node_id, &input.run_id)
                .await?,
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
        transaction
            .enqueue(
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
            transaction
                .checkpoint_compute(&self.scope, self.workflow_id, self.node_id, &input, &result, watermark)
                .await?;
        }
        if let Some(pairing) = &message.request_span_id {
            ReplyPairingCache::shared().remember(&result, pairing)?;
        }
        transaction.complete(&self.owner, &input).await
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

pub struct ComputeExecutor;

impl ComputeExecutor {
    pub fn validate(plan: &ComputePlan, now: DateTime<Utc>) -> Result<()> {
        plan.validate(now)?;
        ensure!(plan.window_ms <= 31_536_000_000, "compute window exceeds one year");
        ensure!(
            (plan.as_of - plan.history_from).num_days() <= 366,
            "history warming range exceeds one year"
        );
        ensure!(
            !plan.aggregations.is_empty() && plan.aggregations.len() <= 7,
            "compute requires bounded aggregations"
        );
        let groups: HashSet<_> = plan.group_by.iter().map(String::as_str).collect();
        ensure!(
            groups.len() == plan.group_by.len() && groups.iter().all(|key| ["source", "symbol"].contains(key)),
            "unsupported grouping dimension"
        );
        let aggregations: HashSet<_> = plan
            .aggregations
            .iter()
            .map(|aggregation| match aggregation {
                Aggregation::Count => "count",
                Aggregation::Sum => "sum",
                Aggregation::Mean => "mean",
                Aggregation::Min => "min",
                Aggregation::Max => "max",
                Aggregation::First => "first",
                Aggregation::Last => "last",
            })
            .collect();
        ensure!(aggregations.len() == plan.aggregations.len(), "duplicate aggregation");
        if let Some(model) = &plan.predict {
            ensure!(
                model.model_version == "mean-price-v1",
                "model is not registered; arbitrary Python prediction is never auto-parallelized"
            );
            match &model.partitioning {
                ModelPartitioning::SingleOwner => {}
                ModelPartitioning::IndependentSymbols => ensure!(
                    groups.contains("symbol"),
                    "independent-symbol model requires symbol grouping"
                ),
                ModelPartitioning::Mergeable { reducer_version } => {
                    ensure!(reducer_version == "sum-count-v1", "unregistered prediction reducer");
                }
            }
        }
        Ok(())
    }

    pub async fn execute(
        transaction: &mut MarketQuerySession,
        scope: &MessageScope,
        run_id: &Uuid,
        plan: &ComputePlan,
        now: DateTime<Utc>,
    ) -> Result<StrategyEventData> {
        Self::validate(plan, now)?;
        transaction.compute(scope, run_id, plan).await
    }
}

#[async_trait::async_trait]
impl IProvider for MarketQueryService {
    async fn close(&self) -> anyhow::Result<()> {
        MarketQueryService::close(self).await;
        Ok(())
    }
}
