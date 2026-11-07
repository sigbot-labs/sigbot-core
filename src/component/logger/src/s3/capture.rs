// SPDX-License-Identifier: GPL-3.0-only

use crate::s3::s3::LogAppend;
use crate::s3::s3::LogLine;
use crate::s3::s3::S3Logger;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common_context::IProvider;
use common_telemetry::readiness::ReadinessGuard;
use common_telemetry::readiness::RuntimeReadiness;
use common_telemetry::tracing;
use common_telemetry::tracing::field::Field;
use common_telemetry::tracing::field::Visit;
use common_telemetry::tracing::span::Attributes;
use common_telemetry::tracing::span::Id;
use common_telemetry::tracing::span::Record;
use common_telemetry::tracing::Event;
use common_telemetry::tracing::Instrument;
use common_telemetry::tracing::Subscriber;
use common_telemetry::tracing_context::MessageTrace;
use common_telemetry::tracing_subscriber::layer::Context as LayerContext;
use common_telemetry::tracing_subscriber::layer::Layer;
use common_telemetry::tracing_subscriber::registry::LookupSpan;
use regex::Regex;
use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use sigbot_messager::messager::IMessager;
use sigbot_messager::messager::MessageHandler;
use sigbot_types::messager::envelope::DurableMessage;
use sigbot_types::messager::envelope::MessageKind;
use sigbot_types::messager::envelope::TraceContext;
use sigbot_types::messager::messager::MessagerProvider;
use sigbot_types::messager::topic::MessageScope;
use sigbot_types::Uuid;
use std::cell::Cell;
use std::fmt;
use std::fmt::Write;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct LogStreamIdentity {
    pub scope: MessageScope,
    pub workflow_id: Uuid,
    pub node_id: i64,
    pub run_id: Uuid,
}

#[derive(Clone)]
pub struct LogCaptureOptions {
    pub queue_capacity: usize,
    pub flush_timeout: Duration,
}
impl Default for LogCaptureOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            flush_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogCaptureStats {
    pub accepted: u64,
    pub durable: u64,
    pub rejected: u64,
    pub write_failures: u64,
}

struct CaptureState {
    admission: std::sync::Mutex<()>,
    accepted: AtomicU64,
    durable: AtomicU64,
    rejected: AtomicU64,
    write_failures: AtomicU64,
    failed: AtomicBool,
    closed: AtomicBool,
    shutdown_complete: AtomicBool,
    readiness: ReadinessGuard,
}
impl CaptureState {
    fn fail(&self, reason: &'static str) {
        self.readiness.set(false);
        if !self.failed.swap(true, Ordering::AcqRel) {
            tracing::warn!(target:"sigbot_archive_internal", reason, "Rust log capture is not durable");
        }
    }
    fn recover(&self) {
        if self.rejected.load(Ordering::Acquire) == 0 && !self.closed.load(Ordering::Acquire) {
            self.failed.store(false, Ordering::Release);
            self.readiness.set(true);
        }
    }
    fn stats(&self) -> LogCaptureStats {
        LogCaptureStats {
            accepted: self.accepted.load(Ordering::Acquire),
            durable: self.durable.load(Ordering::Acquire),
            rejected: self.rejected.load(Ordering::Acquire),
            write_failures: self.write_failures.load(Ordering::Acquire),
        }
    }
}
struct CaptureTaskGuard(Arc<CaptureState>);
impl Drop for CaptureTaskGuard {
    fn drop(&mut self) {
        if self.0.shutdown_complete.load(Ordering::Acquire) {
            self.0.readiness.finish();
        } else {
            self.0.readiness.fail();
        }
    }
}

struct CapturedEvent {
    time: DateTime<Utc>,
    level: String,
    source: String,
    content: String,
    traceparent: Option<String>,
}
enum CaptureCommand {
    Event(CapturedEvent),
    Barrier(oneshot::Sender<Result<()>>),
}

pub struct ArchiveTracingLayer {
    sender: mpsc::Sender<CaptureCommand>,
    state: Arc<CaptureState>,
}
pub struct ArchiveCapture {
    sender: mpsc::Sender<CaptureCommand>,
    state: Arc<CaptureState>,
    options: LogCaptureOptions,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ArchiveCapture {
    pub fn start(
        archive: Arc<S3Logger>,
        identity: LogStreamIdentity,
        options: LogCaptureOptions,
    ) -> Result<(ArchiveTracingLayer, Arc<Self>)> {
        identity.scope.validate()?;
        ensure!(
            identity.workflow_id.get_version_num() == 7
                && identity.node_id > 0
                && identity.run_id.get_version_num() == 7,
            "invalid log stream identity"
        );
        ensure!(
            (1..=4096).contains(&options.queue_capacity)
                && !options.flush_timeout.is_zero()
                && options.flush_timeout <= Duration::from_secs(30),
            "invalid log capture bounds"
        );
        let (sender, receiver) = mpsc::channel(options.queue_capacity);
        let state = Arc::new(CaptureState {
            admission: std::sync::Mutex::new(()),
            accepted: AtomicU64::new(0),
            durable: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            readiness: RuntimeReadiness::task("strategy.rust_logs"),
        });
        let worker = ArchiveCaptureWorker {
            archive,
            identity,
            state: state.clone(),
        };
        state.recover();
        let guard = CaptureTaskGuard(state.clone());
        let task = tokio::spawn(worker.run(receiver, guard));
        let layer = ArchiveTracingLayer {
            sender: sender.clone(),
            state: state.clone(),
        };
        Ok((
            layer,
            Arc::new(Self {
                sender,
                state,
                options,
                task: Mutex::new(Some(task)),
            }),
        ))
    }

    pub fn stats(&self) -> LogCaptureStats {
        self.state.stats()
    }

    /// FIFO barrier covers accepted events before this call, never mere channel enqueue.
    pub async fn flush_durable(&self) -> Result<()> {
        let (send, receive) = oneshot::channel();
        let result = tokio::time::timeout(self.options.flush_timeout, async {
            self.sender
                .send(CaptureCommand::Barrier(send))
                .await
                .context("log capture stopped")?;
            receive.await.context("log capture worker stopped")?
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.state.fail("durable_barrier_failed");
                Err(error)
            }
            Err(_) => {
                self.state.fail("durable_barrier_timeout");
                anyhow::bail!("log WAL durability barrier timed out")
            }
        }
    }

    /// Stop intake first; no accepted record is silently discarded by graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        {
            let _admission = self.state.admission.lock().unwrap_or_else(|error| error.into_inner());
            self.state.closed.store(true, Ordering::Release);
        }
        let result = self.flush_durable().await;
        self.state.shutdown_complete.store(result.is_ok(), Ordering::Release);
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
        }
        self.state.readiness.set(false);
        result
    }

    pub fn wrap_messager(self: &Arc<Self>, inner: Arc<dyn IMessager>) -> Arc<dyn IMessager> {
        Arc::new(ArchiveMessager {
            inner,
            capture: self.clone(),
        })
    }
}

struct ArchiveCaptureWorker {
    archive: Arc<S3Logger>,
    identity: LogStreamIdentity,
    state: Arc<CaptureState>,
}
impl ArchiveCaptureWorker {
    fn message(&self, event: &CapturedEvent) -> LogAppend {
        let trace = MessageTrace::child("logger.append", event.traceparent.as_deref().unwrap_or(""));
        LogAppend::new(DurableMessage {
            version: 2,
            scope: self.identity.scope.clone(),
            workflow_id: Some(self.identity.workflow_id),
            account_id: None,
            node_id: self.identity.node_id,
            target_node_id: None,
            run_id: self.identity.run_id.clone(),
            seq: 0,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: event.time,
            available_time: event.time,
            produced_at: Utc::now(),
            traceparent: trace.traceparent(),
            payload: LogLine {
                level: event.level.clone(),
                source: event.source.clone(),
                content: event.content.clone(),
            },
        })
    }
    async fn run(self, mut receiver: mpsc::Receiver<CaptureCommand>, _guard: CaptureTaskGuard) {
        while let Some(command) = receiver.recv().await {
            match command {
                CaptureCommand::Event(event) => {
                    let mut message = self.message(&event);
                    loop {
                        match self.archive.append_generated(&mut message).await {
                            Ok(_) => {
                                self.state.durable.fetch_add(1, Ordering::Release);
                                self.state.recover();
                                break;
                            }
                            Err(_) => {
                                self.state.write_failures.fetch_add(1, Ordering::Relaxed);
                                self.state.fail("wal_append_failed");
                                let retry = MessageTrace::child("logger.append.retry", &message.message().traceparent);
                                if message.retry(retry.traceparent(), Utc::now()).is_err() {
                                    self.state.fail("retry_limit");
                                    return;
                                }
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                        }
                    }
                }
                CaptureCommand::Barrier(reply) => {
                    let result = if self.state.rejected.load(Ordering::Acquire) > 0 {
                        Err(anyhow::anyhow!(
                            "Rust log capture rejected events; durability cannot be reported as complete"
                        ))
                    } else {
                        Ok(())
                    };
                    let _ = reply.send(result);
                }
            }
        }
    }
}

#[derive(Default)]
struct TraceFields {
    traceparent: Option<String>,
}
impl Visit for TraceFields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "traceparent" {
            let mut text = BoundedText::new(128);
            let _ = write!(&mut text, "{value:?}");
            let value = text.text.trim_matches('"');
            if TraceContext::parse(value).is_ok() {
                self.traceparent = Some(value.to_owned());
            }
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "traceparent" && TraceContext::parse(value).is_ok() {
            self.traceparent = Some(value.to_owned());
        }
    }
}
thread_local! { static CAPTURING:Cell<bool>=const {Cell::new(false)}; }
struct ReentryGuard;
impl Drop for ReentryGuard {
    fn drop(&mut self) {
        CAPTURING.with(|flag| flag.set(false));
    }
}

impl<S> Layer<S> for ArchiveTracingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut fields = TraceFields::default();
            attrs.record(&mut fields);
            span.extensions_mut().insert(fields);
        }
    }
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(fields) = span.extensions_mut().get_mut::<TraceFields>() {
                values.record(fields);
            }
        }
    }
    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        let meta = event.metadata();
        // The first integration is the dedicated Strategy host. Archive/SDK transport
        // internals and other services never feed their own diagnostics back into WAL.
        if !meta.target().starts_with("sigbot_strategy_") || !matches!(meta.level().as_str(), "INFO" | "WARN" | "ERROR")
        {
            return;
        }
        if CAPTURING.with(|flag| flag.replace(true)) {
            return;
        }
        let _guard = ReentryGuard;
        if self.state.closed.load(Ordering::Acquire) {
            return;
        }
        let mut fields = CaptureFields::default();
        event.record(&mut fields);
        let mut traceparent = fields.traceparent.take();
        if traceparent.is_none() {
            if let Some(scope) = ctx.event_scope(event) {
                for span in scope.from_root() {
                    if let Some(trace) = span
                        .extensions()
                        .get::<TraceFields>()
                        .and_then(|value| value.traceparent.clone())
                    {
                        traceparent = Some(trace);
                    }
                }
            }
        }
        let content = fields.finish(traceparent.is_none());
        let record = CapturedEvent {
            time: Utc::now(),
            level: meta.level().as_str().to_owned(),
            source: format!("rust:{}", meta.target()),
            content,
            traceparent,
        };
        // Serialize only admission, never formatting or I/O. Shutdown closes this
        // gate before enqueuing its barrier, so accepted events cannot land after it.
        let _admission = self.state.admission.lock().unwrap_or_else(|error| error.into_inner());
        if self.state.closed.load(Ordering::Acquire) {
            return;
        }
        match self.sender.try_send(CaptureCommand::Event(record)) {
            Ok(()) => {
                self.state.accepted.fetch_add(1, Ordering::Release);
            }
            Err(_) => {
                self.state.rejected.fetch_add(1, Ordering::Release);
                self.state.fail("bounded_queue_rejected");
                self.state.readiness.fail();
            }
        }
    }
}

struct BoundedText {
    text: String,
    limit: usize,
}
impl BoundedText {
    fn new(limit: usize) -> Self {
        Self {
            text: String::new(),
            limit,
        }
    }
}
impl fmt::Write for BoundedText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let remaining = self.limit.saturating_sub(self.text.len());
        if value.len() <= remaining {
            self.text.push_str(value);
            return Ok(());
        }
        let mut boundary = remaining;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        self.text.push_str(&value[..boundary]);
        Err(fmt::Error)
    }
}

#[derive(Default)]
struct CaptureFields {
    fields: Map<String, Value>,
    traceparent: Option<String>,
    truncated: bool,
}
impl CaptureFields {
    fn insert(&mut self, key: &str, value: String) {
        if LogRedactor::pairing(key) {
            return;
        }
        if key == "traceparent" {
            let value = value.trim_matches('"');
            if TraceContext::parse(value).is_ok() {
                self.traceparent = Some(value.to_owned());
            }
            return;
        }
        if self.fields.len() >= 24 {
            self.truncated = true;
            return;
        }
        self.fields.insert(
            key.to_owned(),
            if LogRedactor::sensitive(key) {
                json!("[REDACTED]")
            } else {
                json!(LogRedactor::text(&value))
            },
        );
    }
    fn finish(mut self, background: bool) -> String {
        self.fields.insert("background".into(), json!(background));
        self.fields.insert("fields_truncated".into(), json!(self.truncated));
        let content = Value::Object(self.fields.clone()).to_string();
        if content.len() <= 16_384 {
            content
        } else {
            json!({"message":self.fields.remove("message").unwrap_or(json!("bounded tracing event")),
                "background":background,"fields_truncated":true})
            .to_string()
        }
    }
}
impl Visit for CaptureFields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if LogRedactor::pairing(field.name()) {
            return;
        }
        if LogRedactor::sensitive(field.name()) {
            self.insert(field.name(), "[REDACTED]".into());
            return;
        }
        let mut text = BoundedText::new(1024);
        if write!(&mut text, "{value:?}").is_err() {
            self.truncated = true;
        }
        self.insert(field.name(), text.text);
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        let mut text = BoundedText::new(1024);
        if text.write_str(value).is_err() {
            self.truncated = true;
        }
        self.insert(field.name(), text.text);
    }
}

pub struct LogRedactor;
impl LogRedactor {
    fn pairing(key: &str) -> bool {
        key.replace('_', "").eq_ignore_ascii_case("requestspanid")
    }
    fn sensitive(key: &str) -> bool {
        let key = key.to_ascii_lowercase().replace(['_', '-'], "");
        [
            "password",
            "passphrase",
            "secret",
            "token",
            "authorization",
            "apikey",
            "accesskey",
            "cookie",
            "privatekey",
            "credential",
        ]
        .iter()
        .any(|name| key.contains(name))
    }
    fn value(value: &mut Value) {
        match value {
            Value::Object(map) => {
                map.retain(|key, _| !Self::pairing(key));
                for (key, value) in map.iter_mut() {
                    if Self::sensitive(key) {
                        *value = json!("[REDACTED]");
                    } else {
                        Self::value(value);
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    Self::value(value);
                }
            }
            Value::String(value) => *value = Self::plain(value),
            _ => {}
        }
    }
    pub fn text(value: &str) -> String {
        if let Ok(mut json) = serde_json::from_str::<Value>(value) {
            Self::value(&mut json);
            json.to_string()
        } else {
            Self::plain(value)
        }
    }
    fn plain(value: &str) -> String {
        // Debug-formatted envelopes are not necessarily valid JSON. Omit such text
        // entirely rather than risk persisting a temporary pairing value.
        if value.replace('_', "").to_ascii_lowercase().contains("requestspanid") {
            return "[OMITTED TEMPORARY PAIRING]".into();
        }
        // serde_json::Value Debug uses Object/String wrappers, not JSON. Once a
        // sensitive assignment is found, omit the entire unparsed value.
        static SENSITIVE_ASSIGNMENT: OnceLock<Regex> = OnceLock::new();
        if SENSITIVE_ASSIGNMENT.get_or_init(|| Regex::new(
            r#"(?i)\b[a-z0-9_-]*(?:password|passphrase|secret|token|authorization|api[_-]?key|access[_-]?key|cookie|private[_-]?key|credential)[a-z0-9_-]*["'\\\s]*[:=]"#,
        ).expect("sensitive assignment pattern")).is_match(value) {
            return "[REDACTED SENSITIVE MESSAGE]".into();
        }
        static PATTERNS: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();
        let patterns = PATTERNS.get_or_init(|| {
            vec![
                (
                    Regex::new(r"(?i)Bearer\s+[A-Za-z0-9._~+/=-]+").expect("bearer pattern"),
                    "Bearer [REDACTED]",
                ),
                (
                    Regex::new(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@\s]+:[^/@\s]+@").expect("URI pattern"),
                    "${1}[REDACTED]@",
                ),
            ]
        });
        patterns.iter().fold(value.to_owned(), |text, (pattern, replacement)| {
            pattern.replace_all(&text, *replacement).into_owned()
        })
    }
}

struct ArchiveMessager {
    inner: Arc<dyn IMessager>,
    capture: Arc<ArchiveCapture>,
}
#[async_trait]
impl IMessager for ArchiveMessager {
    fn provider(&self) -> MessagerProvider {
        self.inner.provider()
    }

    async fn publish(&self, to: &str, message: &str) -> Result<String> {
        self.inner.publish(to, message).await
    }
    async fn publish_confirmed(&self, to: &str, message: &str) -> Result<()> {
        self.inner.publish_confirmed(to, message).await
    }
    async fn subscribe(&self, topic: &str, handler: MessageHandler) -> Result<()> {
        let capture = self.capture.clone();
        self.inner.subscribe(topic,Arc::new(move|bytes| {
            let capture=capture.clone();let handler=handler.clone();
            Box::pin(async move {
                let incoming=serde_json::from_slice::<Value>(&bytes).ok().and_then(|value|
                    value.get("traceparent").and_then(Value::as_str).map(str::to_owned))
                    .filter(|value|TraceContext::parse(value).is_ok());
                let context=MessageTrace::child("strategy.consume",incoming.as_deref().unwrap_or(""));
                let span=tracing::info_span!(target:"sigbot_strategy_runner::runtime","strategy.delivery",traceparent=%context.traceparent());
                let result=async {
                    let result=handler(bytes).await;
                    tracing::info!(target:"sigbot_strategy_runner::runtime",success=result.is_ok(),"strategy delivery processed");
                    result
                }.instrument(span).await;
                capture.flush_durable().await?;
                result
            })
        })).await
    }
}

#[async_trait::async_trait]
impl IProvider for ArchiveMessager {
    async fn close(&self) -> anyhow::Result<()> {
        self.inner.close().await
    }
    async fn init(&self) -> anyhow::Result<()> {
        self.inner.init().await
    }
}
