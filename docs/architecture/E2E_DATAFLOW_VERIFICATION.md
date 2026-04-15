# E2E Data Flow Verification Report

## Architecture Overview

```
┌──────────────┐     MQTT      ┌───────────────┐     MQTT      ┌──────────────┐
│  Datafeed    │ ────────────> │   Evaluator   │ ────────────> │  Strategy    │
│  Ingestor    │  market data  │   (MAS)       │  hyperparams  │   Runner     │
└──────────────┘               └───────────────┘               └──────────────┘
                                      │                              │
                                      │                              │ MQTT
                                      │                              ▼
┌──────────────┐     MQTT      ┌───────────────┐               ┌──────────────┐
│  Exporter    │ <──────────── │    Order      │ <──────────── │  Exchange    │
│  (Kafka/     │  trading      │   Manager     │  trading      │   Client     │
│  Sheets)     │  placed       │               │  signals      │              │
└──────────────┘               └───────┬───────┘               └──────────────┘
                                      │
                                      │ MQTT
                                      ▼
                               ┌───────────────┐
                               │    Wallet     │
                               │   Manager     │
                               └───────┬───────┘
                                       │
                                       ▼
                                ┌───────────────┐
                                │  PostgreSQL   │
                                └───────────────┘

┌──────────────┐     MQTT      ┌───────────────┐
│  All Services│ ────────────> │  Log Service  │
│   (logs)     │  wf logs      │               │
└──────────────┘               └───────┬───────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    ▼                  ▼                  ▼
             ┌────────────┐    ┌────────────┐    ┌────────────┐
             │ PostgreSQL │    │  MinIO/OSS │    │ WebSocket  │
             │ (archiving)│    │ (archiving)│    │ (realtime) │
             └────────────┘    └────────────┘    └────────────┘
```

## Data Flow Status

| Flow | Status | Implementation File |
|------|--------|---------------------|
| datafeed → mqtt → evaluator | ✅ Partial | `src/datafeed/src/server/datafeed_ingestor.rs`, `src/evaluator/src/executor/adk/executor_mas.rs` |
| evaluator → mqtt → strategy | ⚠️ Missing | `src/evaluator/src/executor/adk/executor_mas.rs`, `src/strategy/runner/src/server/strategy_runner.rs` |
| strategy → mqtt → order | ✅ Complete | `src/strategy/runner/src/server/strategy_runner.rs`, `src/order/src/server/order_server.rs` |
| order → exchange | ✅ Complete | `src/order/src/manager/order_default.rs`, `src/exchange/src/client/` |
| order → mqtt → wallet | ✅ Complete | `src/order/src/manager/order_default.rs`, `src/wallet/src/server/wallet_server.rs` |
| wallet → postgresql | ✅ Complete | `src/wallet/src/manager/wallet_default.rs`, `src/core/src/modules/wallet/store/` |
| order → mqtt → exporter | ⚠️ Placeholder | `src/exporter/src/manager/exporter_kafka.rs`, `src/exporter/src/manager/exporter_googlestreet.rs` |
| all services → mqtt → log service | ✅ Complete | `src/logservice/src/server/log_server.rs` |
| log service → postgresql | ✅ Complete | `src/logservice/src/manager/logmanager_default.rs`, `src/core/src/sys/store/log_postgres.rs` |
| log service → minio/oss | ❌ Missing | Not implemented |
| log service → websocket | ✅ Complete | `src/logservice/src/server/log_server.rs` |

## Issues Found

### Issue 1: Log Service MinIO/OSS Archiving Not Implemented

**Severity**: Medium

**Description**: Logs are only archived to database (PostgreSQL/SQLite/MongoDB), not to object storage (MinIO/OSS) as specified in the requirements.

**Current Implementation**:
- `src/logservice/src/manager/logmanager_default.rs` - Only archives to database via `LogHandler.append()`

**Required Fix**:
1. Add MinIO/OSS client configuration
2. Implement async upload function for log batches
3. Configure log rotation and retention policy
4. Add health check for object storage connectivity

### Issue 2: Evaluator Event-Driven Trigger Not Implemented

**Severity**: Medium

**Description**: The `subscribe_from_input_stage()` method in evaluator executor only logs information but doesn't actually subscribe to datafeed events.

**Current Implementation**:
- `src/evaluator/src/executor/adk/executor_mas.rs:129-170` - Empty implementation with TODO comments

**Required Fix**:
1. Subscribe to datafeed topics (Twitter, TruthSocial, Kline)
2. Implement data accumulation threshold detection
3. Trigger orchestrator when threshold reached
4. Support dynamic prompt updates via messager

### Issue 3: Strategy Runner Hyperparameter Subscription Missing

**Severity**: High

**Description**: Strategy runner doesn't subscribe to `TOPIC_WF_HYPERPARAMETER_UPDATE` topic, so evaluator-calculated hyperparameters cannot be dynamically applied to running strategies.

**Current Implementation**:
- `src/evaluator/src/executor/adk/executor_mas.rs:233-253` - Publishes hyperparameter updates
- `src/strategy/runner/src/server/strategy_runner.rs` - No subscription to hyperparameter updates

**Required Fix**:
1. Add hyperparameter update handler in strategy runner
2. Subscribe to `TOPIC_WF_HYPERPARAMETER_UPDATE` topic
3. Update strategy configuration with new hyperparameters
4. Support hot-reload of strategy parameters

### Issue 4: Exporter Placeholder Implementation

**Severity**: Low

**Description**: Kafka and Google Sheets exporters are placeholders that only log data instead of actually exporting.

**Current Implementation**:
- `src/exporter/src/manager/exporter_kafka.rs:53-62` - Placeholder Kafka writer
- `src/exporter/src/manager/exporter_googlestreet.rs:48-62` - Placeholder Google Sheets writer

**Required Fix**:
1. Implement Kafka producer using `rdkafka` crate
2. Implement Google Sheets API integration using `google-sheets4` crate
3. Add error handling and retry logic
4. Configure batch vs stream modes

### Issue 5: Insufficient E2E Test Coverage

**Severity**: High

**Description**: Only basic indicator unit tests exist. No end-to-end tests for the complete data flow.

**Current Implementation**:
- `tests/integration/indicators/test_rsi.rs`
- `tests/integration/indicators/test_sma.rs`
- `tests/integration/indicators/test_ema.rs`

**Required Fix**:
1. Add integration tests for each service
2. Add E2E tests for complete data flows
3. Use mock MQTT broker for testing
4. Add test fixtures and scenarios

## Recommended Fix Priority

1. **High Priority**:
   - Issue 3: Strategy Runner Hyperparameter Subscription (blocks dynamic strategy adjustment)
   - Issue 5: E2E Test Coverage (required for validation)

2. **Medium Priority**:
   - Issue 1: Log Service MinIO/OSS Archiving (required for compliance)
   - Issue 2: Evaluator Event-Driven Trigger (improves responsiveness)

3. **Low Priority**:
   - Issue 4: Exporter Implementation (nice-to-have for data export)

## Test Plan

### Unit Tests
- [ ] Test each agent in evaluator (BootAgent, LoaderAgent, AlphaAgent, AuditorAgent)
- [ ] Test order manager risk check
- [ ] Test wallet manager trade event processing

### Integration Tests
- [ ] Test datafeed → MQTT → strategy flow
- [ ] Test strategy → MQTT → order → exchange flow
- [ ] Test order → MQTT → wallet → database flow
- [ ] Test evaluator → MQTT → strategy hyperparameter update flow

### E2E Tests
- [ ] Full workflow: datafeed → evaluator → strategy → order → exchange → wallet → exporter
- [ ] Log service: all services → MQTT → log service → database + websocket
- [ ] Error scenarios: network failures, message retries, idempotency

## Verification Checklist

- [ ] All MQTT topics correctly configured
- [ ] All services can connect to messager (EMQX)
- [ ] All services can connect to databases
- [ ] Log service archives to database successfully
- [ ] Log service streams to WebSocket successfully
- [ ] Wallet service updates ledger/balance/position correctly
- [ ] Order service executes trades via exchange client
- [ ] Strategy runner receives and processes market data
- [ ] Evaluator publishes hyperparameter updates
- [ ] MinIO/OSS archiving implemented and tested
- [ ] E2E tests pass for all data flows
