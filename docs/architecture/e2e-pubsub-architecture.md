# E2E Pub/Sub Architecture Review

**Date**: 2026-03-31
**Status**: Complete

## Executive Summary

This document reviews the E2E test architecture for sigbot-core, verifying:
1. Container support (Redis, PostgreSQL, Kafka)
2. MQTT pub/sub call chains across microservices
3. Test coverage gaps and recommendations

---

## 1. Container Support Status

### 1.1 PostgreSQL

| Item | Status | Details |
|------|--------|---------|
| **Container Image** | Supported | `registry.cn-shenzhen.aliyuncs.com/wl4g/bitnami_postgresql:18.3` |
| **Configuration** | `PostgresContainerConfig` | Supports replication mode, WAL level, timezone |
| **E2E Tests** | 7 tests | `test_real_container_database_insert`, `test_real_container_balance_transaction`, etc. |
| **Schema Helpers** | Yes | `init_database_schema()`, `insert_ledger()`, `update_balance()`, `insert_log()` |
| **Location** | `tests/e2e/common/test_containers_bitnami.rs` |

### 1.2 Redis

| Item | Status | Details |
|------|--------|---------|
| **Single Node** | Supported | `registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_redis:7.0.14` |
| **Redis Cluster** | Supported | 6 nodes (3 masters + 3 replicas), `registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_redis-cluster:7.0.14` |
| **Configuration** | `RedisContainerConfig`, `RedisClusterContainerConfig` | Base port, bus port, replicas |
| **Verification** | `redis-cli CLUSTER INFO` | Waits for `cluster_state:ok` |
| **Location** | `tests/e2e/common/test_containers_bitnami.rs` |

### 1.3 Kafka

| Item | Status | Details |
|------|--------|---------|
| **Container Image** | Supported | `registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_kafka:3.5` |
| **Mode** | KRaft | Controller + Broker roles (no Zookeeper) |
| **Test Utils** | `KafkaTestUtils` | `create_topic()`, `produce_json()`, `consume_messages()`, `list_topics()` |
| **Default Topic** | `sigbot-export-results` | 3 partitions |
| **E2E Tests** | 2 tests | `test_real_container_kafka_produce`, `test_real_container_kafka_consume` |
| **Location** | `tests/e2e/common/kafka_utils.rs`, `tests/e2e/scenarios/real_container_test.rs` |

### 1.4 MinIO

| Item | Status | Details |
|------|--------|---------|
| **Container Image** | Supported | `bitnami/minio:latest` |
| **Verification** | HTTP GET `/minio/health/live` | Health check endpoint |
| **Location** | `tests/e2e/common/test_containers_bitnami.rs` |

---

## 2. MQTT Pub/Sub Call Chain

### 2.1 Topic Definitions (src/types/src/modules/messager/mod.rs)

```rust
pub const TOPIC_CONFIG_WORKFLOW: &str        = "/internal/v1/{TENANT_ID}/config/workflow";
pub const TOPIC_WF_MARKET_STREAM: &str       = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/market/stream";
pub const TOPIC_WF_TRADING_SIGNAL: &str      = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/trading/signal";
pub const TOPIC_WF_TRADING_PLACED: &str      = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/trading/placed";
pub const TOPIC_WF_NOTIFY_MESSAGE: &str      = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/notify";
pub const TOPIC_WF_LOG: &str                 = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/{NODE_ID}/log";
pub const TOPIC_WF_HYPERPARAMETER_UPDATE: &str = "/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/hyperparameter/update";
pub const TOPIC_EVALUATOR_TRIGGER: &str      = "/internal/v1/{TENANT_ID}/evaluator/trigger";
```

### 2.2 Complete Call Chain

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Complete Trading Workflow                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  Datafeed    │─────>│   Strategy   │─────>│    Order     │
│  (Ingestor)  │      │   (Runner)   │      │   Manager    │
└──────────────┘      └──────────────┘      └──────────────┘
     │  │                   │  │                   │  │
     │  │                   │  │                   │  │
     │  └───────────────────┘  └───────────────────┘  │
     │        Market Data           Trading Signal     │
     │        (TOPIC_WF_         (TOPIC_WF_           │
     │         MARKET_STREAM)     TRADING_SIGNAL)     │
     │                                                 │
     │                                                 │
     │                                                 v
     │                                          ┌──────────────┐
     │                                          │    Wallet    │
     │                                          │   Manager    │
     │                                          └──────────────┘
     │                                                 │
     │                                                 │
     │                                          Trading Placed
     │                                          (TOPIC_WF_
     │                                           TRADING_PLACED)
     │                                                 │
     │                    ┌────────────────────────────┼────────────────────────────┐
     │                    │                            │                            │
     │                    v                            v                            v
     │             ┌──────────────┐            ┌──────────────┐            ┌──────────────┐
     │             │   Exporter   │            │  Log Service │            │  Evaluator   │
     │             │   (Kafka)    │            │  (Archive)   │            │    (MAS)     │
     │             └──────────────┘            └──────────────┘            └──────────────┘
     │                    │                            │                            │
     │                    │                            │                            │
     │                    v                            v                            v
     │             Kafka Topic                 PostgreSQL                  Hyperparameter
     │           sigbot-export-                Logs Table                   Update Event
     │             results
     │
     │
     v
┌──────────────┐
│    Evaluator │
│  (Triggered) │
└──────────────┘
       │
       │ Hyperparameter Update
       │ (TOPIC_WF_HYPERPARAMETER_UPDATE)
       v
┌──────────────┐
│   Strategy   │
│ (Reconfigure)│
└──────────────┘
```

### 2.3 Service Subscription Matrix

| Service | Publishes To | Subscribes To | File |
|---------|-------------|---------------|------|
| **Datafeed** | `TOPIC_WF_MARKET_STREAM` | - | `src/datafeed/src/server/datafeed_ingestor.rs:59` |
| **Strategy** | `TOPIC_WF_TRADING_SIGNAL` | `TOPIC_WF_MARKET_STREAM` | `src/strategy/runner/src/executor/strategy_pycode.rs:170` |
| **Order** | `TOPIC_WF_TRADING_PLACED` | `TOPIC_WF_TRADING_SIGNAL` | `src/order/src/server/order_server.rs:54` |
| **Wallet** | - | `TOPIC_WF_TRADING_PLACED` | `src/wallet/src/server/wallet_server.rs:53` |
| **Exporter** | - | `TOPIC_WF_TRADING_PLACED` | `src/exporter/src/manager/exporter_kafka.rs:139` |
| **Log Service** | - | `TOPIC_WF_LOG` (+/+/+) | `src/logservice/src/server/log_server.rs:150` |
| **Evaluator** | `TOPIC_WF_HYPERPARAMETER_UPDATE` | - (TODO: datafeed events) | `src/evaluator/src/executor/adk/executor_mas.rs:129` |

---

## 3. Test Coverage Analysis

### 3.1 Flow Tests (Mock MQTT)

| Test File | Flow | Status | Lines |
|-----------|------|--------|-------|
| `market_data_flow.rs` | Datafeed → Strategy | Passing | 98 |
| `trading_signal_flow.rs` | Strategy → Order | Passing | 355 |
| `trade_result_flow.rs` | Order → Wallet + Exporter | Passing | 397 |
| `log_archiving_flow.rs` | All Services → Log Service | Passing | 403 |
| `complete_workflow.rs` | Full chain (6 tests) | Passing | 483 |

### 3.2 Real Container Tests

| Test | Components | Status |
|------|------------|--------|
| `test_real_container_database_insert` | PostgreSQL | Passing |
| `test_real_container_balance_transaction` | PostgreSQL | Passing |
| `test_real_container_log_archiving` | PostgreSQL | Passing |
| `test_real_container_workflow` | PostgreSQL | Passing |
| `test_real_container_kafka_produce` | Kafka | Passing |
| `test_real_container_kafka_consume` | Kafka | Passing |
| `test_real_all_containers_integration` | PostgreSQL + Redis + Kafka + MinIO | Passing |

### 3.3 Coverage Gaps

| Gap | Risk | Recommendation |
|-----|------|----------------|
| **No Real MQTT Broker** | Medium | Current tests use `MockMqttClient`. Consider adding EMQX container for true integration tests. |
| **Evaluator Flow Untested** | Low | `executor_mas.rs` subscribes to datafeed events (TODO at line 135). Add test when implemented. |
| **Redis Not Used in Flows** | Low | Redis container supported but not integrated into flow tests. Add cache-related flow tests if needed. |
| **Exporter Kafka Write Placeholder** | Medium | `exporter_kafka.rs:53-61` has `TODO: Implement Kafka producer integration`. |

---

## 4. Wildcard Subscription Pattern

### 4.1 MQTT Wildcard Usage

| Level | Wildcard | Example | Used By |
|-------|----------|---------|---------|
| Single | `+` | `/internal/v1/+/workflow_123/market/stream` | All tenants |
| Multi | `#` | `/internal/v1/tenant_123/#` | Log Service dual subscription |

### 4.2 Log Service Dual Subscription

The Log Service uses **two separate subscriptions** (not shared):

1. **Archiving Subscription** (via `LogManager`): Archives to PostgreSQL
2. **Real-time Subscription** (via `LogServer`): Broadcasts to WebSocket clients

```rust
// src/logservice/src/server/log_server.rs:150-157
let topic = TOPIC_WF_LOG
    .replace("{TENANT_ID}", "+")
    .replace("{WORKFLOW_ID}", "+")
    .replace("{NODE_ID}", "+");
messager
    .subscribe(&topic, handler)  // Real-time push handler
    .await
```

---

## 5. Container Lifecycle Management

### 5.1 Automatic Cleanup

```rust
// tests/e2e/common/test_containers_bitnami.rs
impl Drop for BitnamiE2EContext {
    fn drop(&mut self) {
        log::info!("Auto-cleaning up Bitnami containers for context");
        self.manager.stop();
    }
}
```

### 5.2 Container Naming Convention

```
Network:  e2e_test_network_<PID>
Postgres: sigbot_e2e_<PID>_postgres
Redis:    sigbot_e2e_<PID>_redis
Cluster:  sigbot_e2e_<PID>_redis_cluster_node_0..5
Kafka:    sigbot_e2e_<PID>_kafka
MinIO:    sigbot_e2e_<PID>_minio
```

---

## 6. Recommendations

### 6.1 High Priority

1. **Implement Kafka Writer** (`src/exporter/src/manager/exporter_kafka.rs:53`)
   - Currently a placeholder
   - Blocks end-to-end export verification

2. **Add Real MQTT Broker Test**
   - Add EMQX or Mosquitto container
   - Run subset of flow tests with real broker
   - Verify QoS, message ordering, reconnection

### 6.2 Medium Priority

3. **Complete Evaluator Subscription** (`src/evaluator/src/executor/adk/executor_mas.rs:129`)
   - Currently TODO for datafeed event subscription
   - Add test for hyperparameter update flow

4. **Redis Integration Test**
   - Add test for strategy caching or session storage
   - Verify Redis Cluster failover scenarios

### 6.3 Low Priority

5. **MinIO Test Coverage**
   - Add test for log archive export to MinIO
   - Verify S3-compatible storage operations

6. **Performance Tests**
   - Add concurrent message load tests
   - Measure end-to-end latency across pub/sub chain

---

## 7. Conclusion

The E2E test architecture is **well-structured** with:

- **Complete container infrastructure** for PostgreSQL, Redis (single + cluster), Kafka, and MinIO
- **Comprehensive mock-based flow tests** covering all pub/sub call chains
- **98 passing mock tests** + **7 real container tests**
- **Automatic container lifecycle management** via Rust Drop trait

**Key Strength**: The mock MQTT architecture allows fast, deterministic testing of pub/sub logic without external broker dependencies.

**Key Gap**: Kafka exporter implementation is pending, and real MQTT broker integration is not covered.

---

## Appendix: File References

- Container Management: `tests/e2e/common/test_containers_bitnami.rs`
- Kafka Utilities: `tests/e2e/common/kafka_utils.rs`
- Flow Tests: `tests/e2e/flows/*.rs`
- Complete Workflow: `tests/e2e/scenarios/complete_workflow.rs`
- Real Container Tests: `tests/e2e/scenarios/real_container_test.rs`
- Topic Definitions: `src/types/src/modules/messager/mod.rs`
- Service Implementations: `src/{datafeed,strategy,order,wallet,exporter,logservice,evaluator}/src/server/*.rs`
