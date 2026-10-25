# E2E Test Running Guide

## Test Architecture

This project's E2E tests use real middleware components via Docker containers AND real business logic from `src/` directory.

### Key Design Principles

1. **Real Business Logic**: Tests call actual service code from `src/core`, `src/wallet`, etc.
   - Example: `f40_wallet_ledger_balance.rs` uses `PostgresWalletUpdater` from `src/core/src/modules/wallet/store/transaction/trade_postgres.rs`
   - Example: `WalletServiceRunner` wraps real business logic for test consumption

2. **Real Middleware**: Tests spin up actual Docker containers for:
   - PostgreSQL: Persistent storage (wallet, ledger, balance, position, logs)
   - TimescaleDB: Time-series data
   - Redis Cluster: Caching
   - Kafka: Event streaming
   - MinIO: Object storage
   - EMQX: MQTT messaging

### Components
| Component | Image | Purpose |
|-----------|-------|---------|
| **PostgreSQL** | `registry.cn-shenzhen.aliyuncs.com/wl4g/bitnami_postgresql:18.3` | Wallet, ledger, balance, position, log storage |
| **TimescaleDB** | `timescale/timescaledb:latest-pg15` | Time-series kline data for backtest |
| **Redis Cluster** | `registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_redis_cluster:7.0` | Caching layer (6 nodes: 3 masters + 3 replicas) |
| **Kafka** | `registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_kafka:3.5` | Event streaming for export results |
| **MinIO** | `bitnami/minio:latest` | Object storage |
| **EMQX** | `emqx/emqx:5` | MQTT broker for inter-service messaging |

### Test Categories

#### 1. Integration Tests (`integration/`)
Tests organized by data flow stages:
**Data Flow**: `datafeed → EMQX → backtest → EMQX → strategy → EMQX → order → wallet → DB`

- **flows/**: Flow tests ordered by data pipeline stage
  | File | Stage | Description |
  |------|-------|-------------|
  | `f00_full_pipeline` | Full | Complete: datafeed → strategy → order → wallet → DB → export |
  | `f10_datafeed_ingest_pub` | ① | Datafeed ingests market data & news from external sources |
  | `f20_strategy_signal_calc_pub` | ③ | Strategy calculates & publishes trading signals to EMQX |
  | `f21_evaluator_hyperparam_pub` | ④ | Evaluator calculates & publishes hyperparameters (parallel with strategy) |
  | `f30_order_trade_exec_pub` | ⑤ | Order service executes trades & publishes results |
  | `f40_wallet_ledger_balance` | ⑥ | Wallet service updates ledger & balance |
  | `f50_logservice_archive` | ⑦ | Log service archives logs to DB |
  | `f60_export_external` | ⑧ | Export to external systems (Kafka, HTTP, Google Sheets) |
  | `f70_logservice_audit` | ⑨ | Cross-service audit trail (compliance) |
  | `f80_notification_forward` | ⑩ | Notification routing and channel forwarding |
  | `f90_backtest_historical` | ⑪ | Historical replay and simulated backtest execution |

#### 2. Scenario Tests (`scenarios/`)
These scenario suites are reserved but currently disabled in
`tests/integration/mod.rs` because they do not compile against the current test
support API. They are not part of `make test-it`:

- **s10_error_handling**: Error scenarios and recovery
- **s20_concurrent_signals**: High volume and concurrent processing
- **s30_message_idempotency**: Duplicate detection and exactly-once semantics
- **s40_middleware_integration**: Direct middleware verification

## Container Lifecycle

1. **Setup**: `MiddlewareE2EContext::setup()` starts containers
   - Create Docker network: `e2e_test_network_<PID>`
   - Start PostgreSQL: `sigbot_e2e_<PID>_postgres`
   - Start TimescaleDB: `sigbot_e2e_<PID>_timescaledb`
   - Start Redis Cluster: `sigbot_e2e_<PID>_redis`
   - Start Kafka: `sigbot_e2e_<PID>_kafka`
   - Start MinIO: `sigbot_e2e_<PID>_minio`
   - Start EMQX: `sigbot_e2e_<PID>_emqx`

2. **Wait**: Health checks for all services
   - PostgreSQL: SQL `SELECT 1`
   - TimescaleDB: SQL `SELECT version()`
   - Redis Cluster: `PING` command
   - Kafka: `kafka-topics.sh --list`
   - MinIO: HTTP `GET /minio/health/live`
   - EMQX: HTTP `GET /status`

3. **Teardown**: Auto cleanup via `Drop` trait

## Running Tests

### Run All Tests

```bash
make test-it

# Short alias
make it
```

The Makefile invokes the `sigbot-integration-tests` library test target with
ignored E2E tests enabled. It runs suites serially because they share fixed
host ports. The equivalent low-level command is:

```bash
cargo test --locked -p sigbot-integration-tests --lib -- \
  --include-ignored --nocapture --test-threads=1
```

### Run Integration Tests

#### Flow Tests

```bash
# Flow 00: Full pipeline (datafeed -> strategy -> order -> wallet -> DB -> export)
make it IT_FILTER=f00_full_pipeline

# Flow 10: Datafeed ingests & publishes market data/news
make it IT_FILTER=f10_datafeed_ingest_pub

# Flow 20: Strategy calculates & publishes trading signals to EMQX
make it IT_FILTER=f20_strategy_signal_calc_pub

# Flow 21: Evaluator calculates & publishes hyperparameters (parallel with strategy)
make it IT_FILTER=f21_evaluator_hyperparam_pub

# Flow 30: Order service executes trades & publishes results
make it IT_FILTER=f30_order_trade_exec_pub

# Flow 40: Wallet service updates ledger & balance
make it IT_FILTER=f40_wallet_ledger_balance

# Flow 50: Log service archives logs to DB
make it IT_FILTER=f50_logservice_archive

# Flow 60: Export to external systems (Kafka, HTTP, Google Sheets)
make it IT_FILTER=f60_export_external

# Flow 70: Cross-service audit trail (compliance)
make it IT_FILTER=f70_logservice_audit

# Flow 80: Notification routing and forwarding
make it IT_FILTER=f80_notification_forward

# Flow 90: Historical backtest replay
make it IT_FILTER=f90_backtest_historical
```

### Run Scenario Tests

Scenario tests have no supported command until the `scenarios` module is
re-enabled. Use the corresponding flow tests for current integration coverage.

### Run Single Test

```bash
make it IT_FILTER=test_complete_workflow_datafeed_to_database
make it IT_FILTER=test_notification_forward_basic
```

## Verify Container Status

```bash
# View running containers
docker ps | grep sigbot_e2e

# View logs
docker logs sigbot_e2e_<PID>_postgres --tail 50
docker logs sigbot_e2e_<PID>_emqx --tail 50

# Manual PostgreSQL connection
docker exec -it sigbot_e2e_<PID>_postgres psql -U test -d testdb

# Manual TimescaleDB connection
docker exec -it sigbot_e2e_<PID>_timescaledb psql -U test -d klinedb

# Manual Redis Cluster connection
docker exec -it sigbot_e2e_<PID>_redis redis-cli -a test ping

# List Kafka topics
docker exec sigbot_e2e_<PID>_kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# EMQX dashboard
# Open http://localhost:18083 in browser
```

## Middleware Utility Classes

### Redis Utils
```rust
use crate::common::{MiddlewareE2EContext, RedisTestUtils};

let ctx = MiddlewareE2EContext::setup().await;
let redis = RedisTestUtils::from_context(&ctx).unwrap();

redis.set("key", "value").unwrap();
let val = redis.get("key").unwrap();
```

### Kafka Utils
```rust
use crate::common::{MiddlewareE2EContext, KafkaTestUtils};

let ctx = MiddlewareE2EContext::setup().await;
let kafka = KafkaTestUtils::from_context(&ctx);

kafka.create_topic("test-topic", 1).await.unwrap();
kafka.produce_message("test-topic", "key", "value").await.unwrap();
let msgs = kafka.consume_messages("test-topic", 1, 10).await.unwrap();
```

### MinIO Utils
```rust
use crate::common::{MiddlewareE2EContext, MinioTestUtils};

let ctx = MiddlewareE2EContext::setup().await;
let minio = MinioTestUtils::from_context(&ctx, "minioadmin".to_string(), "minioadmin".to_string());

minio.create_bucket("test-bucket").await.unwrap();
minio.put_object("test-bucket", "key.txt", b"content").await.unwrap();
```

### TimescaleDB Utils
```rust
use crate::common::{MiddlewareE2EContext, TimescaleDBTestUtils};

let ctx = MiddlewareE2EContext::setup().await;
let tsdb = TimescaleDBTestUtils::from_context(&ctx).await.unwrap();

tsdb.init_klines_schema().await.unwrap();
tsdb.insert_kline(time, "BTCUSDT", "1m", open, high, low, close, volume, quote_volume, trades_count).await.unwrap();
```

## Troubleshooting

### Docker Not Available
```bash
docker --version
docker ps
```

### Image Pull Failed
```bash
# Test Aliyun mirror connection
docker pull registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_kafka:3.5
```

### Port Conflict

Customize ports via configuration:

```rust
use crate::common::{
    MiddlewareE2EContext,
    PostgresContainerConfig, RedisClusterContainerConfig,
    KafkaContainerConfig, MinioContainerConfig, EmqxContainerConfig,
    TimescaleDBContainerConfig,
};

let postgres = PostgresContainerConfig {
    host_port: 15432,
    ..Default::default()
};

let timescaledb = TimescaleDBContainerConfig {
    host_port: 15433,
    ..Default::default()
};

let redis = RedisClusterContainerConfig {
    host_port: 16379,
    ..Default::default()
};

let kafka = KafkaContainerConfig {
    host_port: 19092,
    ..Default::default()
};

let emqx = EmqxContainerConfig {
    host_port: 11883,
    host_dashboard_port: 11083,
    ..Default::default()
};

let ctx = MiddlewareE2EContext::setup_with_configs(
    postgres,
    timescaledb,
    redis,
    kafka,
    MinioContainerConfig::default(),
    emqx,
).await;
```

### EMQX Connection Failed

Verify EMQX broker URL:
```bash
# Default MQTT port
telnet localhost 1883

# Dashboard
curl http://localhost:18083/status
```

### Redis Cluster Issues

```bash
# Check cluster nodes
docker exec sigbot_e2e_<PID>_redis redis-cli -a test cluster nodes

# Check cluster info
docker exec sigbot_e2e_<PID>_redis redis-cli -a test cluster info
```
