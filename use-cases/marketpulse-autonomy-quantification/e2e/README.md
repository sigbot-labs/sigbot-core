# Marketpulse autonomy quantification E2E

This case follows AgentVault's deployment, scenario, evidence and cleanup structure. Twitter, TruthSocial and OpenAI are explicit external boundaries. Datafeed, Evaluator, Strategy, Order, Wallet and Backtest run production Rust components and the isolated Python worker. `runner.py` is the only case execution entry point; the root Makefile discovers every case.

## Run

From the repository root:

```bash
make build test
make e2e-docker E2E_ARGS='--rounds 2' E2E_TIMEOUT=300
make e2e-k8s E2E_ARGS='--reuse-build' E2E_TIMEOUT=360
# Only reuse an already owned and initialized environment:
make e2e E2E_ARGS='--reuse-environment --reuse-build --rounds 2'
```

`make e2e` runs Docker then Kubernetes. `config/sigbot/matrix.json` selects 27 Docker and 3 Kubernetes scenarios. Direct `runner.py` supports repeated/comma-separated `--scenario`, rounds, platform, timeout, deploy, build reuse and cleanup. For a focused run:

```bash
cd use-cases/marketpulse-autonomy-quantification/e2e
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m playwright install chromium
.venv/bin/python runner.py --scenario runtime,workflow,recovery,logs,web --reuse-build --timeout 300
.venv/bin/python runner.py --stop-runtime --reuse-build
```

`E2E_PYTHON` selects an existing interpreter. `PLAYWRIGHT_CHROMIUM_EXECUTABLE` reuses compatible Chromium; legacy `MARKETPULSE_CHROMIUM_PATH` remains readable. A configured executable avoids another browser download. `CONTAINER_CLI` selects local Docker/Podman, including `sudo -n podman`; `PODMAN_COMPOSE_PROVIDER` can select Compose v2. Builds use two jobs and clean `target/` before building if it exceeds 10 GiB. Restricted networks can use `HTTPS_PROXY=http://127.0.0.1:8800`.

## Configuration and retained data

`config/sigbot/config.yaml` contains endpoints, identity scope and credential environment references. `CaseEnvironment` loads the shared image catalog and generates local fixture credentials in private `.runtime/fixture.env` (directory 0700, file 0600). Existing fixture credentials can be supplied explicitly. Deployment rejects conflicting database passwords and unrelated remote database URLs. Scoped four-hour MQTT JWTs authenticate the case; they are separate from Authguard browser identity.

The root registry probe/`IN_CN_GFW` selects `IMAGE_SOURCE=aliyun|global`; Aliyun mode rejects foreign registry overrides. Image availability is still required. Loopback ports are PostgreSQL 15439, EMQX 18883, MinIO 19000, Kafka 19092, Web 15173 and external mocks 18481–18483. Containers have bounded resources.

New Compose installations use a pinned official Timescale HA index and the separate `sigbot-marketpulse-postgres-ha-pg16-v1` volume. Existing plain PostgreSQL data is retained: back up, restore into a new volume and reconcile before switching. Do not attach an old PG data directory to a replacement image. Existing databases require explicit extension-owner bootstrap through `deploy/helm/sigbot/bootstrap/postgres-extensions.sql`. The retained local main-chain database remains plain PG; the extension scenario provisions its own actual dual-extension instance.

When the official MinIO registry is unavailable, `MARKETPULSE_MINIO_BINARY` can select the official binary pinned in `config/minio/provenance.json`. Deployment verifies its checksum, builds a bounded non-root carrier and records provenance. This runs real MinIO in a container; it is not an official registry image. Existing data ownership must match the runtime UID/GID.

The realtime chain uses **Paper** and independent wallet/account **20001**. Legacy wallet 1 and all historical financial facts remain intact. Replay creates new Backtest wallets. Deployment `scope.env` never selects trading behavior. Opening capital enters through external FundingFact and the actual Wallet; the harness does not seed balances.

## Verification boundaries

| Scenarios | Actual coverage |
| --- | --- |
| `structure,schemas,infra,acl,database` | 12 Provider SPI contracts, layout/size rules, schema provenance, actual middleware, MQTT5 isolation, immutable finance |
| `boundaries,rust-boundaries,rust-policy,rust-market` | Actual REST/WS/MCP/A2A clients, PostgreSQL revisions/watermarks; official Fed/Treasury and Binance/Hyperliquid readonly requests, with network failures counted as failures |
| `rust-cache,rust-contracts,rust-extensions` | Authenticated Redis single-node and three-primary cluster, 11 tests each; actual PG/HTTP/S3 contracts; disposable TimescaleDB/pgvector and numeric/late-revision equivalence |
| `rust-messaging,rust-owners,rust-groups` | Durable MQTT replay/attempt/span, current ownership fencing and deduplication, unknown-order reconciliation, mode fences before submit or observation, actual PG compensation coordinator |
| `rust-effects,rust-resting,rust-export,rust-logs` | Real Python callbacks and atomic effects, subsequent-tick matching, actual Kafka, Rust tracing/WAL/object-store contracts |
| `runtime,workflow,recovery,replay,groups,logs` | Eight Rust services and isolated Python, actual reservation/execution/journal chain, owned-process SIGKILL/replay, two isolated deterministic history runs, same-venue two-account compensation, immutable Python/Rust log hashes and cursors |
| `web` | Actual Chromium navigation, menus, Workflow canvas and screenshots, without API interception |
| `kubernetes-prepare,rust-deployment` | Local ABI carrier, real Pod connectivity, production Kubernetes provider create/update/recover/stop and resource ownership fencing |
| `web-full` (separate gate) | Actual Authguard login followed by protected save/start/notifications/trace UI; missing `MARKETPULSE_BROWSER_STORAGE_STATE` fails |

Realtime prices come from public Binance responses; the product simulator receives Paper orders without sending funds to external venues. Pipeline injects only external news, fsyncs evidence before MQTT ACK and requires new orders, executions and balanced non-funding journals. Replay freezes actual finite history and recorded AI outputs with fixed ordering, clock and fees, then compares two isolated runs and restarts the owner. Asynchronous realtime latency is not assumed equivalent to historical replay timing. Queries execute bounded PostgreSQL/Timescale plans; Redis hot caching, distributed incremental shards and automatic arbitrary-Python parallelism are not claimed.

Old unsent Outbox facts are replayed rather than deleted or relabeled with fresh timestamps. Large backlog can exceed a short timeout; retain failed evidence and verify fresh input after catch-up. Hard risk must continue rejecting stale evidence.

Build reuse checks all transitive sources and compiler `.d` inputs and records executable SHA-256. Ignored integration tests must actually execute to count as passed. Kubernetes always uses `KUBECONFIG=/etc/rancher/k3s/k3s.yaml`. Its local ABI debug carrier is separate from a portable release image and from running the entire financial pipeline in Kubernetes.

Timestamped UTC reports retain failures. The environment remains available by default. `--stop-runtime` checks PID start time and executable ownership; `--cleanup-workers` checks the worker namespace owner; `--cleanup` removes this case's deployment and volumes after evidence is archived. Redis/extension fixtures clean their own resources. Named images and unrelated workloads are retained. See the central [acceptance matrix](../../../docs/architecture/runtime-v2-acceptance_ZH.md) for current results.

## External gates

X uses the official `https://api.x.com/2/openapi.json` snapshot with version/time/checksum provenance. Posts are explicitly synthetic and schema-validated. No publicly verifiable official commercial TruthSocial OpenAPI was obtained: the fixture is a clearly labeled Mastodon-compatible subset. The fixed LLM response does not establish model quality. Hosted Login/full authenticated UI, funded live or cross-venue execution and actual SMTP/Telegram/Sheets remain independent gates.
