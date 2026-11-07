# SigBot Core Architecture

[中文](overview_ZH.md) · [Documentation index](../index.md)

**Status:** Current architecture and implementation boundary. The alignment
table records the remaining evidence gaps; it no longer describes superseded
pre-runtime-v2 scaffolding.

SigBot is an AI-assisted quantitative trading system, not a general-purpose
workflow or DAG platform. Its central design is a constrained, typed Workflow:
a non-deterministic AI side path classifies the current
market structure and recommends bounded hyperparameters, while deterministic
Python strategy code combines that recommendation with market data and the
current portfolio state to make the final trading decision.

The AI never places orders. When the market cannot be classified, confidence
is too low, an evaluation has expired, or a required input is inconsistent,
the deterministic boundary emits no opening intent.

## System Model

```mermaid
flowchart LR
    Clients[Clients / Researcher] --> API[SigbotHttpStarter]
    API --> CoreDB[(Core control-plane DB)]
    Controller[SigbotControllerStarter] --> CoreDB

    Sources[Markets / News] --> Datafeed[Tenant SigbotDatafeedStarter]
    Controller -. logical binding .-> Datafeed
    Controller -. strategy-scoped runtime .-> Evaluator[SigbotEvaluatorStarter]
    Controller -. strategy-scoped runtime .-> Strategy[SigbotStrategyRunner]

    Datafeed -->|DatafeedEvent| Evaluator
    Datafeed -->|DatafeedEvent| Strategy
    Evaluator -->|HyperparametersUpdate| Strategy
    Wallet[SigbotWalletServer] -->|versioned portfolio payload| Strategy
    Strategy -->|Effects.intents| Order[SigbotOrderServer]
    Order --> Exchange[IExchange]
    Exchange -->|execution facts| Order
    Order -->|ExecutionFact / OrderStatusFact| Wallet
    Order -->|versioned export request| Exporter[SigbotExporterServer]
    Order -. notification event .-> Notifier[SigbotNotifier]
    Wallet -. notification event .-> Notifier

    Order --> OrderDB[(ord_* table domain)]
    Wallet --> WalletDB[(wlt_* table domain)]

    Backtest[SigbotBacktestServer] -. same contracts .-> Strategy
    Backtest -. backtest adapter .-> Exchange
```

The platform plane consists of API, Controller, and Web. HTTP, MCP, and A2A APIs
own CRUD for tenant bindings, Workflows, worker connections, and related
configuration; Controller owns startup, update, shutdown, and health recovery
for the nine tenant components. Authguard remains the sole owner of users,
memberships, roles, and permissions.

The data plane consists of Backtest, Datafeed, Evaluator, Strategy, Logger,
Exporter, Notifier, Order, and Wallet. Controller dynamically deploys these nine
components with tenant/Workflow resource isolation; they asynchronously pub/sub
the different Workflow node types. A long-running tenant-shared Workflow uses
the same topology without weakening tenant/Workflow topic and state boundaries.

## Decision Contract

The live decision path joins three versioned inputs:

1. `DatafeedEvent` and its bounded market window, normalized by Datafeed.
2. The latest non-expired `HyperparametersUpdate` whose evaluated watermark
   does not look beyond the decision event.
3. A current versioned portfolio payload owned by Wallet.

`HyperparametersUpdate` contains at least the market regime
(`trend`, `range`, `stabilizing`, `suppression`, or `unknown`) probabilities,
schema-validated hyperparameter overrides or weights, input watermark, model
and prompt provenance, effective time, and expiry. The evaluator may be
non-deterministic; the snapshot is immutable and auditable.

`SigbotStrategyRunner` validates the snapshot against the strategy's declared
parameter schema and assembles one immutable execution context. For the same
market window, evaluation snapshot, portfolio version, strategy version, and
risk policy, Python strategy execution must produce the same `Effects`.
`UNKNOWN`, stale or invalid inputs produce no opening intent. Only validated
trade intents cross the Order boundary through MQTT.

## Constrained Workflow

```text
Workflow feed route
  -> Evaluator side path -----------+
  -> Strategy Runner <--------------+ EvaluationSnapshot
       ^
       +------------------------------ PortfolioSnapshot
       -> Order Manager -> Exchange
              -> Wallet Manager
              -> Exporter
              -> Notifier (typed operational events only)
```

Workflow graphs are configurable but not arbitrary code graphs. Nodes are
limited to the nine registered component modules, and edges must resolve to an
allowed v2 topic or a validated module-routing default. The graph can disable
optional capabilities and contain multiple typed nodes, but it cannot introduce
an unknown node kind, bypass Order/Wallet, or create a direct AI-to-Exchange edge.

## Core Services

| Service | Runtime scope | Canonical responsibility | Design |
|---|---|---|---|
| API | Platform service | Exposes HTTP, MCP, and A2A CRUD for tenant bindings, Workflows, workers, and connections | [APIServer](engine/apiserver/overview.md) |
| Controller | Platform service | Manages nine tenant data-plane components on remote Docker/Kubernetes workers | [Controller](engine/controller/overview.md) |
| Web | Platform service | Configures and observes tenant Workflows through Authguard-protected APIs | [APIServer](engine/apiserver/overview.md) |
| Datafeed | Tenant/Workflow component | Normalizes market/news data and publishes event/availability-time records | [Datafeed](engine/datafeed/overview.md) |
| Evaluator | Tenant/Workflow component | Classifies market structure and publishes bounded versioned snapshots | [Evaluator](engine/evaluator/overview.md) |
| Strategy Runner | Tenant/Workflow component | Deterministically joins feed, evaluation, and portfolio inputs | [Strategy Runner](engine/strategy-runner/overview.md) |
| Order | Tenant/Workflow component | Owns checks, reservations, venue submission, reconciliation, and order facts | [Order](engine/order-manager/overview.md) |
| Wallet | Tenant/Workflow component | Owns reservations, immutable ledger, balances, positions, and portfolio snapshots | [Wallet](engine/wallet-manager/overview.md) |
| Backtest | Tenant/Workflow component | Reuses the decision contracts with virtual time and simulated exchange | [Backtest](engine/backtest/overview.md) |
| Notifier | Tenant/Workflow component | Idempotently delivers Email, Telegram, and in-product notifications | [Notifier](engine/notifier/overview.md) |
| Exporter | Tenant/Workflow component | Cursor-based, batched export with retries and backpressure | [Exporter](engine/exporter/overview.md) |
| Logger | Tenant/Workflow component | Durably buffers and archives Python/Rust logs outside the decision path | [Logger](engine/logger/overview.md) |

Tenant, user, identity, membership, role, permission, policy, and logical
namespace semantics have one canonical owner:
[Tenant Governance & IAM](engine/apiserver/tenant-governance-and-iam.md).

## Embedded Components and Protocol Adapters

These components are composed into services; they are not independently
deployed Workflow nodes.

| Component | Responsibility | Design |
|---|---|---|
| Exchange | Unified async trading, account, market-data, and stream capabilities for venues such as Binance, IBKR, Polymarket, and the Backtest venue | [Exchange](engine/exchange/overview.md) |
| Messager | Transport-neutral async event SDK: local queue in all-in-one mode and MQTT/EMQX in distributed mode | [Messager](engine/messager/overview.md) |
| A2A | Optional external agent interoperability; it cannot bypass pipeline policy or place orders directly | [A2A](engine/a2a/overview.md) |
| MCP | Optional tool protocol for API exposure and evaluator context; it is not the canonical live market feed | [MCP](engine/mcp/overview.md) |

## Naming and Extension Rules

- Public service roots and startup types use a domain-specific `Sigbot...`
  name; the exact exported symbol is listed below.
- Existing replaceable ports use concise `I<Capability>` names. New ports
  follow the owning module's established convention instead of inventing a
  parallel naming scheme.
- Concrete adapters carry their provider and capability in a stable name.
- `Provider` denotes a factory-selected implementation family. Current delivery
  media use `NotificationProviderConfig`; export destinations use
  `ExportSinkConfig`.
- Rust acronyms use PascalCase words: `Api`, `Http`, `Mqtt`, `Qq`, `Ibkr`.
- Interfaces belong at actual variation boundaries; avoid a one-to-one trait
  that only mirrors a startup shell.

| Module | Service root | Primary extension ports |
|---|---|---|
| APIServer | `SigbotHttpStarter` | Core handlers and repositories exposed through HTTP/A2A/MCP adapters |
| Controller | `SigbotControllerStarter` | `WorkerProvider` |
| Datafeed | `SigbotDatafeedIngestor` | `IDatafeed` |
| Backtest | `SigbotBacktestServer` | `ReplaySource`, `ReplayDecision`, `IExchange` |
| Evaluator | `SigbotEvaluatorRunner` | `IEvaluator` |
| Strategy Runner | `SigbotStrategyRunner` | `IStrategy` |
| Order | `SigbotOrderServer` | `IOrder`, execution adapters |
| Wallet | `SigbotWalletServer` | `IWallet` |
| Notifier | `SigbotNotifier` | `INotifier`, `NotificationProviderConfig` |
| Exporter | `SigbotExporterServer` | `IExporter`, `ExportSinkConfig` |
| Exchange SDK | Composed, no daemon | `IExchange`, capability-specific subports |
| Messager SDK | Composed, no daemon | `IMessager` |

## Data Ownership and Consistency

- API is the only writer for user-driven control-plane configuration. The
  independent Controller reads immutable Workflow/Worker configuration and is
  the only writer for run claims, selected workers, attempts, and observed
  lifecycle state. Both use the same Core handlers/repositories; data-plane
  components do not read control-plane configuration tables directly.
- Order and Wallet have separate prefixed table domains and exclusive Repository
  write ownership. The current final-state migration places both domains in one
  physical PostgreSQL schema; sharing a schema does not permit cross-domain SQL
  or a second writer.
- Order persists order/execution facts and a transactional outbox atomically.
  Wallet applies each monetary execution fact through an inbox and updates its
  ledger, balance, position, and outbox in one local transaction.
- Cross-service state is recoverable eventual consistency with idempotency,
  replay, and exchange reconciliation. An external venue cannot participate in
  a PostgreSQL transaction.
- If order and portfolio state truly require one ACID transaction, they must be
  one physical bounded-context service. Two daemons sharing tables is not a
  strong-consistency design.
- Specialized market-history, tenant middleware, and object storage remain
  private to their owning component even when an operator co-locates physical
  database infrastructure.

## Messaging Invariants

- A published topic never contains MQTT's `$share` prefix. `$share` is a
  subscription filter concern.
- Evaluator and Strategy Runner use different consumer groups for the same
  feed. Replicas of one component share a group; different component roles do
  not, otherwise they would divide events instead of each receiving them.
- Business-event ownership follows the producer: Datafeed owns feed events,
  Evaluator owns evaluation snapshots, Strategy Runner owns trade intents, Order owns
  execution facts, and Wallet owns portfolio snapshots. Messager owns only the
  envelope and delivery semantics.
- Every envelope carries event ID, tenant ID, Workflow ID, producer, schema
  version, event time, publication time, `run_id`, seq, and attempt_no. No
  separate correlation ID is permitted.
- W3C `traceparent` carries trace context. For asynchronous requests,
  `requestSpanId` equals the producer span_id and is echoed by the response; it
  is short-lived QoS pairing state and is never persisted.
- Consumers assume duplicate and delayed delivery, establish an idempotency
  boundary, and define ordering/partition keys explicitly.
- MQTT/EMQX plus PostgreSQL Inbox/Outbox is the durable distributed runtime.
  Local transport is a single-process compatibility adapter and must not be
  presented as equivalent recovery or persistent fan-out.

## Global Safety Invariants

- AI output is advisory, bounded by a strategy-declared schema, versioned, and
  auditable. It cannot call private exchange operations.
- Strategy Runner cannot call an exchange. Order Manager is the only service
  allowed to invoke private trading capabilities and must repeat hard risk and
  account checks.
- A placement acknowledgement is not a fill. Wallet changes money and position
  only from authoritative fill, settlement, fee, funding, transfer, or
  liquidation facts.
- No component reads another component's private tables.
- Credentials and private keys stay within their owning security boundary and
  never appear in events, logs, or unredacted responses.
- Backtests prevent look-ahead: an evaluation or portfolio snapshot may only be
  used after its original publication/effective time.

## Implementation Alignment

| Area | Implemented boundary | Remaining evidence or product gap |
|---|---|---|
| API and IAM | HTTP CRUD plus MCP/A2A outlets use explicit Authguard request identity and Core PostgreSQL/SQLite repositories | Real Hosted Login, production domains/TLS, and an authenticated browser save/start/trace flow still need external credentials |
| Controller | Independently reconciles nine component kinds and typed Redis/pgvector/TimescaleDB/EMQX resources on Docker or Kubernetes | The latest four-middleware reconciler has manifest/compile tests but has not completed a fresh end-to-end real-cluster lifecycle run |
| Datafeed | REST/MCP/A2A social ingestion, policy RSS, persisted history, LATEST/AS_OF snapshots, and Binance/Hyperliquid public market reads exist | Full liquidation heatmaps, lending/funding/position-ratio coverage and additional venues remain incomplete |
| Evaluator | Consumes feed events, calls the bounded LLM path, separates facts/inference, stores cases, and publishes versioned five-state snapshots | Real-model quality/calibration and production pgvector memory quality have not been accepted |
| Strategy Runner | Transactional state drafts, serial callbacks, typed Effects, market/compute/service results, sandboxing, checkpoints, and intent-only trading output are implemented | Redis hot caching and general distributed incremental/model sharding are not implemented |
| Order and Wallet | Reservation-first ordering, hard risk, prepared submissions, unknown-result reconciliation, immutable executions, balanced ledger/projections, and transactional Inbox/Outbox are implemented | Real-money venue settlement, long-running reconciliation, corporate actions/margin, and genuine cross-venue compensation remain unaccepted |
| Backtest | Frozen history, virtual time, replay barriers, deterministic matching/fees, resting limit orders, and isolated replay are implemented | Depth queues, realistic partial liquidity/latency, and all model-shard equivalence are outside the current simulator |
| Notifier and Exporter | In-app/Email/Telegram delivery state and Kafka/Google Sheets sinks have typed requests, checkpoints, retries, and idempotency boundaries | Real SMTP/Telegram/Sheets credentials and Kafka multi-broker failover have not been accepted |
| Logger | SQLite WAL durable ACK, immutable conditional S3 chunks, hash verification, PostgreSQL cursors, and Python/Rust capture are implemented | Host-volume loss, production bucket retention/replication, and capture from every Rust host remain open |
| Messager | MQTT QoS 1 persistent sessions, manual ACK, scoped topics, Inbox/Outbox, replay, backpressure, and temporary reply pairing are implemented | Broker-disk disaster and sustained-load tests remain open; Local transport is intentionally not durability-equivalent |

The component documents own their detailed contracts and capability status.
The coding-agent handoff records the exact verification baseline and next work:
[`docs/development/HARDCOVER.md`](../development/HARDCOVER.md).
