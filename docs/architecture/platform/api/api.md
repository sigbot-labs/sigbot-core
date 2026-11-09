# API Services

[中文](api_CN.md) · [Domain schema and contracts](#domain-contracts)

HTTP and A2A expose the same ten tenant-scoped domains: tenant, cluster, datasource, exchange, strategy, order, ledger, notifier, exporter and workflow. Protocol adapters reuse ResourceHandler and IGenericRepository for authorization, validation and persistence.

HTTP has five CRUD operations per domain. Each entity module lives at src/api/http/src/<domain>/<entity>.rs; the Axum router and IHttpApi contract are consolidated in src/api/http/src/http.rs. Swagger assets and implementation remain under src/api/http/src/swagger.

All requests carry an explicit environment; tenant identity and grants come only from verified Authguard context. The tenant domain owns Sigbot business metadata, while IAM ownership remains with Authguard. Cluster connections reference their business tenant; credentials remain Secret references.

Workflow DAGs are stored atomically as dag_config JSON. The CRUD boundary cannot overwrite running workflows, active cluster configuration, submitted orders or immutable financial facts. Ledger queries include balances and recent immutable entries.

Start the REST process with sigbot api --config etc/sigbot.yaml. Its starter is src/cmd/src/startup/api/http.rs (SigbotHttpStarter). The A2A starter is adjacent; the optional aggregate process lives at startup/standalone.rs (SigbotStandaloneStarter). All use the shared provider context and graceful lifecycle. MCP is generated independently from HTTP OpenAPI with [mcpfather](api.md#mcp).

<a id="domain-contracts"></a>

## Ten-domain configuration and CRUD

The first part of the database baseline defines ten domain root tables; the
remainder implements execution, recovery and audit storage. HTTP and A2A share
Core's ResourceHandler and IGenericRepository. Protocol adapters never write SQL.

| Domain     | Root table        | Content                                                                                                              |
| ---------- | ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| tenant     | `tenant_info`     | Business metadata; ID is the Authguard-verified tenant UUID                                                          |
| cluster    | `cluster_info`    | Tenant-owned Docker/Kubernetes connection, Secret references, resources and middleware; multiple clusters per tenant |
| datasource | `datasource_info` | Twitter, TruthSocial, custom HTTP/MCP/A2A connections                                                                |
| exchange   | `exchange_info`   | Trading connections such as Binance, OKX, IBKR and Polymarket                                                        |
| strategy   | `strategy_info`   | Python code, default_parameters and configuration                                                                    |
| order      | `order_info`      | Execution intent, order state and venue order identifiers                                                            |
| ledger     | `ledger_info`     | Account, execution mode and wallet ownership; queries include balances and the latest 100 entries                    |
| notifier   | `notifier_info`   | Notification connections; channel details in configuration                                                           |
| exporter   | `exporter_info`   | Kafka, Google Sheets and other downstream connections                                                                |
| workflow   | `workflow_info`   | Complete graph in dag_config JSON, without nodes/edges child tables                                                  |

`tenant_info` owns business metadata, never login, users, roles, permissions or
IAM tenant lifecycle. `cluster_info.tenant_id` and connection/strategy root
tables reference this business row. Account/order composite keys continue to
enforce tenant, environment and execution idempotency.

Each configuration entity occupies one row: `configuration` holds nonsensitive
provider parameters, `secret_ref` holds a central Secret reference. Strategy
`code` and `default_parameters` distinguish the artifact from initial parameters.
An order's UUIDv7 ID serves CRUD; existing composite keys retain execution
idempotency.

Collections are `tenants`, `clusters`, `datasources`, `exchanges`, `strategies`,
`orders`, `ledgers`, `notifiers`, `exporters` and `workflows`. Each exposes:

```text
GET    /api/v1/<collection>?env=dev&limit=50&offset=0
POST   /api/v1/<collection>?env=dev
GET    /api/v1/<collection>/<id>?env=dev
PUT    /api/v1/<collection>/<id>?env=dev
DELETE /api/v1/<collection>/<id>?env=dev
```

POST/PUT bodies contain entity fields. The server owns `id`, `tenant_id`, `env`,
audit fields and portfolio version. Create returns an ID, list returns a `data`
array and get returns a `data` object. Unknown fields and unauthorized resources
are rejected; list limits are 1..200. External [MCP](#mcp) tools come from the
actual HTTP OpenAPI, with no second built-in registry.

A2A DataPart or JSON TextPart uses the same command:

```json
{
  "domain": "strategy",
  "operation": "create",
  "env": "dev",
  "fields": {
    "name": "sample",
    "code": "class Strategy: pass",
    "default_parameters": { "window": 20 }
  }
}
```

Both built-in protocols preserve the Authguard-verified principal, resource URN
and allow/deny grants; callers cannot override them in the body. Generated MCP
upstream requests pass through the same authorization.

Only pending orders without prepared submissions or executions are editable.
Account configuration becomes immutable once financial portfolio state exists;
balances and executions are queried as component-owned projections/facts.
Committed workflows, active clusters and immutable financial facts cannot be
overwritten by configuration CRUD. Foreign keys still constrain deletion.

The old `cfg_connection` and HTTP `connections/workers/knowledge/market/notifications/overview`
endpoints are removed. Public operations are the ten-domain CRUD boundary;
Controller and components retain their internal execution logic. Internal tables
and sole writers are cataloged in the [overview](../../overview.md#runtime-storage).
Baseline application and dedicated database checks are in [Build and test](../../../development/build-and-test.md#database-migrations).

<a id="iam"></a>

## Identity and tenant governance

### Single ownership

Authguard is the sole writer for identities, users, tenants, memberships, roles,
permissions, sessions, login, and authorization policy. Sigbot does not persist
IAM mirror tables, expose local user CRUD, or maintain a process-global “current
user.” Migrations therefore must not contain local IAM mirror tables.

Sigbot persists only quantitative-trading resources. `tenant_id` is the UUIDv7
scope issued by Authguard and carried by the authenticated request; it is not an identity issued by Sigbot. Business tables may reference
`tenant_info`, whose ID is that same verified UUID; this is not an IAM mirror. Workflows, connections, workers,
runs, wallets, and message topics carry that scope explicitly. There is no
second workspace or namespace business identifier.

| Concern                                        | Sole owner               | How Sigbot consumes it                                           |
| ---------------------------------------------- | ------------------------ | ---------------------------------------------------------------- |
| Login, sessions, users, and tenant lifecycle   | Authguard                | Reads signed identity after Gateway authentication               |
| Memberships, roles, permissions, and policy    | Authguard                | Authorizes through the official Rust adapter at the API boundary |
| Workflow, connection, and worker configuration | Sigbot API/Core          | CRUD partitioned by the authorized `tenant_id`                   |
| Lifecycle of the nine data-plane components    | Sigbot Controller        | Reconciles Tenant/Workflow/Worker scopes                         |
| Orders, ledgers, market data, and logs         | The owning Sigbot domain | Never creates or updates IAM data                                |

### Request boundary

1. Envoy Gateway delegates authentication and authorization to Authguard.
2. An API adapter verifies the Authguard-signed identity and never trusts a
   tenant supplied by the caller.
3. A handler obtains `tenant_id` from an explicit `RequestIdentity` and checks
   the target resource scope again.
4. Repository reads and writes always include tenant scope. A global user
   context or implicit default tenant is forbidden.
5. Controller accepts only persisted, authorized workflow and worker
   configuration; pod environment values cannot elevate permissions.

HTTP and A2A share the same handlers and authorization rules. External MCP is generated by mcpfather and calls the same authorized HTTP APIs. A2A exposes implemented domain operations and reuses Core authorization and persistence; it does not bypass Authguard.

### Data and deployment isolation

- Control-plane PostgreSQL stores Sigbot business configuration, runtime state,
  and facts, but no Authguard IAM tables.
- Control-plane Redis caches Sigbot configuration and short-lived coordination
  state. Authguard owns its IAM cache.
- Controller selects a remote Docker or Kubernetes worker by `tenant_id` and
  optional `workflow_id`, configures Redis, pgvector, TimescaleDB, and EMQX, and
  starts the nine tenant components.
- MQTT starts at `internal/v2/{env}/{tenant_id}` and is further isolated by
  workflow or account.
- Secrets enter only through bootstrap or an external Secret provider, never
  through workflow snapshots, database payload values, Helm values, or logs. Database Secret key references are permitted.

### Invariants

- Authguard is the sole IAM authority; Sigbot has no compatibility IAM path.
- `tenant_id` is UUIDv7 and remains identical across adapters, handlers,
  repositories, topics, and deployment labels.
- Unauthenticated, invalidly signed, cross-tenant, or unauthorized access is
  rejected before a business write transaction starts.
- `run_id` is the public business idempotency and correlation identity; Trace
  Context is only observability propagation.
- Sigbot does not infer IAM tenant deletion. Business resource cleanup requires
  an explicit, authorized, auditable platform workflow.

### Source map

- Authguard adapter and request identity: `src/core/src/context/`
- HTTP API: `src/api/http/`
- A2A API: `src/api/a2a/`; external MCP: [mcpfather integration](api.md#mcp)
- Business handlers and repositories: `src/core/src/modules/`
- Worker and tenant-component orchestration: `src/controller/`
- Helm integration: `deploy/helm/sigbot/charts/middleware/`

### Verified request context

Authguard owns authentication, accounts, sessions and grants. Sigbot retains business tenant bindings and accepts only the official SDK's verified, bounded-lifetime request context. The integration pins `authguard-adapter-rust` to commit `56f00ee51a33136dae7e83beba558cdbf05c4a85`. `HeaderAccessContextResolver` verifies the Authguard HMAC signature and expiry; `GrpcAccessContextResolver` resolves opaque scope tokens. Sigbot does not call the SDK's thread-local context functions. Each HTTP request carries a `RequestIdentity` in Axum extensions, and each business handler receives that identity explicitly.

`authguard` is the typed Sigbot runtime configuration. `provider` is `signed_header` or `grpc`; `service`, `partition`, `region`, `read_action` and `write_action` bind the signed context to the API request. The defaults expect `urn:iam:default:sigbot:global:<tenant>:<request-path-without-leading-slash>` and `sigbot:read` for GET/HEAD, `sigbot:write` for other methods. Configure matching Authguard route policies. A supplied `x-sigbot-tenant-id` must match the signed tenant. Deny grants override allows. Trust cannot be established by supplying an unsigned identity header.

Pass `AUTHGUARD__AUTHZ__SCOPE_DELIVERY__DIRECT_CONTEXT_HMAC_KEY` through a Secret shared with Authguard AuthZ for signed-header delivery. For gRPC use the SDK's `AUTHGUARD_GRPC_TARGET` and `AUTHGUARD_GRPC_TLS` settings and restrict backend network access to the Gateway/workloads. The API refuses to bind without a valid verifier configuration. Worker-only states install a deny-all provider when no verifier is configured, so they do not need Gateway signing keys. No secret values are stored in the chart. Business tenant key custody additionally requires `SIGBOT_SYSTEM_MASTER_KEY`; the previous default master key and JWT-secret fallback are removed.

Domain grants additionally check `environment/<env>/<domain>/<id-or-all>`; create checks `new`, and deny patterns override allows. A valid session alone does not authorize every resource. Legacy overview/market/connection/notification grants do not restore the removed aggregate HTTP endpoints.

A Workflow start transaction freezes the graph into `wf_run` and marks it pending for Controller; repeated start reuses the active run. Acceptance is neither deployment completion nor a trading acknowledgement. Raw unscoped reads/deletions fail closed; worker service identity explicitly supplies tenant, environment and principal, without a default all-tenant scan. These are internal lifecycle contracts, not additional routes in the ten-domain public CRUD API.

Request-identity checks and Hosted Login deployment procedures are in the [Kubernetes runbook](../../../runbooks/deployment.md#authguard).

<a id="mcp"></a>

## MCP through mcpfather

Sigbot maintains its HTTP/A2A APIs. MCP servers are generated and deployed independently with mcpfather from Sigbot's HTTP OpenAPI document. This is the real Sigbot integration case for mcpfather; the Rust workspace no longer includes an MCP server or a sigbot mcp command.

The former implementation and its latest uncommitted Starter changes are preserved under .archived/20261109382032_builtin_mcp_server_replaced_by_mcpfather/, including protocol/authentication code, tests, documentation and integration snapshots. SHA256SUMS verifies the moved files. The archive follows the repository's existing .gitignore and is local unless deliberately versioned later.

### Generate the external server

1. Start Sigbot HTTP with sigbot api --config etc/sigbot.yaml.
2. Obtain its actual OpenAPI JSON through an authorized HTTP endpoint. The shipped YAML uses swagger.openapi_url=/swagger-ui/openapi.json; account for server.context_path and the Gateway route/policy. Save the document as sigbot.openapi.json. Do not maintain a second handwritten API schema.
3. Use the adjacent project's installed generator (build it there first if needed):

```sh
MCPFATHER_ROOT=/Users/agent/Documents/mcpfather
"${MCPFATHER_ROOT}/bin/mcpfather" \
  --input /path/to/sigbot.openapi.json \
  --output /path/to/generated/sigbot-mcp

make -C /path/to/generated/sigbot-mcp
export MCP__UPSTREAM__DEFAULT__ENDPOINT="${SIGBOT_HTTP_ORIGIN:?set the real Sigbot upstream origin}"
/path/to/generated/sigbot-mcp/bin/sigbot-mcp --transport http --port 8080

# Inspect the generated names before invoking a tool.
/path/to/generated/sigbot-mcp/bin/sigbot-mcp --transport cli list
```

The local generator's normal mode is used for this OpenAPI 3.1 case. Its optional --validation currently rejects the valid JSON Schema propertyNames keyword in ResourceCommand, so strict-validator compatibility is not claimed; the source schema is not rewritten to hide that keyword.

All 50 ten-domain CRUD operations have unique operationId values in the maintained HTTP schema. mcpfather owns generated tool names, MCP transport, OAuth discovery, inbound authentication and deployment. Keep generated source in its independent project, outside the Sigbot Rust workspace.

### Authentication and verification

Configure mcpfather's inbound OIDC and its separate outbound upstream credentials using that project's configuration. The AI client's token is not blindly forwarded upstream. Send upstream calls through the Authguard/Gateway policy for the appropriate tenant and service principal; retain environment/domain grants and W3C trace context. Never put privileged credentials in the OpenAPI document or loosen Sigbot tenant checks for the generated server.

The opt-in HTTP schema integration test exports the actual SwaggerHttpApi document to a temporary directory and invokes the local generator:

```sh
MCPFATHER_BIN=/Users/agent/Documents/mcpfather/bin/mcpfather \
  cargo test -p sigbot-http --test swagger mcpfather_generates_from_sigbot_openapi -- --ignored
```

This verifies schema/generator interoperability, not a live OIDC login or trading execution. Datafeed's outbound MCP client and external-source fixtures have independent purposes and remain active.
