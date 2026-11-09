# API Services

[中文](overview_ZH.md) · [Domain schema and contracts](../../domain-model_ZH.md)

HTTP and A2A expose the same ten tenant-scoped domains: tenant, cluster, datasource, exchange, strategy, order, ledger, notifier, exporter and workflow. Protocol adapters reuse ResourceHandler and IGenericRepository for authorization, validation and persistence.

HTTP has five CRUD operations per domain. Each entity module lives at src/api/http/src/<domain>/<entity>.rs; the Axum router and IHttpApi contract are consolidated in src/api/http/src/http.rs. Swagger assets and implementation remain under src/swagger.

All requests carry an explicit environment; tenant identity and grants come only from verified Authguard context. The tenant domain owns Sigbot business metadata, while IAM ownership remains with Authguard. Cluster connections reference their business tenant; credentials remain Secret references.

Workflow DAGs are stored atomically as dag_config JSON. The CRUD boundary cannot overwrite running workflows, active cluster configuration, submitted orders or immutable financial facts. Ledger queries include balances and recent immutable entries.

Start the REST process with sigbot api --config etc/sigbot.yaml. Its starter is src/cmd/src/startup/api/http.rs (SigbotHttpStarter). The A2A starter is adjacent; the optional aggregate process lives at startup/standalone.rs (SigbotStandaloneStarter). All use the shared provider context and graceful lifecycle. MCP is generated independently from HTTP OpenAPI with [mcpfather](../mcp/overview.md).
