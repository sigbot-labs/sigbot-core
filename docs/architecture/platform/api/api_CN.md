# API 服务

[English](api.md) · [十领域表和协议契约](api_CN.md#domain-contracts)

HTTP、A2A 对齐 tenant、cluster、datasource、exchange、strategy、order、ledger、notifier、exporter、workflow 十个领域的 CRUD。两个内置协议复用 Core ResourceHandler 和 IGenericRepository 的权限、校验和持久化逻辑。

HTTP 每个领域提供 list/get/create/update/delete；实体入口位于 src/api/http/src/<domain>/<entity>.rs。IHttpApi 和 Axum 实现合并在 src/api/http/src/http.rs；Swagger 资源和实现继续位于 src/api/http/src/swagger。

所有操作显式传 env；tenant、principal 和 allow/deny grants 仅来自 Authguard 已验证上下文。tenant_info 保存 Sigbot 业务资料，不复制 IAM。Cluster 通过外键归属 tenant，认证配置使用 Secret 引用。

Workflow 的 nodes/edges 整体保存在 dag_config JSON。对运行中 Workflow、活跃 Cluster、已提交订单和不可变财务事实的修改受到约束。Ledger 查询包含余额与最近流水。

REST 入口为 src/cmd/src/startup/api/http.rs 的 SigbotHttpStarter；A2A 在相邻文件。聚合入口为 startup/standalone.rs 的 SigbotStandaloneStarter。所有进程使用同一 Provider 生命周期架构。MCP 改为通过 [mcpfather](api_CN.md#mcp) 从 HTTP OpenAPI 独立生成。

<a id="domain-contracts"></a>

## 十领域配置与 CRUD

数据库基线的前半部分只定义十个领域主表，后半部分定义运行时执行、恢复和审计存储。HTTP、A2A 共用 Core 的 ResourceHandler 和 IGenericRepository；协议层不写 SQL。

| 领域       | 主表            | 主要内容                                                                                  |
| ---------- | --------------- | ----------------------------------------------------------------------------------------- |
| tenant     | tenant_info     | Sigbot 业务租户资料；id 必须是 Authguard 已验证的 tenant UUID                             |
| cluster    | cluster_info    | 租户独享 Docker/Kubernetes 连接、认证 Secret 引用、资源和中间件；一个租户可有多个 Cluster |
| datasource | datasource_info | Twitter、TruthSocial、自定义 HTTP/MCP/A2A 等连接                                          |
| exchange   | exchange_info   | Binance、OKX、IBKR、Polymarket 等交易连接                                                 |
| strategy   | strategy_info   | Python code、default_parameters 和配置                                                    |
| order      | order_info      | 仓位执行意图、订单状态和交易所订单标识                                                    |
| ledger     | ledger_info     | 账户、执行模式与钱包归属；查询同时返回 balances 和最近 100 条 entries                     |
| notifier   | notifier_info   | 各种通知渠道连接；渠道细节放 configuration                                                |
| exporter   | exporter_info   | Kafka、Google Sheets 等外部推送连接                                                       |
| workflow   | workflow_info   | DAG 整体保存在 dag_config JSON 中，不额外建 nodes/edges 子表                              |

tenant_info 是业务资料，不负责登录、用户、角色、权限或 IAM 租户生命周期。cluster_info.tenant_id 有外键，连接和策略主表同样引用 tenant_info。账户和订单仍由复合键约束租户、环境及幂等坐标。

每个配置实体一行，configuration 保存对应 provider 的非敏感参数，secret_ref 保存 central Secret 引用。strategy_info 用 code 和 default_parameters 明确区分可执行代码与初始参数。订单的 UUIDv7 id 用于 CRUD，已有复合主键继续负责执行幂等。

### 内置协议与外部 MCP

HTTP 每个领域提供以下五种操作，集合名分别为 tenants、clusters、datasources、exchanges、strategies、orders、ledgers、notifiers、exporters、workflows：

- GET /api/v1/<collection>?env=dev&limit=50&offset=0
- POST /api/v1/<collection>?env=dev
- GET /api/v1/<collection>/<id>?env=dev
- PUT /api/v1/<collection>/<id>?env=dev
- DELETE /api/v1/<collection>/<id>?env=dev

POST/PUT body 是实体字段对象。id、tenant_id、env、审计字段、组合版本由服务端管理。创建返回 id，列表返回 data 数组，详情返回 data 对象。未知字段和无权限资源会被拒绝；limit 范围 1..200。

MCP 由独立的 [mcpfather](api_CN.md#mcp) 直接从 HTTP OpenAPI 生成，tool 名称/参数以生成结果为准；Sigbot 不再维护第二份内置 MCP registry。

A2A DataPart 或 JSON TextPart 使用同一命令，例如：

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

内置 HTTP/A2A 均保留 Authguard 验证后的 principal、resource URN 和 allow/deny grants；外部 MCP 的上游 HTTP 调用仍须经过这些鉴权规则。调用者不能通过 body 覆盖它们。

### 存储与写入限制

订单只可编辑 pending 且尚未形成 prepared_submission/成交的记录。账户配置只可在尚未形成组合财务状态时编辑；余额和成交流水作为组件拥有的事实/投影查询。已提交 Workflow、活跃 Cluster、财务事实不能经配置 CRUD 任意覆盖。删除仍受外键约束。

旧 cfg_connection 和 HTTP 的 connections/workers/knowledge/market/notifications/overview 入口已移除。当前对外操作范围收敛为十领域 CRUD；运行时 Controller/组件仍保留其内部执行逻辑。

内部运行时表和唯一写入方见[总览](../../overview_CN.md#runtime-storage)，基线应用与专用数据库测试见[构建与测试](../../../development/build-and-test.md#database-migrations)。

<a id="iam"></a>

## 身份与租户治理

### 唯一归属

Authguard 是平台身份、用户、Tenant、Membership、Role、Permission、Session、登录和
授权策略的唯一写入方。Sigbot 不保存 IAM 镜像表，不提供本地用户 CRUD，也不维护
进程级“当前用户”。因此迁移中禁止出现任何本地 IAM 镜像表。

Sigbot 只保存量化业务资源。`tenant_id` 是 Authguard 签发并由请求身份携带的 UUIDv7
作用域，不是 Sigbot 自行签发的 IAM 身份。业务表可引用同 UUID 的
`tenant_info` 资料行，这不构成 IAM 镜像。Workflow、Connection、Worker、Run、Wallet
和消息主题必须显式携带该作用域；系统中不存在第二个工作区或 Namespace 业务 ID。

| 关注点                                  | 唯一归属                   | Sigbot 消费方式                      |
| --------------------------------------- | -------------------------- | ------------------------------------ |
| 登录、Session、用户和 Tenant 生命周期   | Authguard                  | 经 Gateway 认证后读取签名身份        |
| Membership、Role、Permission 和策略判定 | Authguard                  | API 边界调用官方 Rust Adapter 授权   |
| Workflow、Connection、Worker 配置       | Sigbot API/Core            | 以授权后的 `tenant_id` 分区 CRUD     |
| 九个数据面组件生命周期                  | Sigbot Controller          | 以 Tenant/Workflow/Worker 作用域编排 |
| 订单、账本、行情、日志等业务事实        | 对应 Sigbot 领域唯一写入方 | 不反向创建或更新 IAM 数据            |

### 请求边界

1. Envoy Gateway 交给 Authguard 完成认证和授权。
2. API Adapter 校验 Authguard 签名身份，不接受客户端自报 Tenant。
3. Handler 从显式 `RequestIdentity` 取得 `tenant_id`，并对目标资源再次做作用域校验。
4. Repository 的查询和写入条件始终包含 Tenant；禁止使用全局用户上下文或隐式默认租户。
5. Controller 只接受已经持久化并授权的 Workflow/Worker 配置，不能通过 Pod 环境变量扩权。

HTTP、A2A 共用同一 Handler 与授权规则；mcpfather 生成的外部 MCP 通过同一 HTTP 鉴权边界接入。A2A 复用十领域命令及 Core 授权/持久化，不另建权限或 CRUD 实现，也不得绕过 Authguard。

### 数据与部署隔离

- 控制面 Postgres 保存 Sigbot 业务配置、运行状态和事实，不保存 Authguard IAM 表。
- 控制面 Redis 只缓存 Sigbot 配置和短期协调数据；IAM 缓存由 Authguard 自己管理。
- Controller 按 `tenant_id` 和可选 `workflow_id` 选择远程 Docker/Kubernetes Worker，动态
  配置 Redis、pgvector、TimescaleDB 与 EMQX，并启动九个租户级组件。
- MQTT 基础主题是 `internal/v2/{env}/{tenant_id}`，Workflow 与 Account 继续在其下隔离。
- Secret 值只能由 Bootstrap 或外部 Secret Provider 注入，不进入 Workflow 快照、数据库正文、
  Helm values 或日志；数据库只允许保存 Secret key 引用。

### 不变量

- Authguard 是 IAM 唯一权威；Sigbot 不实现兼容 IAM 路径。
- `tenant_id` 必须为 UUIDv7，并在入口、Handler、Repository、Topic 和部署标签中一致。
- 未认证、签名无效、跨租户或权限不足的访问在进入业务写事务前拒绝。
- `run_id` 是公开业务幂等与关联标识；Trace Context 只负责可观测传播。
- 删除 Tenant 的 IAM 生命周期不由 Sigbot 猜测；业务资源清理必须由显式、授权且可审计的
  平台流程触发。

### 源码地图

- Authguard 适配与请求身份：`src/core/src/context/`
- HTTP API：`src/api/http/`
- A2A API：`src/api/a2a/`；外部 MCP：[mcpfather 接入](api_CN.md#mcp)
- 业务 Handler/Repository：`src/core/src/modules/`
- Worker 与租户组件编排：`src/controller/`
- Helm 集成：`deploy/helm/sigbot/charts/middleware/`

### 已验证的请求上下文

Sigbot 只接受官方 SDK 验证且有时效的身份。适配固定到 `authguard-adapter-rust`
提交 `56f00ee51a33136dae7e83beba558cdbf05c4a85`；`HeaderAccessContextResolver`
验证 HMAC 和有效期，`GrpcAccessContextResolver` 解析不透明 Scope Token。每次请求的
`RequestIdentity` 放在 Axum Extensions 并显式传给 Handler，不调用 SDK 的 thread-local API。

强类型 `authguard.provider` 为 `signed_header` 或 `grpc`；`service`、`partition`、
`region`、`read_action`、`write_action` 将身份绑定到具体请求。默认资源为
`urn:iam:default:sigbot:global:<tenant>:<去掉前导斜杠的请求路径>`，GET/HEAD 使用
`sigbot:read`，其余方法使用 `sigbot:write`；Gateway Policy 必须匹配。
`x-sigbot-tenant-id` 如存在必须等于签名租户；Deny 优先，未签名 Header 不能建立信任。

Signed Header 通过 Secret 与 Authguard AuthZ 共享
`AUTHGUARD__AUTHZ__SCOPE_DELIVERY__DIRECT_CONTEXT_HMAC_KEY`；gRPC 使用 SDK 的
`AUTHGUARD_GRPC_TARGET`、`AUTHGUARD_GRPC_TLS`，后端网络仅允许 Gateway/工作负载。
API 没有有效 Verifier 配置就拒绝监听；纯 Worker 无 Verifier 时安装 deny-all Provider，
不需要 Gateway 签名密钥。业务租户密钥托管另需 `SIGBOT_SYSTEM_MASTER_KEY`，无默认密钥或
JWT Secret 回退，Chart 不保存明文值。

领域授权另校验 `environment/<env>/<domain>/<id-or-all>`，创建校验 `new`，Deny 优先。
有效 Session 不表示能访问全部资源；旧 overview/market/connection/notification Grants
不会恢复已移除的聚合 HTTP 入口。

Workflow 启动事务将图冻结到 `wf_run` 并置为 Controller 待处理状态；重复启动复用活跃 Run。
接受请求不代表部署完成或成交。无作用域查询/删除必须拒绝；Worker 显式携带服务租户、
环境和 Principal，缺身份不能扫描所有租户。这是内部生命周期契约，不是十领域 CRUD
以外的新增公开路由。请求身份检查和 Hosted Login 部署见 [Kubernetes 手册](../../../runbooks/deployment.md#authguard)。

<a id="mcp"></a>

## 通过 mcpfather 暴露 MCP

Sigbot 维护 HTTP/A2A API，由独立部署的 mcpfather 生成服务把 HTTP OpenAPI 转换为 MCP。这是 Sigbot 对 mcpfather 的真实接入案例；活动 Rust workspace 不再包含 MCP server，也不再注册 sigbot mcp 子命令。

旧实现、当前未提交的 MCP Starter 修改、认证适配器、测试、文档和集成快照保存在 .archived/20261109382032_builtin_mcp_server_replaced_by_mcpfather/。目录内 SHA256SUMS 可核验搬移前后的文件一致性。沿用现有 .gitignore，归档当前仅保存在本地。

生成时，先通过获授权的 HTTP 入口取得 Sigbot 实际的 OpenAPI JSON。示例配置路径为 /swagger-ui/openapi.json，须结合 server.context_path 和 Gateway 的路由/权限；不要另写一份 API schema。然后调用相邻项目的 bin/mcpfather，以 --input 指定 schema、--output 指定独立的 sigbot-mcp 项目，使用生成器默认模式。当前本地生成器的可选 --validation 严格校验不接受 OpenAPI 3.1 中合法的 propertyNames；这是校验器兼容性限制，不改写 schema 掩盖该字段，也不宣称严格校验通过。完整命令见[英文生成步骤](api.md#mcp)。

Sigbot schema 的十领域 50 个 CRUD 操作各自有 operationId；生成器负责 MCP tools、传输、OAuth 发现、认证和部署。生成代码放独立项目，不再塞回 Sigbot workspace。

分别配置 mcpfather 的前端 OIDC 与后端上游凭据，通过 Authguard/Gateway 以对应租户和服务主体调用 Sigbot。保留 env/domain grants 和 Trace Context，不透传 AI 客户端令牌，也不在 schema 中放机密值或绕过租户隔离。

HTTP crate 的可选集成测试会导出真实 SwaggerHttpApi 文档并调用本地 mcpfather 生成器；运行方式见[英文验证步骤](api.md#mcp)。该验证不等同于真实 OIDC 登录或交易 E2E。Datafeed 的外部 MCP 客户端和外部源 fixture 继续保留。
