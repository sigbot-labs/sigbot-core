# 租户治理与身份访问管理

[English](tenant-governance-and-iam.md) · [文档索引](../../../index_ZH.md)

**状态：** 当前架构规范。

## 唯一归属

Authguard 是平台身份、用户、Tenant、Membership、Role、Permission、Session、登录和
授权策略的唯一写入方。Sigbot 不保存 IAM 镜像表，不提供本地用户 CRUD，也不维护
进程级“当前用户”。因此迁移中禁止出现任何本地 IAM 镜像表。

Sigbot 只保存量化业务资源。`tenant_id` 是 Authguard 签发并由请求身份携带的 UUIDv7
作用域，不是 Sigbot 本地 Tenant 表的外键。Workflow、Connection、Worker、Run、Wallet
和消息主题必须显式携带该作用域；系统中不存在第二个工作区或 Namespace 业务 ID。

| 关注点 | 唯一归属 | Sigbot 消费方式 |
| --- | --- | --- |
| 登录、Session、用户和 Tenant 生命周期 | Authguard | 经 Gateway 认证后读取签名身份 |
| Membership、Role、Permission 和策略判定 | Authguard | API 边界调用官方 Rust Adapter 授权 |
| Workflow、Connection、Worker 配置 | Sigbot API/Core | 以授权后的 `tenant_id` 分区 CRUD |
| 九个数据面组件生命周期 | Sigbot Controller | 以 Tenant/Workflow/Worker 作用域编排 |
| 订单、账本、行情、日志等业务事实 | 对应 Sigbot 领域唯一写入方 | 不反向创建或更新 IAM 数据 |

## 请求边界

1. Envoy Gateway 交给 Authguard 完成认证和授权。
2. API Adapter 校验 Authguard 签名身份，不接受客户端自报 Tenant。
3. Handler 从显式 `RequestIdentity` 取得 `tenant_id`，并对目标资源再次做作用域校验。
4. Repository 的查询和写入条件始终包含 Tenant；禁止使用全局用户上下文或隐式默认租户。
5. Controller 只接受已经持久化并授权的 Workflow/Worker 配置，不能通过 Pod 环境变量扩权。

HTTP、A2A 共用同一 Handler 与授权规则；mcpfather 生成的外部 MCP 通过同一 HTTP 鉴权边界接入。A2A 只暴露实际需要的能力，不复制
HTTP 的全部 CRUD 面，也不得绕过 Authguard。

## 数据与部署隔离

- 控制面 Postgres 保存 Sigbot 业务配置、运行状态和事实，不保存 Authguard IAM 表。
- 控制面 Redis 只缓存 Sigbot 配置和短期协调数据；IAM 缓存由 Authguard 自己管理。
- Controller 按 `tenant_id` 和可选 `workflow_id` 选择远程 Docker/Kubernetes Worker，动态
  配置 Redis、pgvector、TimescaleDB 与 EMQX，并启动九个租户级组件。
- MQTT 基础主题是 `internal/v2/{env}/{tenant_id}`，Workflow 与 Account 继续在其下隔离。
- Secret 只能由 Bootstrap 或外部 Secret Provider 注入，不进入 Workflow 快照、表字段、
  Helm values 或日志。

## 不变量

- Authguard 是 IAM 唯一权威；Sigbot 不实现兼容 IAM 路径。
- `tenant_id` 必须为 UUIDv7，并在入口、Handler、Repository、Topic 和部署标签中一致。
- 未认证、签名无效、跨租户或权限不足的访问在进入业务写事务前拒绝。
- `run_id` 是公开业务幂等与关联标识；Trace Context 只负责可观测传播。
- 删除 Tenant 的 IAM 生命周期不由 Sigbot 猜测；业务资源清理必须由显式、授权且可审计的
  平台流程触发。

## 源码地图

- Authguard 适配与请求身份：`src/core/src/context/`
- HTTP API：`src/api/http/`
- A2A API：`src/api/a2a/`；外部 MCP：[mcpfather 接入](../mcp/overview_ZH.md)
- 业务 Handler/Repository：`src/core/src/modules/`
- Worker 与租户组件编排：`src/controller/`
- Helm 集成：`deploy/helm/sigbot/charts/middleware/`
