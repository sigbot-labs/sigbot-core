# API 服务

[English](overview.md) · [十领域表和协议契约](../../domain-model_ZH.md)

HTTP、A2A 对齐 tenant、cluster、datasource、exchange、strategy、order、ledger、notifier、exporter、workflow 十个领域的 CRUD。两个内置协议复用 Core ResourceHandler 和 IGenericRepository 的权限、校验和持久化逻辑。

HTTP 每个领域提供 list/get/create/update/delete；实体入口位于 src/api/http/src/<domain>/<entity>.rs。IHttpApi 和 Axum 实现合并在 src/http.rs；Swagger 资源和实现继续位于 src/swagger。

所有操作显式传 env；tenant、principal 和 allow/deny grants 仅来自 Authguard 已验证上下文。tenant_info 保存 Sigbot 业务资料，不复制 IAM。Cluster 通过外键归属 tenant，认证配置使用 Secret 引用。

Workflow 的 nodes/edges 整体保存在 dag_config JSON。对运行中 Workflow、活跃 Cluster、已提交订单和不可变财务事实的修改受到约束。Ledger 查询包含余额与最近流水。

REST 入口为 src/cmd/src/startup/api/http.rs 的 SigbotHttpStarter；A2A 在相邻文件。聚合入口为 startup/standalone.rs 的 SigbotStandaloneStarter。所有进程使用同一 Provider 生命周期架构。MCP 改为通过 [mcpfather](../mcp/overview_ZH.md) 从 HTTP OpenAPI 独立生成。
