# 十领域配置与 CRUD

数据库基线的前半部分只定义十个领域主表，后半部分定义运行时执行、恢复和审计存储。HTTP、MCP、A2A 共用 Core 的 ResourceHandler 和 IGenericRepository；协议层不写 SQL。

| 领域 | 主表 | 主要内容 |
| --- | --- | --- |
| tenant | tenant_info | Sigbot 业务租户资料；id 必须是 Authguard 已验证的 tenant UUID |
| cluster | cluster_info | 租户独享 Docker/Kubernetes 连接、认证 Secret 引用、资源和中间件；一个租户可有多个 Cluster |
| datasource | datasource_info | Twitter、TruthSocial、自定义 HTTP/MCP/A2A 等连接 |
| exchange | exchange_info | Binance、OKX、IBKR、Polymarket 等交易连接 |
| strategy | strategy_info | Python code、default_parameters 和配置 |
| order | order_info | 仓位执行意图、订单状态和交易所订单标识 |
| ledger | ledger_info | 账户、执行模式与钱包归属；查询同时返回 balances 和最近 100 条 entries |
| notifier | notifier_info | 各种通知渠道连接；渠道细节放 configuration |
| exporter | exporter_info | Kafka、Google Sheets 等外部推送连接 |
| workflow | workflow_info | DAG 整体保存在 dag_config JSON 中，不额外建 nodes/edges 子表 |

tenant_info 是业务资料，不负责登录、用户、角色、权限或 IAM 租户生命周期。cluster_info.tenant_id 有外键，连接和策略主表同样引用 tenant_info。账户和订单仍由复合键约束租户、环境及幂等坐标。

每个配置实体一行，configuration 保存对应 provider 的非敏感参数，secret_ref 保存 central Secret 引用。strategy_info 用 code 和 default_parameters 明确区分可执行代码与初始参数。订单的 UUIDv7 id 用于 CRUD，已有复合主键继续负责执行幂等。

## 三种协议

HTTP 每个领域提供以下五种操作，集合名分别为 tenants、clusters、datasources、exchanges、strategies、orders、ledgers、notifiers、exporters、workflows：

- GET /api/v1/<collection>?env=dev&limit=50&offset=0
- POST /api/v1/<collection>?env=dev
- GET /api/v1/<collection>/<id>?env=dev
- PUT /api/v1/<collection>/<id>?env=dev
- DELETE /api/v1/<collection>/<id>?env=dev

POST/PUT body 是实体字段对象。id、tenant_id、env、审计字段、组合版本由服务端管理。创建返回 id，列表返回 data 数组，详情返回 data 对象。未知字段和无权限资源会被拒绝；limit 范围 1..200。

MCP 有 50 个工具，命名为 <domain>_<list|get|create|update|delete>，参数为 env、按需提供的 id/fields 和列表分页参数。

A2A DataPart 或 JSON TextPart 使用同一命令，例如：

```json
{"domain":"strategy","operation":"create","env":"dev","fields":{"name":"sample","code":"class Strategy: pass","default_parameters":{"window":20}}}
```

三个协议均保留 Authguard 验证后的 principal、resource URN 和 allow/deny grants。调用者不能通过 body 覆盖它们。

## 必要的内部存储

wf_run/checkpoint/sequence/timer、msg_inbox/outbox、行情和评估历史、回测游标、成交及订单执行组、复式账本/余额/预留、通知投递和导出游标仍有实际运行时读写方。这些表承担不同的幂等、恢复或财务约束，不是配置表的重复子表，不能合成一个 JSON 字段或删除后保留假成功。没有运行时读写方的 wlt_equity_snapshot，以及旧聚合通知页面专用的 ntf_read 和对应死代码已删除。

订单只可编辑 pending 且尚未形成 prepared_submission/成交的记录。账户配置只可在尚未形成组合财务状态时编辑；余额和成交流水作为组件拥有的事实/投影查询。已提交 Workflow、活跃 Cluster、财务事实不能经配置 CRUD 任意覆盖。删除仍受外键约束。

旧 cfg_connection 和 HTTP 的 connections/workers/knowledge/market/notifications/overview 入口已移除。当前对外操作范围收敛为十领域 CRUD；运行时 Controller/组件仍保留其内部执行逻辑。

## 基线应用

这次按要求直接整理 20261031000000 初始基线，不执行在线 DROP/ALTER，也不改动任何现有业务库。MigrationRunner 的 SHA-256 漂移检查继续生效：已经应用旧基线的库不能直接重复应用新文件，需要单独的数据升级/导入方案。新建空库可直接使用 migration/migrate.py。

本地验证：cargo test -p sigbot-core --test domain。该测试覆盖完整 SQLite DDL、十领域 CRUD、未知字段及租户隔离；HTTP 集成测试另验证真实 TCP 请求和 Authguard 签名。PostgreSQL 测试必须指向专用空库，使用 SIGBOT_DOMAIN_TEST_POSTGRES_URL 并显式运行 ignored 测试。
