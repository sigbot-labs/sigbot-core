# 领域组件结构与当前边界

本文记录当前源码结构，不描述旧目录兼容层。平台面由 API、独立 Controller 和 Web
组成；数据面由九个租户级 Workflow 组件组成。Messager 与 Exchange 是嵌入宿主进程
的共享库，不独立部署。认证、用户、租户身份和权限唯一归 Authguard；Sigbot 只保存
经过验证的业务租户 UUID。

## 当前目录与职责

| 目录 / crate | 领域契约 | 主要实现 | 部署边界 |
| --- | --- | --- | --- |
| `src/component/backtest` | `IBacktest` | 历史读取、虚拟时钟、屏障、撮合 | 租户级动态组件 |
| `src/component/datafeed` | `IDatafeed` | 社交/政策/交易所接入、事实持久化、快照与计算 | 租户级动态组件 |
| `src/component/evaluator` | `IEvaluator` | LLM 旁路评估、案例引用、五态概率 | 租户级动态组件 |
| `src/component/exporter` | `IExporter` | Kafka、Google Sheets、游标与重试 | 租户级动态组件 |
| `src/component/logservice` | `ILogservice` | WAL 缓冲、不可变对象归档、索引和游标读取 | 租户级动态组件 |
| `src/component/notifier` | `INotifier` | Email、Telegram、站内投递、模板和状态 | 租户级动态组件 |
| `src/component/order` | `IOrder` | 风控、预占、提交、执行组、对账 | 租户级动态组件 |
| `src/component/strategy/runner` | `IStrategy` | Python 沙箱、事务状态、串行回调和消息路由 | 租户级动态组件 |
| `src/component/wallet` | `IWallet` | 资金事实、不可变账本、预占和投影 | 租户级动态组件 |
| `src/controller` | `IController` | Docker/Kubernetes Worker 与租户中间件生命周期 | 独立平台服务 |
| `src/exchange` | `IExchange` | Binance、IBKR、Polymarket 等市场/执行适配 | 嵌入 Order/Datafeed 等宿主 |
| `src/messager` | `IMessager` | Local/MQTT transport、Inbox/Outbox 和临时响应配对 | 嵌入九个数据面宿主 |
| `src/core` | Handler/Repository | 业务用例编排、唯一数据库访问入口 | 被 API/Controller/组件复用 |
| `src/api/{http,a2a,mcp}` | 协议 Adapter | 鉴权、DTO 转换、路由 | 平台 API 进程 |

九个组件均由 Workflow 节点的 `config.module` 选择。Controller 从不可变 Run 图生成
WorkerPlan，按订阅者优先顺序启动节点，并通过同一套 Docker/Kubernetes Provider 完成
部署、更新、恢复和停止。Order 与 Wallet 不再作为平台常驻服务静态部署。

## Provider 与源码组织

根领域契约继承 `common_context::IProvider`。入口负责向同一
`ApplicationContext` 注册实例、启动 Context 并在统一信号到达后关闭。Provider 的
业务函数放在具体类型的 `impl` 中；生产代码不以自由函数承载领域状态。

数据库访问只允许位于 `src/core/src/modules/<domain>/storage/`。协议 Adapter、动态
组件、Controller、Messager 与 Exchange 不直接执行 SQL。PostgreSQL/SQLite Adapter
共享同一领域契约；PostgreSQL 是跨进程和生产持久性的验收基准，SQLite 仅用于本地
单进程开发和相应测试。

生产源码文件上限为 800 行；Core 领域 Handler/Storage 分别遵守更严格的 500/250
行职责边界。测试镜像源码目录放在对应 crate 的 `tests/`，不能用内联大测试或独立
测试项目绕开结构门禁。

## 配置与身份边界

- 所有业务模块通过顶部导入后的短名 `get_config()` 读取统一配置。
- 配置优先级是 defaults `<` YAML `<` `SIGBOT__...` 双下划线环境覆盖；未知字段拒绝。
- Secret 值由中央配置或部署 Secret 注入，Worker 元数据只保存 Secret key 名，不保存
  token、密码、私钥或自定义环境变量。
- 请求身份由 Authguard 官方 Adapter 验证后显式传入 Handler；禁止进程全局当前用户。
- `env + tenant_id + workflow_id + node_id + run_id` 是数据面隔离范围；
  `traceparent` 和 `requestSpanId` 不参与业务身份或持久幂等。

`ctl_worker.middleware` 是强类型租户中间件声明，只允许为 Redis、pgvector、
TimescaleDB、EMQX 配置批准的 `image` 和 `resources` 字段。Web 表单模式与 YAML
模式使用同一 API DTO 和校验器；Controller 在九组件启动前幂等调和中间件并绑定
端点。运行凭据必须已存在于远端 Secret 或受控 Docker env 文件，Controller 不把
明文 Secret 保存到数据库。

## Storage 与 ID

当前项目只有最终态基线迁移：

```text
migration/20261031000000/
├── postgres/{init.ddl.sql,init.dml.sql}
└── sqlite/{init.ddl.sql,init.dml.sql}
```

实体主键和相互依赖外键使用 UUIDv7。`node_id`、`seq`、`revision`、交易所原生 ID 等
是有序坐标或外部事实标识，不应机械改成 UUID。项目不提供旧 `APPDB`、本地 IAM 表、
旧消息 topic 或旧表名的兼容层。

控制面 PostgreSQL 保存 Workflow、Worker、连接等配置；部署镜像可包含 TimescaleDB
和 pgvector 扩展。租户数据面的 pgvector/TimescaleDB 则由 scoped `ctl_worker` 独立
声明和调和，两者不能混写为“一定是同一个数据库”。金融事实表由对应数据面组件的
Core Repository 唯一写入。

## 部署环境与交易模式

`scope.env` 是 dev/sit/uat/prod 等部署与消息隔离环境。交易行为只由
`ExecutionMode::{Live, Paper, Backtest}` 决定，两者互不推断：

- Live 调用真实交易所执行 Adapter；未知结果必须按原订单身份查询，禁止盲目重下。
- Paper 使用明确的模拟盘或内部撮合 Adapter，不修改真实交易所 URL 来伪装模式。
- Backtest 使用冻结历史、虚拟时钟、固定顺序和专用撮合服务。

Workflow 保存、Run 快照、WorkerPlan、Order、Wallet 与回放入口都校验同一模式。
详细数据表和恢复契约见[运行时数据模型](runtime-v2-migration_ZH.md)，当前证据和明确
缺口见[验收矩阵](runtime-v2-acceptance_ZH.md)及[编码代理交接文档](../development/HARDCOVER.md)。
