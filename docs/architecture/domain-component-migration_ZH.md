# 领域组件结构与兼容迁移

本轮保留现有 workspace crate 和业务边界，统一领域接口、实现目录和共享生命周期。业务逻辑不放在测试框架，Messager、Exchange、Logservice 不独立部署。主机 controller 仍嵌入 API；服务入口组织订阅与持久运行时。

| crate | 领域契约 | 实现目录 | 内部职责 |
| --- | --- | --- | --- |
| backtest | `backtest.rs / IBacktest` | `kline/kline.rs`、`trades/trades.rs` | `replay/`：时钟、屏障、历史 PG、运行协调；`trades/`：撮合与 tick |
| datafeed | `datafeed.rs / IDatafeed` | `binance/`、`twitter/`、`truthsocial/`、`policy/` | `social/`：共享协议；`ingestion/`：持久事实/回放导入；`query/`：快照与计算 |
| controller | `controller.rs / IController` | `docker/`、`kubernetes/`、`standalone/` | `worker/`：冻结计划和 reconcile |
| evaluator | `evaluator.rs / IEvaluator` | `mas/mas.rs` | MAS 的 agents/core/tools 保持内部组织；`llm/` 为旁路评估运行时 |
| exchange | `exchange.rs / IExchange` | `binance/`、`ibkr/`、`polymarket/`、`hyperliquid/`、`kalshi/` | `execution/`：执行契约/加密提交；`market/`：公共快照；交易所专用执行留在各实现目录 |
| exporter | `exporter.rs / IExporter` | `kafka/`、`google_sheets/` | `delivery/`：幂等 sink 和游标运行时 |
| logservice | `logservice.rs / ILogservice` | `default/default.rs` | `archive/`：持久缓冲、不可变对象、索引及 Rust tracing 接线 |
| notification | `notification.rs / INotifier` | `email/`、`telegram/` | `delivery/`：模板、站内投递和有限重试 |
| order | `order.rs / IOrder` | `default/default.rs` | `processing/`：风控/预占/提交；`execution/`：事实、组、跟踪、对账 |
| strategy/runner | `strategy.rs / IStrategy` | `python/python.rs` | `python/worker/`：受限嵌入式 Python；`checkpoint/`：事务状态；`server/`：实例串行和消息路由 |
| messager | `messager.rs / IMessager` | `local/`、`mqtt/` | `storage/`：Inbox/Outbox；MQTT 内聚重试、补放和临时响应配对 |
| wallet | `wallet.rs / IWallet` | `default/default.rs` | `ledger/`：资金/执行事实和预占；账本与投影唯一写入 |

每个根契约继承 `common_context::IProvider`。具体实现的 `init/close` 放在 `impl IProvider`；带配置的旧 `init(argument)` 改为 `configure(argument)`。旧 `<domain>_factory.rs`、manager/client/executor 的转发壳和拼错的 `datafeed_trushsocial.rs` 已移除。Rust 调用方迁移到新模块/类型；不保留一套重复的实现。SDK 是类型与回调协议库，继续保留 `Effects` 与兼容 TradingSignal 模型，不额外建立 provider 注册中心。

共享 `src/common/context` 的 ApplicationContext 按接口类型和 instance_id 懒加载。各领域 Factory 仅为构造/CLI 门面，使用同一个 context；不再各自维护全局 HashMap。入口启动和关闭 context。并发重复注册只保留首个构造器，首次 get 只有一个初始化 owner，关闭会尝试该域所有实例；显式独立测试用 `ApplicationContext::new()`。配置实例键包含隔离身份，排除 traceparent、requestSpanId、attempt_no，只是进程内缓存键，不持久化为业务关联 ID。

Provider 和参数类型按 `DatafeedArgument`、`ExchangeFactory`、`BinanceExchangeConfig` 等统一格式命名。枚举 Rust variant 使用 UpperCamelCase，原有 JSON/MQTT provider 字符串通过显式 serde 名称保留。原有测试按职责迁入对应 `tests/` 镜像目录，保留 private 单元测试访问和显式 Cargo 集成入口。结构验收检查 12 个契约、禁止遗留 `_factory.rs`、移除 sys/store，以及业务域单文件不超过 1200 行。

## storage 与身份边界

| 旧入口 | 新入口 | 兼容处理 |
| --- | --- | --- |
| `SIGBOT__APPDB__TYPE` | `SIGBOT__STORAGE__PROVIDER` | 旧环境源先归一化；部署文件使用新键 |
| `core::store`、业务 `store/` | `core::storage`、业务 `storage/` | 全部 Rust 引用迁移；不改变金融表名称或账本记录 |
| `core::sys` 平台 IAM | Authguard SDK/Gateway | 本地用户/权限实现删除；身份适配在 `context/` |
| sys 业务租户/日志/锁 | `modules/tenant`、`modules/log`、`storage/lock` | 保留业务能力，禁止全局当前用户上下文 |

`TIMESCALEDB` 旧 provider 迁到同一个 PostgreSQL 连接配置；TimescaleDB/pgvector 为该 PG 的扩展。历史迁移文件和已保存的金融事实不因源码重命名而改写。非空旧库验收仍检查金融表保留、Workflow/Run ID 映射和二次幂等。

## 部署环境与交易模式

`scope.env` 表示部署隔离，例如 dev/sit/uat/prod；校验 MQTT-safe 标识，不负责选择交易行为。模式为单一类型 `ExecutionMode::{Live,Paper,Backtest}`：Live 使用真实外部 SDK；Paper 使用模拟盘 API；Backtest 使用专用历史撮合 API、冻结输入和虚拟时钟。不存在第四种环境或交易模式，也不接受迁移别名。

Workflow 保存时验证显式模式与 Order 适配器一致，Run 固定该模式，WorkerPlan 校验并注入模式；虚拟 worker 时钟只允许 Backtest。Order 在写入意图和请求预占前比较钱包与适配器模式；恢复后的对账及成交观察在调用交易所前使用同一风控检查，不匹配则拒绝。回放同时要求独立 scope/run，冻结可得时间，读取持久屏障时钟，关闭实时主动查询；不再根据环境名前缀推断回测。已经保存为 paper/backtest/replay-* 的旧环境标识不自动改成 prod，避免改变租户隔离与历史身份。

E2E 实时主链改用 Paper 和新的独立测试钱包 20001，旧钱包 1 与账本保留；历史回放继续创建各自的 Backtest 钱包。同所双账户补偿使用 Paper。目录/类型检查、单测、真实 Docker/Kubernetes 与 UI 的已执行结果和外部门禁见[验收矩阵](runtime-v2-acceptance_ZH.md)；源码存在不等于生产验收完成。
