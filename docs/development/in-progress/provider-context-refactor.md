# 领域组件与 ApplicationContext 收敛重构

日期：2026-10-31 至 2026-11-01。状态：本文是该轮重构的历史实施记录，不是当前能力清单。
当本文的阶段性结论与现行架构或最新验收边界冲突时，以
[`HARDCOVER.md`](../HARDCOVER.md)、架构总览和领域 Overview 为准。本文记录本轮已确认
的改动范围、目录映射和当时的验证证据；后写阶段覆盖前面阶段的历史状态。

## 1. 范围与约束

本轮处理 backtest、datafeed、evaluator、exchange、exporter、logger、messager、notifier、strategy runner，并同步处理 controller、API、cmd、types 与 Core Repository 边界。核心是让现有 Provider 设计统一使用共享 ApplicationContext，并合并重复的工厂、装配、持久化与服务宿主逻辑。

- 遵循根目录及 `.agents/AGENTS.md`，保留领域边界与工作区骨架；领域接口继承 `IProvider`。
- `lib.rs` 与 `mod.rs` 仅声明模块和导出；`server.rs` 承担装配和服务入口；具体实现放在有业务含义的包中。
- 删除已被替代的旧执行分支、无实现的占位逻辑和重复配置解析，不引入兼容路由或另一套 Registry/Manager。
- 不以重命名代替架构收敛：注册、实例获取、初始化和关闭都落在共享 ApplicationContext；重复单例与旁路生命周期必须处理。
- 保留现有有效业务能力，包括真实行情、LLM、历史回放、撮合、日志、导出、通知及消息事务。
- 禁止新增含义泛化的 `runtime.rs`、`feed.rs`、`matching.rs`、`replay.rs`、`delivery.rs` 或 `in_app/`。明确的领域名和必要的业务辅助文件可以保留。
- 单源码文件不超过 800 行有效代码；领域 PostgreSQL 仓储优先复用通用 CRUD，并遵守仓储文件的更严格限制。不能通过压行或无意义拆分满足限制。
- 代码量与文件数必须实质减少。移入 Core 的代码仍计入本轮总量，不能把搬运记作减少。

### 冻结目录

以下目录整体冻结，包括文件内容、结构、文档及嵌入式 Python 接口：

```text
src/component/strategy/
├── sdk/                         # 完整冻结
└── runner/src/python/           # 完整冻结
```

检查发现 `runner/src/python/python.rs` 引用 `sigbot_exchange::exchange::ExchangeFactory`。按已确认的冻结要求，暂缓删除这个被引用的符号；该入口只使用共享上下文，不新增旧注册表或兼容执行路径。其他未被冻结目录依赖的旧 Factory 入口删除。不得通过修改共享接口间接破坏冻结目录。

工作区已有大量其他改动。本轮在现有工作区上实施，不重置、覆盖或提交这些改动。源码基线与冻结目录 SHA-256 已保存到本机 `/tmp/sigbot-provider-refactor-20261031/`，不包含凭据。

## 2. 重构前结构

以下仅列本轮 Rust 源码；花括号表示同层文件，冻结目录不展开。

```text
src/
├── backtest/src/
│   ├── {lib.rs,backtest.rs}
│   ├── kline/{mod.rs,kline.rs}
│   ├── trades/{mod.rs,trades.rs,matching.rs,ticks.rs}
│   ├── replay/{mod.rs,clock.rs,barrier.rs,postgres.rs,runtime.rs}
│   └── server/{mod.rs,backtest_server.rs,execution_runtime.rs}
├── datafeed/src/
│   ├── {lib.rs,datafeed.rs}
│   ├── binance/{mod.rs,binance.rs}
│   ├── twitter/{mod.rs,twitter.rs}
│   ├── truthsocial/{mod.rs,truthsocial.rs}
│   ├── policy/{mod.rs,policy.rs}
│   ├── social/{mod.rs,feed.rs,transport.rs}
│   ├── ingestion/{mod.rs,ingestor.rs,persistent.rs,replay.rs}
│   └── query/{mod.rs,market.rs,compute.rs}
├── evaluator/src/
│   ├── {lib.rs,evaluator.rs,config.rs}
│   ├── llm/{mod.rs,llm.rs}
│   ├── mas/
│   │   ├── {mod.rs,mas.rs}
│   │   ├── agents/{mod.rs,alpha_agent.rs,auditor_agent.rs,boot_agent.rs,loader_agent.rs}
│   │   ├── core/{mod.rs,agent_base.rs,agent_loop.rs,agent_tool.rs,orchestrator.rs}
│   │   └── tools/{mod.rs,binance_tool.rs,twitter_tool.rs}
│   └── server/{mod.rs,evaluator_runner.rs}
├── exchange/src/
│   ├── {lib.rs,exchange.rs}
│   ├── binance/{mod.rs,binance.rs,execution.rs}
│   ├── ibkr/{mod.rs,ibkr.rs,execution.rs}
│   ├── polymarket/{mod.rs,polymarket.rs,execution.rs,settlement.rs}
│   ├── hyperliquid/{mod.rs,hyperliquid.rs}
│   ├── kalshi/{mod.rs,kalshi.rs}
│   ├── execution/{mod.rs,adapter.rs,prepared.rs}
│   ├── market/{mod.rs,market.rs}
│   └── order_book/mod.rs
├── exporter/src/
│   ├── {lib.rs,exporter.rs}
│   ├── kafka/{mod.rs,kafka.rs}
│   ├── google_sheets/{mod.rs,google_sheets.rs}
│   ├── delivery/{mod.rs,runtime.rs,sink.rs}
│   └── server/{mod.rs,exporter_server.rs}
├── logger/src/
│   ├── {lib.rs,logger.rs}
│   ├── default/{mod.rs,default.rs}
│   ├── archive/{mod.rs,archive.rs,tracing.rs}
│   └── server/{mod.rs,log_server.rs}
├── messager/src/
│   ├── {lib.rs,messager.rs}
│   ├── local/{mod.rs,local.rs}
│   ├── mqtt/{mod.rs,mqtt.rs,outbox.rs,reply.rs}
│   └── storage/{mod.rs,postgres.rs}
├── notification/src/
│   ├── {lib.rs,notification.rs}
│   ├── email/{mod.rs,email.rs}
│   ├── telegram/{mod.rs,telegram.rs}
│   ├── delivery/{mod.rs,runtime.rs}
│   └── server/{mod.rs,notification_forwarder.rs}
└── strategy/
    ├── runner/src/
    │   ├── {lib.rs,strategy.rs}
    │   ├── bin/sigbot-strategy-worker.rs
    │   ├── checkpoint/{mod.rs,postgres.rs}
    │   ├── server/{mod.rs,strategy_node.rs,strategy_runner.rs,strategy_runtime.rs}
    │   └── python/                 # 冻结
    └── sdk/                        # 冻结
```

## 3. 确认后的目标结构与实际归属

下面是实施目标。合并以职责和有效代码行数为依据；最终实际树在本文验收部分更新，不为符合示意树而拆出空包装。

```text
src/
├── api/
│   ├── a2a/src/{lib.rs,a2a.rs,a2a/workflow.rs}
│   ├── http/src/{lib.rs,http.rs,<domain>/<entity>.rs}
│   └── mcp/src/{lib.rs,mcp.rs,mcp/{authguard.rs,http.rs,workflow.rs}}
├── common/context/src/
│   ├── lifecycle/{application.rs,provider.rs}
│   └── util/{mod.rs,cgroup.rs,mems.rs,panics.rs,signal.rs}
├── core/src/
│   ├── storage/{mod.rs,storage.rs,postgres.rs,sqlite.rs,mongo.rs,parameters.rs,lock/}
│   └── modules/<domain>/
│       ├── handler/{mod.rs,<domain>.rs}
│       └── storage/{mod.rs,postgres.rs,sqlite.rs}
├── cmd/src/
│   ├── lib.rs
│   ├── bin/sigbot.rs
│   └── startup/{mod.rs,api/{mod.rs,std.rs,a2a.rs,http.rs},component/{mod.rs,backtest.rs,
│                datafeed.rs,evaluator.rs,exporter.rs,logger.rs,notifier.rs,order.rs,strategy.rs,wallet.rs},
│                controller.rs,mcp.rs,internal/}
├── component/
│   ├── backtest/src/
│   │   ├── {lib.rs,server.rs,backtest.rs}
│   │   ├── kline/{mod.rs,kline.rs}
│   │   ├── trades/{mod.rs,trades.rs}
│   │   └── history/{mod.rs,history.rs,virtual_clock.rs}
│   ├── datafeed/src/
│   │   ├── {lib.rs,server.rs,datafeed.rs}
│   │   ├── binance/{mod.rs,binance.rs}
│   │   ├── twitter/{mod.rs,twitter.rs}
│   │   ├── truthsocial/{mod.rs,truthsocial.rs}
│   │   ├── fed/{mod.rs,federal_reserve.rs}
│   │   ├── treasury/{mod.rs,treasury.rs}
│   │   ├── social/{mod.rs,social.rs}
│   │   └── query/{mod.rs,market_query.rs}
│   ├── evaluator/src/
│   │   ├── {lib.rs,server.rs,evaluator.rs}
│   │   └── llm/{mod.rs,llm.rs}
│   ├── logger/src/
│   │   ├── {lib.rs,server.rs,logger.rs}
│   │   └── s3/{mod.rs,s3.rs,capture.rs}
│   ├── exporter/src/
│   │   ├── {lib.rs,server.rs,exporter.rs}
│   │   ├── kafka/{mod.rs,kafka.rs}
│   │   └── google/{mod.rs,google_sheets.rs}
│   ├── notifier/src/
│   │   ├── {lib.rs,server.rs,notifier.rs}
│   │   ├── email/{mod.rs,email.rs}
│   │   └── telegram/{mod.rs,telegram.rs}
│   ├── order/src/{lib.rs,server.rs,order.rs,execution/,processing/}
│   ├── wallet/src/{lib.rs,server.rs,wallet.rs,ledger/}
│   └── strategy/
│       ├── runner/src/
│       │   ├── {lib.rs,server.rs,strategy.rs}
│       │   ├── bin/worker.rs
│       │   ├── checkpoint/{mod.rs,checkpoint.rs}
│       │   └── python/                 # 整体冻结
│       └── sdk/                        # 整体冻结
├── controller/src/
│   ├── {lib.rs,server.rs,controller.rs}
│   ├── docker/{mod.rs,docker.rs}
│   ├── docker/{mod.rs,docker.rs,middleware.rs}
│   ├── kubernetes/{mod.rs,kubernetes.rs,middleware.rs}
│   └── worker/{mod.rs,middleware.rs,plan.rs,reconciler.rs,router.rs}
├── exchange/src/
│   ├── {lib.rs,server.rs,exchange.rs}
│   ├── binance/{mod.rs,binance.rs,orders.rs}
│   ├── ibkr/{mod.rs,ibkr.rs,orders.rs}
│   ├── polymarket/{mod.rs,polymarket.rs,orders.rs,settlement.rs}
│   ├── hyperliquid/{mod.rs,hyperliquid.rs}
│   ├── kalshi/{mod.rs,kalshi.rs}
│   ├── paper/{mod.rs,paper.rs}
│   └── market/{mod.rs,market.rs}
├── messager/src/
│   ├── {lib.rs,server.rs,messager.rs}
│   ├── local/{mod.rs,local.rs}
│   └── mqtt/{mod.rs,mqtt.rs}
└── types/src/
    ├── lib.rs
    └── {backtest,controller,datafeed,evaluator,exchange,exporter,llm,logger,
         messager,notifier,order,strategy,wallet,workflow}/
```

`component/` 下九个数据面组件由 controller 按租户或 Workflow 动态调度：backtest、datafeed、evaluator、strategy、logger、exporter、notifier、order、wallet。`api/`、controller、web 是 Helm 预部署的平台面；exchange 与 messager 是共享库，不独立部署。所有异步地址都由统一 topic 类型生成，scope 固定为 `env/tenant_id`，Workflow 再按 `workflow_id` 隔离。

## 4. 详细改动矩阵

| 领域 | 改动 | 保留的关键行为 |
|---|---|---|
| 通用装配 | 使用已存在的 ApplicationContext；删除重复 Factory 转发、独立实例注册表和旁路单例；入口负责注册/init/shutdown | 惰性初始化、相同实例共享、失败清理、反向关闭、租户/账户/模式隔离 |
| Backtest | server 平铺；历史读取与回放宿主合并到 history；虚拟时钟与屏障合并；逐笔撮合合并到 trades | 固定次序、事件可得时间、时钟推进屏障、隔离重放、部分成交、费用和检查点 |
| Datafeed | ingestion 公共事务逻辑并入领域主文件；查询与计算合理合并；社交源共享解析/传输；Fed/Treasury 具名实现 | REST/WS/MCP/A2A/APIToken 接入、来源/时间/序列、历史导入、多所快照、查询边界、迟到修订 |
| Evaluator | server 平铺；删除无生产调用方的旧 MAS 分支及其重复调度配置；真实 LLM Provider 接入生命周期 | 异步旁路消费、概率与超参、有效期、事实/推断、案例记忆、无未来数据及无交易副作用 |
| Exchange | 将执行契约与公共提交类型归入 exchange.rs；具名各交易所订单实现；Paper API 独立实现；删除多套适配器工厂 | Live/Paper/Backtest 模式、精度/限流、预构建交易、未知回执先对账、成交游标和资金隔离 |
| Exporter | 公共游标/批量/背压逻辑归入 exporter.rs；Kafka/Google 各自直接实现领域契约；删除二次 SinkFactory 包装 | 幂等固定目标范围、批量、错误重试、游标推进、Kafka 回执 |
| Logger | 本地 WAL、S3 与日志采集统一放入 s3；索引 Repository 位于 Core；服务入口平铺 | 持久缓冲 ACK、不可变对象、索引、断点续传、去重、游标读取、Python/Rust 输出与异常 |
| Messager | SQL 仓储移至 Core；消息通用暂存/重试与 MQTT 网络逻辑分离；小型配对和 relay 类合并到具名领域文件 | QoS1、持久会话、落盘后 ACK、Inbox/Outbox、租户 ACL、重试/背压/补放、traceparent |
| Notification | 公共投递状态机并入 notification.rs；保留 Email/Telegram 实现；站内状态仍在公共事务中 | 模板、投递状态、业务幂等键、未知结果不盲重发、站内通知 |
| Strategy runner | 外部宿主/节点协调合并到 strategy.rs；服务入口平铺；worker 源文件重命名 | 实例串行、事务草稿与 Effects 同提交、请求/结果路由、检查点、隔离 worker；SDK/Python 不改 |

### Inbox/Outbox 的依赖和事务边界

`sigbot-messager -> sigbot-core -> sigbot-types` 单向依赖。Core 不引用 Messager、MQTT 客户端或临时响应缓存。

Core 消息仓储复用基础 PostgreSQL 能力，内部保留调用方传入事务的接口。Inbox 接受/完成、业务状态写入、Outbox 入队可在同一事务提交。原子 seq 分配、相同键的事实一致性校验、行锁和重试状态是专用仓储操作，不能退化为互不关联的 CRUD。`storage` 模块为 Core crate 私有；组件、三种 API 和测试宿主只使用公开 Handler，不能绕过服务层持有 Repository。

网络发布、broker PUBACK 等待、重试调度和 `requestSpanId` 临时回显仍归 Messager。持久化不得包含 `requestSpanId`；重试保留 run_id 并递增 attempt_no。迁移期间保持原有发件箱并发保护，不能引入重启丢失或双 owner。

### Google Sheets 与 worker 命名

Google Sheets 已使用 reqwest 的 HTTPS 请求，SDK 只用于 ValueRange 类型。本轮统一到 `google/google_sheets.rs`，复用现有 HTTP 客户端和凭据机制，移除无其他用途的 google-sheets4 依赖。固定范围写入保留幂等语义。官方接口：[values.update](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets.values/update)、[values.batchUpdate](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate)。

worker 源码改为 `src/component/strategy/runner/src/bin/worker.rs`；通过 Cargo 的显式 `[[bin]]` 声明保持业务可执行文件 `sigbot-strategy-worker`，同步 Docker 构建引用。

## 5. 实施顺序

1. 保存源码量、冻结目录哈希和现有工作区基线；先写本文。
2. 处理 Core 消息仓储边界，更新全部非冻结调用方。
3. 合并具名领域文件、平铺 server.rs、修改模块声明和测试路径。
4. 删除重复工厂与旧分支，将服务对象的生命周期纳入 ApplicationContext。
5. 合并 Exporter 实现、落实 Fed/Google/worker 命名，清理未使用依赖。
6. 验证冻结目录、依赖方向、源码量及文件限制；格式化仅触及允许的文件。
7. 运行受影响工作区编译、lint、现有 UT 和必要的消息/回放集成测试；同步文档和实际结果。

## 6. 验收与执行记录

基线：九个领域及 strategy SDK 共 166 个 Rust/Python 生产源码文件，20,014 行有效代码（剔除注释、文档注释和空白），冻结范围 49 个文件。最终总量需加上迁入 Core 的新增实现，不能只比较九个目录。冻结目录以本轮开始时 SHA-256 为准；已有 staged SDK 修改属于此前工作，不回退。

- [x] 保存基线并生成本文。
- [x] 两个冻结目录的文件集合与哈希完全一致。
- [x] 非冻结目录不存在被淘汰的泛化文件和重复工厂。
- [x] ApplicationContext 生命周期收敛，关闭无遗留任务。
- [x] Core 仓储没有对 Messager/SDK 的反向依赖，消息事务/去重不减弱。
- [x] 源码总量减少，文件满足大小限制，mod.rs 仅有轻量声明和导出。
- [x] 格式化、编译、Clippy 和现有 UT 通过。
- [ ] 消息重复/乱序/重启、历史回放/分片与真实链路回归通过。

构建前检查 target 大小，超过 10 GiB 先执行 cargo clean；控制 Cargo 并发和产物体积。持续检查 CPU、可用内存、磁盘与 I/O，必要时停止本轮低优先级重负载任务。遇网络限制使用 `HTTPS_PROXY=http://127.0.0.1:8800`。本轮不读取凭据内容，不执行 git commit、发布或远端部署。

后续在此追加各阶段完成情况、实际文件树、代码量和验证命令结果。

### 第六阶段：单配置入口、E2E 收敛与 Helm 可观测性

- 业务组件不再读取自定义数据库/token 环境变量；环境覆盖只由 Core 配置加载器解析，调用方统一在顶部导入并使用短名
  `get_config()`。E2E 的 `MARKETPULSE_*` 仅属于测试宿主边界，由 runner 一次转换为 `SIGBOT__...` 层级。
- `use-cases/runner.py` 与 `deploy/tests/` 已删除；Marketpulse case 内的 `runner.py` 是唯一入口，负责部署、场景、报告和清理。
  verifier 已按实际时间线重排到 infra/controller/datafeed/exchange/messager/strategy/order/backtest/exporter/logger/workflow/web。
- 镜像只有一个 `deploy/images.py` catalog 和一套 Helm values；global/aliyun 通过运行时值覆盖选择，不创建双目录或双 YAML。
- Helm 模板由根级大文件拆为 `api/`、`controller/`、`platform/`、`middleware/`、`gateway/`、`web/`、
  `observability/`。Order/Wallet 不再静态预部署；它们与另外七个组件统一由 Controller 调度。
- Secret bootstrap 对齐 AgentVault 的统一前缀：Kubernetes、AWS、GCP、Vault 共用 `setup-secret.sh` 和同一份 env schema；
  chart 本身只保存 Secret 引用，应用密码通过 `SIGBOT__` 中央配置层映射。
- Grafana 使用一套 dashboard 生成模板产出 backtest、datafeed、evaluator、exporter、logger、notifier、strategy、
  controller、order、wallet 十个领域仪表盘；ServiceMonitor 与 dashboard sidecar label 均为显式开关。Controller 创建的动态
  worker Pod 暴露管理端口并带 Prometheus scrape 注解。
- `migration/tests/` 已删除；SQLite 迁移幂等/漂移、Worker scope 外键和真实 PostgreSQL 钱包约束统一由 Marketpulse
  `database` 场景验收，钱包约束 SQL 归入该 use case 的 `config/sigbot/`。
- 命令宿主的管理端实现重命名为 `startup/internal/management.rs`。API、controller、九个动态组件与 standalone
  的主结构体都显式持有 `management: SigbotManagementStarter`，统一启动、就绪、指标与优雅关闭生命周期。

### 第一阶段实施记录

- 已平铺九个领域的入口并合并已确认的泛化文件。Backtest 的未实现 Cron 包装已删除，Kline/Trades Provider 接入现有真实撮合及历史协调实现，初始化失败由上下文清理任务。
- 已将消息仓储迁入 Core，复用 `PostgresRepository` 连接池。出箱行锁通过 `PendingMessage` 保持至网络发布结果提交；Core 不依赖 MQTT、Messager 或临时配对缓存。消息发布仍在 Messager 内执行。
- 已删除消息缺失 Workflow 的旧恢复方法及回放 owner 自动改写；对应旧兼容断言不再适用。E2E 的兼容升级专用场景、探针和样本已撤销，正式数据库迁移与当前格式的重复/重启/回放测试保留。
- 已删除九个数据面组件服务入口中的重复单例和旧执行开关；Order/Wallet 后续已一并迁入 `component/`，不存在平台级兼容宿主。
- 实际 LLM 实现通过 `IEvaluator` 接口投影共享同一个上下文实例；领域工作任务采用较高关闭阶段，先于消息与存储资源关闭。SDK 与 Python 的策略契约保持原样。
- Kafka/Sheets 的 Writer/Sink/Factory 重复包装已合并。Kafka 关闭时等待生产回执；Sheets 使用现有 reqwest 与固定目标范围。S3 归档构造器和 HTTP 日志任务都接入宿主生命周期。
- 无生产调用方的旧 MAS 工具、代理、调度配置及空目录已删除。真实 LLM 路径只接受有界事实并校验事件/可得时间；模型返回非法 JSON 时返回错误，不制造占位超参。
- 已同步 worker 源文件路径、Cargo 显式 bin 配置和 Docker 构建路径；测试源码按新的领域职责归位，并通过显式 Cargo test target 保留用例入口，避免重复发现同一组测试。
- 第一阶段首次完整检查曾需要重建本机已清理的依赖缓存；该阶段只记录中间状态，不作为最终验收结论。两个冻结目录文件集合与字节哈希均保持一致，最终编译与测试证据见第五阶段。

### 第二阶段：类型扁平化与 Repository 边界

新增约束不保留旧路径兼容层：

```text
src/types/src/
├── lib.rs
├── llm/
├── backtest/
├── controller/
├── datafeed/
├── evaluator/
├── exchange/
├── exporter/
├── logger/
├── messager/
├── notifier/
├── order/
├── strategy/
├── wallet/
└── workflow/
```

删除空的 `api/` 和中间层 `modules/`，`decode_arg_config` 迁到 types 根；所有可修改源码直接使用
`sigbot_types::<domain>`。由于 Strategy SDK 与 runner Python 被明确冻结，crate 根只保留一个不对应物理目录、标记为
`doc(hidden)` 的导出 facade，专供这两个冻结子树编译；新代码禁止引用该 facade。

12 个独立领域生产源码不得持有 SQL 字符串、`sqlx::query*`、`PgPool` 或数据库事务实现。数据库能力按领域归入
`src/core/src/modules/<domain>/storage/`，由具名 `XxxRepository` 封装；领域 Provider 只编排仓储方法、消息和外部服务。
测试可通过 Repository 或 E2E verifier 读取事实，不把业务 SQL 复制回领域 crate。初次扫描发现需要迁移的生产文件：

- Backtest：`history/history.rs`、`history/virtual_clock.rs`、`trades/trades.rs`；
- Datafeed：`datafeed.rs`、`query/market_query.rs`；
- Controller：`worker/reconciler.rs`；Evaluator：`llm/llm.rs`；Exporter：`exporter.rs`；
- Logger：`s3/s3.rs`、`server.rs`；Notification：`notification.rs`；
- Order：`execution/{events,groups,tracker,venue}.rs`、`processing/reservation.rs`、`server/order_runtime.rs`；
- Strategy runner：`checkpoint/postgres.rs`、`strategy.rs`；
- Wallet：`ledger/{execution,reservation}.rs`。

Exchange 与 Messager 生产源码当前未发现直接 SQL；仍纳入最终禁止项扫描。Repository 迁移后追加静态门禁，扫描上述
12 个领域 `src/`，禁止 SQL 宏、SQL 字面量、`PgPool`、`Transaction` 和 `QueryBuilder`。

第二阶段开始前已取得第一阶段证据：workspace all-target check 无告警；8 个可修改领域通过
`clippy --no-deps --all-targets -D warnings`，Strategy runner 只对冻结 Python 子树的既有 lint 做命令级豁免；Core 与九领域
`lib + tests` 为 0 失败。Docker E2E 的 structure/schema/infra/ACL/database 与大多数 Rust 边界通过；完整 pipeline 因
Evaluator 到 mock OpenAI 的请求失败而超时，旧版本运行已停止并保留报告
`use-cases/marketpulse-autonomy-quantification/e2e/reports/20261031070850/`。第二阶段完成后必须使用新产物重跑，旧报告不作为最终通过证据。

### 第三阶段：最终命名、宿主边界与严格配置

- 顶层动态计算组件已迁至 `src/component/{backtest,datafeed,evaluator,logger,strategy,notifier,exporter,order,wallet}`；部署域已统一为
  `src/controller`、`IController`、`XxxController`；通知域已统一为 `src/component/notifier`。旧 crate 路径和旧模块导出不保留。
- 命令源码已从 `cmd/` 迁至 `startup/`，文件直接使用领域名；所有入口先导入 `ApplicationContext` 再使用短类型名。
- `src/agents` 已改为 `src/api`，包含 A2A、HTTP 与 MCP 三个协议 crate。HTTP 使用 Axum 和 Authguard identity middleware；
  MCP 使用 `rust-mcp-sdk` 的 Streamable HTTP/Auth SPI，A2A 使用 Linux Foundation `a2a-lf`/`a2a-server-lf` JSON-RPC 与 Agent Card。
  两者都调用 Core Workflow Repository 提供 overview/events/start/stop，不生成虚假 tool、resource 或业务结果。
- `ApplicationContext::global().init()` 自动安装 Unix SIGTERM/Ctrl+C 关闭钩子并调用 `close()`；进程入口最后仍调用幂等
  `shutdown()`，保证正常返回和信号终止具有同一生命周期顺序。
- `utils` 中跨项目通用的 cgroup、memory、panic 能力已迁至 `common/context/src/util/`；原文件删除。
- Google Sheets 位于 `component/exporter/src/google/google_sheets.rs`，直接使用 `reqwest::Client` 调用 Sheets v4 HTTPS API，支持可选
  `proxy_url`；固定范围、幂等 key、认证 token 环境变量与失败传播不变。
- 配置只接受 `snake_case` 键和 canonical 枚举值，不保留旧字段、旧服务名、serde alias 或交易模式别名；Workflow 必须显式声明 `live|paper|backtest`，且与 Order adapter 完全一致。
- `types/src/modules` 和空 `types/src/api` 已删除，领域类型直接位于 `types/src/<domain>`。生产领域源码的 SQL、连接池、
  transaction 和 QueryBuilder 静态扫描为零；Inbox/Outbox 位于 Core Messager Repository。
- 与 166 个文件、20,014 行有效代码的九领域基线相比，当前生产源码为 122 个文件、16,124 行；其中冻结生产子树为
  36 个文件、3,285 行，非冻结部分为 86 个文件、12,839 行。共减少 44 个文件和 3,890 行有效代码。最大文件为
  `exchange/binance/binance.rs` 的 769 行有效代码，
  低于 800 行上限。除冻结的 `strategy/runner/src/python/worker/mod.rs` 外，`mod.rs` 均为轻量声明/导出。
- 非冻结源码不存在 `_factory.rs`、`runtime.rs`、`feed.rs`、`matching.rs`、`replay.rs` 或 `delivery.rs`。唯一保留的
  `ExchangeFactory` 符号由冻结的 `runner/src/python/python.rs` 直接引用，内部仍委托共享 `ApplicationContext`，不拥有第二套注册表。

当前验证证据：

```text
cargo check --workspace --all-targets
  PASS（2026-10-31，dev profile，包含全部 API/组件/平台服务与 tests/benches）

cargo test -p sigbot-core --test config_loader --no-fail-fast
  PASS（4 passed；严格 snake_case、unknown key、storage canonical section、环境覆盖）

cargo check -p sigbot-a2a
cargo check -p sigbot-mcp
cargo check -p sigbot-cmd
  PASS（官方 A2A/MCP 协议库与三协议 API 启动层联编）

cargo fmt --all -- --check
helm lint deploy/helm/sigbot
helm template ... --set server.protocols.mcp.enabled=true --set server.protocols.a2a.enabled=true
  PASS（MCP/A2A 独立端口、Authguard Gateway 路由和公开 Agent Card 路由均成功渲染）
```

冻结校验：SDK 与 runner Python 共 49 个冻结文件，文件集合无增减，迁移前后 SHA-256 全部一致。生产领域静态扫描未发现
`sqlx::query*`、`PgPool`、`QueryBuilder`、PostgreSQL transaction 或 SQL 字面量；数据库实现只存在于
`src/core/src/modules/<domain>/storage/`。该段记录的是第三阶段当时的历史状态；最新 Docker/K3s 权限和验证结果见第六阶段。

平台部署边界已落到 `cluster_info` 和 `wf_run.cluster_id`：Controller 是唯一动态部署 owner，按
`env/tenant_id/workflow_id` 创建九个数据面组件；API、Controller、Web 由 Helm 预部署。所有节点仍使用统一
MessageScope 与 Workflow topic 生成器隔离。

待完成门禁仅以本节后续追加的最新结果为准：完整 workspace UT、Clippy、Helm lint/render、Web Playwright 与 Docker/Kubernetes
E2E。执行 Kubernetes E2E 前固定设置 `KUBECONFIG=/etc/rancher/k3s/k3s.yaml`。

### 第四阶段：API/Handler/Repository 最终边界

- Core 顶层领域已收敛为 16 个具名模块，每个模块都只公开 `handler/`，并将
  `storage/{mod.rs,postgres.rs,sqlite.rs}` 保持为 crate-private。原顶层 `order_*`、`wallet_*`、`backtest_*`、
  `strategy_input` 仓储包改为父领域内部的具名私有包，避免把同一业务域伪装成数十个独立领域。
- Order、Wallet 与 Backtest 的大体量仓储按业务能力在父领域内部组织；每个仓储目录仍严格只有
  `mod.rs`、`postgres.rs`、`sqlite.rs`，PostgreSQL/SQLite 类型统一为 `XxxPostgresRepository` /
  `XxxSQLiteRepository`，单个仓储实现不超过 250 行有效代码。
- 删除重复 Portfolio 顶层模块，将一致性读取和组合发布合并到 Wallet Portfolio Repository；Strategy 只通过
  `WalletPortfolioHandler` 获取版本化组合，不依赖仓储类型。
- HTTP 路由实现全部位于 `src/api/http`，统一命名为 `XxxHttpApi`；A2A/MCP 只暴露 Workflow 观察及启停等必要能力，
  三种协议均调用同一个 `WorkflowHandler`，Core 中不存在 route/router 源码。
- 独立组件及三种 API 的生产源码均未引用 Core `storage` 或 `Repository`，也未出现 `sqlx::query*`、连接池或事务类型。
  组件与 API 只能调用 Handler；SQL 与事务原子性只由 Core Repository 持有。
- 通用 CRUD 契约迁入 `core/storage/storage.rs`，统一为 `IGenericRepository`，包含 `select`、`select_by`、`insert`、
  `update`、`upsert`、`delete_by_id` 等操作；`storage/mod.rs` 只保留模块声明和导出。旧 `IAsyncRepository`、
  `select_by_id` 及会在生产路径 panic 的空泛 Repository 实现已删除。
- Strategy runner 的 `checkpoint/postgres.rs` 已改为后端无关的 `checkpoint/checkpoint.rs`，组件只持有
  `StrategyHandler`；具体 PostgreSQL 实现继续封装在 Core Strategy Repository。
- 已将旧 Core HTTP 边界测试迁入 `src/api/http/tests/workflow/`，避免 Core 反向依赖 API，并保留真实 TCP、
  Authguard 签名上下文和 PostgreSQL 事务验收。

第四阶段静态结果：`cargo fmt --all -- --check` 与 `git diff --check` 通过；所有正式源码均低于 800 行有效代码，
Core Handler 低于 500 行，Core PostgreSQL/SQLite Repository 不超过 250 行。该阶段之后执行的全工作区最终编译和测试结果
记录在第五阶段。

### 第五阶段：最终验证结果与外部阻塞

最终本机验证结果如下：

```text
cargo test --workspace --no-run
  PASS（全部 1,431 个测试目标完成编译）

cargo test --workspace --no-fail-fast --quiet
  PASS（0 failed；需要 PostgreSQL、MinIO、Kubernetes 等外部设施的验收测试按既有约定保持 ignored）

cargo clippy --workspace --all-targets -- -D clippy::correctness
  PASS（correctness 级别 0 error）

cargo fmt --all -- --check
git diff --check
  PASS

Marketpulse runner --scenario structure --scenario deployment
  PASS（原独立 deploy/tests 已删除，验收已并入唯一 case runner）

e2e/runner.py --scenario database
  PASS（SQLite 幂等/漂移/Worker 外键及真实 PostgreSQL 钱包约束）

helm dependency list deploy/helm/sigbot
helm lint deploy/helm/sigbot
helm template（global 与 Aliyun 两套镜像源）
  PASS（仅 Helm 建议配置 chart icon，不影响 lint）

cd web && npm run build
  PASS

cd web && npm test
  PASS（真实 Chromium，2 passed，生成 6 张页面/工作流画布截图）

e2e/runner.py --scenario structure,schemas --skip-build
  PASS（报告：use-cases/marketpulse-autonomy-quantification/e2e/reports/20261031135100）
```

E2E 结构门禁按规范统计“剔除空白和注释后的有效代码行”，不再用物理行误判。最终扫描结果为：冻结目录 49/49
文件集合一致且 SHA-256 无变化；36 个 Core 仓储目录都严格只有 `mod.rs`、`postgres.rs`、`sqlite.rs`；Core route
目录和 Axum Router 为零；非测试的组件/API/平台服务中数据库类型、SQL、Repository 直接依赖均为零；正式源码最大为
`web/src/domains/workflows.tsx` 的 798 行有效代码，没有超过 800 行的文件。

Web 截图证据位于 `web/test-results/`，覆盖 Overview、市场行情、Workflow、数据源、交易所与真实工作流节点/连线画布。
这些测试使用本轮安装的 Chromium 141.0.7390.37，不是 DOM 静态快照或伪截图。

第五阶段结束时尚未把完整 Docker/Kubernetes E2E 标记为通过；其当时的宿主权限结论已经被第六阶段的重新配置取代。

### 第六阶段：远程 Worker、组件边界、单基线迁移与 Swagger

本阶段不保留旧部署入口或旧迁移兼容逻辑，完成以下收敛：

- Exporter、Notifier、Order 与 Wallet 均迁入 `src/component/`。九个数据面组件的 `server.rs` 只启动 Controller
  注入的单个 Workflow 节点配置，不再存在 `wf_service_binding` 或平台级 order/wallet Host。Helm 仅预部署
  API、Controller、Web 及控制面 PostgreSQL/Redis；数据面组件由 Controller 统一部署。
- Controller 变为唯一中央调度 provider。`CentralController` 持有 scheduler/reconciler；Docker 与 Kubernetes 类型仅是
  绑定到具体 `cluster_info` 的执行适配器，不再各自拥有 scheduler、ApplicationContext 生命周期或旧 `new()` 入口。
  `WorkerRouter` 根据数据库记录逐次连接任意远程 Docker/Kubernetes endpoint。
- 新增 Worker 领域并收敛到 Controller：`types/controller` 定义 provider、status 和带 `auth_type` 标签的认证配置；
  `core/modules/controller/{handler,storage}` 提供作用域校验与 PostgreSQL/SQLite Repository；HTTP 暴露 scoped list/save/delete/status。
  `auth_config` 只保存宿主 Secret 环境变量名，不持久化 JWT、证书或私钥正文。带 `runtime_type` 的
  `configuration` 分别保存 Docker network/env-file/log-volume 或 Kubernetes namespace/Secret/ConfigMap/ServiceAccount/
  egress/log-PVC 资源边界；Docker env-file 名必须带 tenant 前缀且只能从固定宿主目录读取。Worker 可绑定 tenant 或
  特定 Workflow，支持 priority、capacity、heartbeat 与 online/draining/offline/error 状态。
- `wf_run.cluster_id` 固定一次运行的计算目标。中央 Controller 从每条 Workflow 读取真实 scope，不使用进程级全局用户上下文；
  首次 claim 优先选择 Workflow 专属 Worker，其次选择同租户的共享 Worker，
  使用行锁、容量计数和 `SKIP LOCKED` 防止重复分配；重试和停止继续使用已绑定 Worker，不因状态切换而偷偷迁移。
- 所有迁移已归并到唯一基线 `migration/20261031000000/{postgres,sqlite}/init.{ddl,dml}.sql`。旧分散版本目录不保留；
  `migration/migrate.py` 对两个文件计算联合 SHA-256，PostgreSQL 使用事务、advisory lock 和 psql 条件执行，SQLite 使用
  `BEGIN IMMEDIATE`。迁移元数据表命名为 `mig_version`；业务表统一使用 `cfg_`、`wf_`、`ctl_`、`msg_`、
  `mkt_`、`eval_`、`bt_`、`ord_`、`wlt_`、`ntf_`、`exp_`、`log_` 内部运行时前缀；配置主表使用十领域 `*_info`（见 domain-model_ZH.md）。连接主表的 `configuration`
  约束业务 `kind`，`cluster_info.configuration` 约束 `runtime_type`；PostgreSQL 与 SQLite 都以复合外键禁止 Worker
  绑定其他租户或环境的 Workflow。
- 部署文件已收敛到根 `deploy/`，应用镜像 Dockerfile 位于 `deploy/docker/Dockerfile`；根 `static/`
  与旧根 `tests/` 已删除。Core 中旧静态页面嵌入和 RustEmbed handler 同步删除，Web 资源只归 `web`。
- Swagger 实现与本地 UI 压缩包位于 `src/api/http/src/swagger/`。逐项对比旧 GitHub 实现后，OpenAPI 恢复
  metadata、license、contact、external docs、
  context path、Authguard bearer security、当前 HTTP operation 及 Connection/Workflow/Worker/Notification schema；
  `utoipa-swagger-ui` 启用 `vendored` feature，并通过 workspace 本地 patch 直接嵌入已提交的 5.17.14 压缩包；Cargo 构建不再下载 Swagger UI。
- Helm/Compose 的迁移任务统一执行 `/app/migration/migrate.py --provider postgres`；Helm 平台 Deployment 使用迁移
  initContainer，并保持 Secret 与 ConfigMap 分离。Controller Helm 配置不再选择一个全局 Docker/Kubernetes provider，
  provider 由每条 `cluster_info` 记录决定。

本阶段已经取得的验证证据：

```text
cargo check -p sigbot-types -p sigbot-core -p sigbot-http -p sigbot-controller \
  -p sigbot-notifier -p sigbot-exporter --all-targets
  PASS

e2e/runner.py --scenario database
  PASS（SQLite 重复执行、checksum drift、Worker 类型配置与跨 scope 外键、真实 PostgreSQL 钱包约束）

PostgreSQL 16 临时实例：render 后执行基线两次
  PASS（第一次创建 37 张 public 表并登记 1 个 mig_version；第二次无重复 DDL）

helm lint deploy/helm/sigbot
helm template（Controller 与平台服务开启）
  PASS

docker compose config（显式临时 Secret/镜像配置）
  PASS
```

宿主权限已经就绪：agent 用户可访问 Podman 兼容 Docker socket；使用
`KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get nodes` 可读取本机 Ready 节点。构建期间曾发现根分区 97%，已仅删除一个
确认未被容器使用的 3.39 GB `<none>` TimescaleDB 孤立镜像，未清理任何正式命名镜像，空间恢复到约 13 GiB。
完整 Docker/Kubernetes E2E 仍必须在新镜像构建完成后运行，不能用旧容器报告替代；执行范围包括重复、乱序、重启、
回放、分片等价和真实 Playwright 截图。

### 第七阶段：真实 Docker/Kubernetes 验收与镜像源收尾

本阶段使用当前源码编译出的 `sigbot`、`sigbot-strategy-worker` 和带 Cargo 依赖证明的集成测试目标执行真实环境验收，
没有使用旧容器报告替代。为保护宿主机，在 `target/` 超过安全增长预期、根盘接近 kubelet 阈值时，仅保留验收二进制后
执行 `cargo clean`；未删除正式命名镜像、数据库卷或验收报告。

- Docker 全矩阵报告：
  `use-cases/marketpulse-autonomy-quantification/e2e/reports/20261031230803/`。structure、schema、infra、ACL、数据库、
  三种外部边界、真实 Rust contract、Datafeed/Evaluator/Strategy/Order/Wallet、ExecutionGroup、恢复、确定性回放、Kafka、
  S3 日志与 Playwright Web 截图全部通过。ACL 场景实际重启 EMQX 并验证 QoS 1 durable session 补放；回放执行两轮
  592 步并比较确定性结果。
- Kubernetes 报告：
  `use-cases/marketpulse-autonomy-quantification/e2e/reports/20261101001131/`。controller 使用本机 k3s 真实创建 Datafeed 与
  Strategy Deployment、NetworkPolicy 和 Service，验证重复 apply 不替换实例、仅更新变更节点、Pod 删除后健康恢复、
  检查点不回退、停止幂等，以及拒绝接管非本 controller 资源。测试执行 1 个 required ignored case，结果 1 passed、
  0 failed，耗时 45.71 秒。
- 修复真实验收暴露的问题：历史回放 UNION 的 UUID/text 类型冲突、Workflow 服务绑定缺失、ExecutionGroup wallet/account
  混用、报告秒级目录冲突、Podman Compose 容器名假设、OCI 临时包清理、配置中的非 canonical pgvector 枚举值、
  Kubernetes worker 的 ServiceAccount 与 per-worker egress 配置归属。Core 原先未接入的 config/context/mgmt 子目录测试
  已加入 `tests/integration.rs`，不再是孤儿测试。
- Core integration 使用 dev profile 实际执行：7 passed、0 failed、12 ignored；ignored 项均需要真实 Redis/PostgreSQL，
  已由 Docker E2E 的对应场景覆盖。迁移 4 个测试、镜像 catalog 6 个测试、CI/release 2 个测试全部通过。
- Helm dependency/lint 及 global/aliyun 双源 render 通过。统一镜像 catalog 会在 `IMAGE_SOURCE=aliyun` 时拒绝任何非
  `aliyuncs.com` 地址；Makefile 以 Docker Registry HTTP 200/401 判断非 GFW，否则选择阿里云源。EMQX 使用截至
  2026-11-01 官方最新发布版 `6.3.2`，Docker 与 Helm 默认开启 durable sessions、7 天消息保留/会话过期并挂载持久卷；
  E2E 已验证 broker 重启后的持久会话。
- 已尝试按 `/home/agent/flowgent/deploy/docker/mirror-pull.sh` 的跳板流程补齐远端镜像，但跳板机 ED25519 host key 与本机
  已信任记录不一致，因此未绕过校验。当前 EMQX、Kafka、Python 的阿里云 manifest 可读；MinIO E2E 使用经官方 SHA-256
  校验的本地 binary carrier，TimescaleDB 使用官方 digest。MinIO/TimescaleDB 的新 ACR 仓库远端发布仍需由仓库管理员
  确认新的跳板机指纹或预创建对应 ACR repository；代码中的双地址选择与拒绝回退门禁已经完成，但这两项不能伪报为
  已完成远端发布。

最终静态复核：生产组件、Exchange、Messager、Controller、Order、Wallet 中没有 SQL/连接池实现；SQL 仅存在于 Core
Repository。非冻结源码不存在 `_factory.rs`、`*_starter.rs` 及被禁泛化文件名；最大正式 Rust 文件分别为 Binance
775 行有效代码和 IBKR 690 行有效代码，均低于 800 行上限。多余的空间隔离字段、旧数据库配置名、旧系统模块、旧通知/
部署模块均已删除，文档和 E2E scope 只保留 `env/tenant_id/workflow_id`。

### 第八阶段：中央 Secret、管理宿主与模块化可观测性收敛

- Messager 配置不再接受 `mqtt_password_env` 或内嵌密码，只保存稳定的 `mqtt_password_secret`；MQTT、交易所、LLM、
  Google Sheets、Notifier、S3 及 Controller worker 认证均通过短名 `get_config().secrets.require()` 读取中央配置。
  Compose、Helm 与 Marketpulse 进程边界只负责把宿主 Secret 映射为 `SIGBOT__SECRETS__VALUES__*`，领域组件不直接读取环境变量。
- `src/cmd/src/startup/internal/management_server.rs` 已收敛为 `management.rs`。全部 12 个启动宿主结构体显式持有
  `management: SigbotManagementStarter`；管理端关闭时仍由同一对象等待终止信号，确保 `ApplicationContext::shutdown()`
  按统一顺序关闭 provider。Banner 不再读取配置路径环境变量，也不在 verbose 输出中序列化 Secret。
- Helm 模板按 `api/controller/web/platform/middleware/observability/gateway` 分层；PostgreSQL 与 Redis 继续细分自己的
  service、statefulset 和 storage。`bootstrap/` 以同一 schema 支持 Kubernetes、AWS、GCP、Vault Secret 同步，任一
  bootstrap 失败都会阻止安装。Grafana sidecar ConfigMap 由一份模板生成 backtest、datafeed、evaluator、exporter、
  logger、notifier、strategy、controller、order、wallet 共 10 个 dashboard；ServiceMonitor 保持显式可选。
- `migration/tests/` 与 `deploy/tests/` 已删除。迁移幂等、checksum drift、Worker scope 外键及钱包账本约束统一由
  `use-cases/marketpulse-autonomy-quantification/e2e/verifier/infra/s04_database.py` 在真实 E2E 环境验证；唯一入口仍是 case
  内的 `runner.py`。`types/log` 同步对齐为 `types/logger`，未使用的 Snowflake ID 实现删除，所有持久实体继续使用 UUIDv7。
- 生产源码 panic 占位复核已收敛：Memory cache 内部改用原始字节，修复 `from_utf8_lossy` 破坏 bit offset 的问题；实现与
  Redis 一致的 `set` 秒级 TTL 和 `expire` 毫秒级 TTL，并以版本令牌防止旧定时任务删除覆盖后的新值。原内联测试已迁移到
  `tests/cache/memory_test.rs`。IBKR 股票订单接口复用统一订单查询；Hyperliquid 旧接口不再 `unimplemented!()`，而是返回
  可恢复错误并明确路由到现有 scoped execution/`MarketExchanges` 实现。Strategy SDK 与 Python 目录继续保持冻结未改动。

本阶段当前证据：

```text
cargo check --locked（Core、Types、Messager、Controller、Exchange、Cmd 及公共库）
  PASS

cargo test --locked -p sigbot-types
  PASS（3 个模块单元测试 + 11 个运行时契约测试；0 failed）

cargo test --locked -p sigbot-messager --test subscription_lifecycle --test reply_pairing
  PASS（2 passed；验证同 topic 多 consumer 生命周期与不落库的 requestSpanId 临时配对）

cargo test --locked -p sigbot-core --test integration cache::memory
  PASS（6 passed；验证原始 bit、字符串/Hash、删除、显式过期、缺失键和覆盖竞争）

cargo check --locked -p sigbot-exchange
  PASS

e2e/runner.py --scenario database --reuse-environment --reuse-build
  PASS（报告：use-cases/marketpulse-autonomy-quantification/e2e/reports/20261101053351）

e2e/runner.py --scenario structure,deployment --reuse-environment --reuse-build
  PASS（报告：use-cases/marketpulse-autonomy-quantification/e2e/reports/20261101054134）

e2e/runner.py --scenario structure --skip-build --reuse-environment（最终冻结哈希/边界复核）
  PASS（报告：use-cases/marketpulse-autonomy-quantification/e2e/reports/20261101063431）

helm lint deploy/helm/sigbot
helm template（controller + platform services）
  PASS

cd web && npm run build
PLAYWRIGHT_CHROMIUM_EXECUTABLE=/tmp/sigbot-chromium-1194/chrome-linux/chrome npm test
  PASS（真实 Chromium 141.0.7390.37，2 passed；生成 Overview、行情、Workflow、数据源、交易所和工作流画布 6 张截图）

cargo fmt --all -- --check
git diff --check
python3 -m compileall（Marketpulse E2E 与 migration）
make lint-helm
  PASS
```

最终生产源码扫描未发现组件、API、Controller、Order、Wallet、Messager、Exchange 绕过 Core Repository 直接执行 SQL，
也未发现这些模块直接读取环境变量；Controller 计划中的 `SIGBOT__*` 字符串仅用于向隔离 worker 注入中央配置键。
测试夹具可从验收宿主读取 endpoint、临时凭据和数据库地址，但不属于生产配置路径。全部正式 Rust/TypeScript/Python
源码仍满足 800 行有效代码上限，Core Handler/Repository 同时满足更严格的 500/250 行上限。

### 第九阶段：九组件数据面与租户中间件实际调度

- 平台面只保留 API、Controller、Web。API 负责租户业务绑定、Workflow、Worker 与连接等配置 CRUD；Controller 负责
  Backtest、Datafeed、Evaluator、Strategy、Logger、Exporter、Notifier、Order、Wallet 九个组件的启动运行生命周期。
  数据面九组件分别异步 pub/sub 执行 Workflow 中不同类型的节点。
- `cluster_info.middleware` 使用严格类型保存 Redis、pgvector、TimescaleDB、EMQX 的 `image/resources`，未知字段、空镜像、
  `latest` 标签与越界资源在 Handler 层拒绝。Web 同时提供表单和 YAML 编辑；两种模式提交同一个强类型 API，不接受任意
  PodSpec、hostPath、特权、安全上下文或宿主网络片段。
- Controller 在部署 Workflow 节点前执行 Worker 级中间件预备：Kubernetes 以 Service/StatefulSet/PVC 调度，Docker 以
  具名 Container/Volume 调度；资源均带 `worker-target` ownership label，拒绝接管外部对象。Redis 固定六节点（三主三从）
  拓扑，四类中间件全部就绪后才按 subscriber-first 顺序启动组件。禁用配置会停止工作负载但保留 PVC/Volume，避免不可恢复
  的数据删除；停止单个 Workflow 不删除租户共享中间件。
- Worker 选定后，Controller 才把 Redis、TimescaleDB、pgvector 和 EMQX 的稳定服务地址注入组件配置；Secret 值继续只从
  Kubernetes runtime Secret 或宿主 owner-only env-file 读取。Helm 的 Controller middleware 值是 API 返回给 Web 的唯一默认
  image/resource catalog，`deploy/images.py` 在 CN 模式将同一值覆盖为 aliyuncs.com 地址，不维护第二套目录或 YAML。
- `src/types/src/logger` 是唯一日志类型目录。S3 不提供对象内 append：Logger 先把 append 写入本地 SQLite WAL 并在
  `FULL` 同步提交后 ACK；归档任务再按 seq 创建不可变对象，`PutMode::Create` 冲突时读取并校验 SHA-256；索引提交成功后才
  清除本地 payload。读取通过索引游标拼接分块，因此逻辑上是 append stream，物理上没有修改既有 S3 object。

本阶段已执行的验证：Controller/Core 测试目标编译检查通过；Helm lint 及 global/aliyun 单 values override 渲染通过，渲染结果
包含六副本 Redis Cluster 与两套 registry 地址；Web TypeScript/Vite production build 通过。完整 Docker/Kubernetes E2E 仍以
Marketpulse 用例新报告为最终证据，不能沿用第七阶段旧镜像的报告冒充本阶段运行结果。

### 第十阶段：最终结构、IAM 与发布边界复核

- Helm 默认启用 Authguard，通过 `charts/middleware` 包装子 Chart，使
  `middleware.authguard` 的值真正传递到 Authguard；根 `values.yaml` 仅保留
  `global/sigbot/middleware/secrets` 四层。Bootstrap 文件统一为
  `setup-secret*.sh`，日志前缀为 `[sigbot-bootstrap]`，并恢复可执行权限。
- Authguard 继续独占用户、Tenant、Membership、Role、Permission 与 IAM 存储；Sigbot
  Migration 不含本地 IAM 表。Sigbot 控制面 Postgres/Redis 只保存业务配置与缓存，API
  不维护全局当前用户。现行 API/IAM/Logger 文档已删除旧 `core/sys`、本地 IAM 和
  API 内嵌 Controller 的描述。
- Core 的 Logger Repository 统一命名为 `LoggerPostgresRepository` 与
  `LoggerSQLiteRepository`；Strategy 补齐 `storage/sqlite.rs` 适配边界。九组件、
  Exchange、Messager、Controller 与 API 的生产源码无直写 SQL，数据库语句只在 Core
  Repository。
- Release 生成的 `dist/*.tgz` 不再作为源码提交；`dist/` 被忽略，合并发布任务仍会重新
  打包、推送并拉回校验 Helm OCI Artifact。

最终增量证据：

```text
cargo check -p sigbot-core -p sigbot-controller -p sigbot-http --tests
  PASS

Controller worker_lifecycle
  PASS（7 passed、0 failed、1 个真实 K3s 外部生命周期用例按条件 ignored；覆盖宿主
  Secret 引用、租户中间件端点绑定、幂等清单、归属、持久日志卷和 MQTT 会话）

cargo check -p sigbot-core --tests（Repository 最终命名复核）
  PASS（无项目源码 warning）

Marketpulse structure + deployment
  PASS（reports/20261101081040）

Marketpulse schemas
  PASS（reports/20261101081049）

Helm lint + default global/aliyun render
  PASS（Authguard 默认启用；Aliyun render 的全部镜像均为 aliyuncs.com）

CI/release Python tests
  PASS（2 passed）
```
