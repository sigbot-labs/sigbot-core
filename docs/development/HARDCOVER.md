# Sigbot Core 编码代理交接手册

> 快照日期：2026-11-01（Asia/Shanghai）  
> 基准分支：`dev`  
> 基准提交：`6ee0d535de5d`（`Refactor runtime into provider-based domain components`）  
> 本文用途：新机器或新编码代理接手时的事实入口。规范设计仍以
> [`docs/architecture/overview.md`](../architecture/overview.md) 及各领域 Overview 为准。

## 当前入口与 MCP 归档（2026-11-09）

内置 MCP server 已归档到 `.archived/20261109382032_builtin_mcp_server_replaced_by_mcpfather/`，包括最新未提交的 Starter 修改、crate、测试、旧文档以及 manifest/config/deploy 快照；SHA256SUMS 已核验搬移内容。沿用现有 .gitignore，归档仅在本地保存，不会自动入库。活动 workspace 不再构建 sigbot-mcp，CLI/standalone、YAML、Helm/Compose 中均无内置 MCP 服务入口。

MCP 改由 [mcpfather](../architecture/engine/mcp/overview_ZH.md) 从真实 HTTP OpenAPI 生成并独立部署，这是该项目的 Sigbot 接入案例。当前内置 API 只有 HTTP/A2A，启动目录为 startup/api/{http.rs,a2a.rs}，另有根 standalone.rs/controller.rs；12 个正式模块统一使用 --config/-c。Datafeed 的外部 MCP 客户端和相关 fixture 保留。

A2A 的 Agent Card、Extended Agent Card、协议路由和监听统一在 src/api/a2a/src/a2a.rs；domain/resource.rs 仅聚合/分发资源。Authguard、管理端、Provider 生命周期与九组件编排继续复用已有实现。十领域模型见 [设计说明](../architecture/domain-model_ZH.md)。真实远端生命周期、OIDC 登录和交易 E2E 的历史证据与本轮归档/生成验证分别记录，不混算验收。

## 1. 先读结论

Sigbot 已经完成从单体/共享守护进程思路向“平台控制面 + 九个租户数据面组件”的主体
重构。当前正确架构是：

- 平台面：HTTP/A2A API（MCP 由 mcpfather 独立生成）、独立 Controller、Web。
- 数据面：Backtest、Datafeed、Evaluator、Exporter、Logger、Notifier、Order、
  Strategy、Wallet 九个动态组件。
- 进程内共享库：Messager、Exchange、Core Handler/Repository、Types、Config、Telemetry。
- IAM：Authguard 唯一拥有认证、用户、租户身份、会话、角色和授权。
- 持久层：业务 SQL 只允许出现在 `src/core/src/modules/<domain>/storage/`。
- 部署：Controller 根据 `cluster_info` 在 Docker/Kubernetes 上调和租户中间件与九组件。
- 测试：唯一 E2E 入口是
  `use-cases/marketpulse-autonomy-quantification/e2e/runner.py`。

不要恢复以下旧设计：本地 IAM 表、`core/sys`、全局当前用户、静态 Order/Wallet 平台
服务、API 内嵌 Controller、`internal/v1` topic、`space_id`、APPDB、组件直连 SQL、
业务模块直接读取环境变量、S3 对象原地 append。

## 2. 平台面与数据面

```text
Browser / external agents
        │
        ▼
Authguard Gateway + IAM
        │ verified RequestIdentity
        ▼
HTTP / A2A API ──► Core Handler ──► Core Repository ──► control-plane PG
        │                       │
        │ create immutable Run │
        ▼                       ▼
independent Controller ──► cluster_info (Docker or Kubernetes)
                               │
                    prepare scoped middleware
                    Redis / pgvector / TimescaleDB / EMQX
                               │
                    deploy nine Workflow components
                               │
                       MQTT v2 pub/sub graph
```

API 是经过 Authguard 授权的配置写入口，负责 Worker、Connection、Workflow 等 CRUD 和
Run 创建。Controller 使用平台 service principal 读取待处理 Run，通过 Core Repository
写 Worker/Run 生命周期状态。二者不是父子进程，也不能共享用户请求上下文。

数据面组件只执行不可变 Run 图中的节点。节点之间用 v2 MQTT topic 异步通信；组件不
读取或修改其他租户/Workflow 的配置。Messager 和 Exchange 随宿主链接，不是独立
Deployment。Logger 是九组件之一，不是平台共享守护进程。

## 3. 目录地图

| 目录 | 当前职责 |
| --- | --- |
| `src/api/http` | Axum API、Authguard identity、Swagger、DTO 转换 |
| `src/api/a2a` | A2A 协议 Adapter，不承载领域数据库逻辑 |
| `docs/architecture/engine/mcp` | mcpfather 外部生成接入；旧内置 MCP 实现已归档 |
| `src/cmd/src/startup` | 进程入口；每个主结构体持有 `SigbotManagementStarter` |
| `src/cmd/src/startup/internal/management.rs` | 统一 panic hook、Provider context 初始化、APM、管理监听与退出清理；关闭管理端也保留基础初始化 |
| `src/controller` | 独立 Controller；WorkerPlan、Docker/Kubernetes Provider、中间件调和 |
| `src/component/*` | 九个租户数据面组件 |
| `src/core/src/modules/*/handler` | 领域用例编排、事务边界 |
| `src/core/src/modules/*/storage` | 唯一 SQL/Repository 所有者 |
| `src/exchange` | 市场/执行 Adapter 和统一 Instrument/precision/limit 边界 |
| `src/messager` | Local/MQTT transport、命名订阅、Inbox/Outbox relay |
| `src/types` | 跨 crate 契约；日志类型位于 `src/types/src/logger` |
| `src/core/src/config` | 统一配置入口和强类型模型 |
| `migration/20261031000000` | 唯一最终态 PostgreSQL/SQLite 基线 |
| `deploy/helm/sigbot` | 控制面 Helm、Authguard wrapper、Secret bootstrap、Grafana |
| `deploy/docker` | 本地控制面与 E2E 支撑 fixture，不是生产拓扑定义 |
| `web` | 平台 UI；Worker 表单/YAML 共用强类型 API |

## 4. 配置、Secret 与身份

业务代码使用顶部导入后的短名 `get_config()`。配置优先级：defaults `<` YAML `<`
`SIGBOT__A__B` 环境覆盖；标量支持 `${VARIABLE:default}` 插值，未知字段拒绝。

只有中央配置层和通用运行工具可以解析环境。组件、API、Controller、Order、Wallet、
Exchange、Messager 不得直接 `std::env::var`。需要新增 Secret 时：

1. 在中央 `SecretsProperties`/typed config 中定义稳定 key；
2. Helm/bootstrap 或远端 runtime Secret 注入值；
3. Worker/API DTO 只保存 Secret key 名；
4. 禁止日志打印 URI、token、密码、private key 或完整配置 Debug。

Authguard 是 IAM 唯一 owner。HTTP Adapter 验证官方 signed header/gRPC context 后创建
`RequestIdentity`，Handler 显式接收；禁止 thread-local/global current user。Sigbot 数据库
保存业务 `tenant_id UUID` 与 `tenant_info` 资料，不复制用户、角色、权限或 Authguard 的 IAM 租户生命周期。

## 5. 数据模型与唯一写入方

最终态迁移只有：

```text
migration/20261031000000/
├── postgres/{init.ddl.sql,init.dml.sql}
└── sqlite/{init.ddl.sql,init.dml.sql}
```

`migration/migrate.py` 通过 advisory transaction lock 与联合 SHA-256 防止并发和已应用
文件漂移。运行账号不得执行 DDL。当前项目不提供旧库升级链；接手者不要重新加入
`migration/tests`，迁移验收归 Marketpulse `database/schemas` 场景。

| 领域 | 表前缀/核心表 | 唯一写入方 |
| --- | --- | --- |
| 配置 | `tenant_info/datasource_info/exchange_info/strategy_info/notifier_info/exporter_info` | Core ResourceHandler/IGenericRepository |
| Workflow | `workflow_info/run/checkpoint/sequence/timer` | Core Workflow Repository；Controller 写生命周期 |
| Cluster | `cluster_info` | Core ResourceHandler/IGenericRepository；Controller 维护运行状态 |
| 消息 | `msg_inbox/outbox` | Core Messager Repository，领域事务调用 |
| 行情 | `mkt_event/delivery` | Datafeed Repository |
| 评估 | `eval_case/snapshot` | Evaluator Repository |
| 回测 | `bt_checkpoint/order` | Backtest Repository |
| 订单 | `ord_*` | Order Repository |
| 钱包 | `wlt_*` | Wallet Repository |
| 日志 | `log_chunk` | Logger Repository + immutable object store |
| 通知 | `notifier_info`、`ntf_delivery` | ResourceHandler 配置 CRUD、Notifier 投递事务 |
| 导出 | `exp_checkpoint` | Exporter Repository |

实体主键/相互依赖外键使用 UUIDv7。`node_id`、`seq`、`revision`、`line_no` 和交易所原生
ID 是业务坐标或外部事实标识，不要机械改 UUID。账本/Journal/Posting 与交易所观察事实
不可更新；更正使用新事实或反向分录。余额、仓位、净值是可重建投影。

## 6. 消息、幂等与追踪

Topic 必须由 `src/types/src/messager/topic.rs` 生成：

```text
internal/v2/{env}/{tenant_id}/workflow/{workflow_id}/...
internal/v2/{env}/{tenant_id}/account/{account_id}/...
```

Workflow 分支覆盖 feed、evaluation、strategy、snapshot/compute/deployment/notification/
export request/result、runtime/log event；Account 分支覆盖 order/execution event、wallet
reservation request/result 和 portfolio snapshot。

MQTT 使用 QoS 1、持久会话、命名订阅、真实 SUBACK/PUBACK 和手动消费 ACK。业务消费
必须先在同一事务完成 Inbox、领域事实/投影、Outbox，再 ACK。Outbox 补放保持
`run_id + node_id + seq`，递增 `attempt_no` 并创建新 span。

`run_id` 是业务幂等/关联 ID。W3C `traceparent` 只承担可观测性。`requestSpanId` 必须等于
producer span ID，只进入有界临时 ReplyPairingCache，响应回显后丢弃；它不属于
`DurableMessage`，不能落库或成为业务恢复键。禁止创建第二个 correlation ID。

## 7. 九组件状态

### 7.1 Datafeed

已实现：Twitter/TruthSocial REST、MCP、A2A 边界；Binance/Hyperliquid 公共行情；Fed/
Treasury RSS；事件/可得时间、revision、水位和 canonical delivery；LATEST/AS_OF 快照；
有界 history/window/group/aggregate 查询；Outbox 发布。

缺口：完整清算热力图、借贷/资金费率、多空比/量采集未形成全源生产管线；TruthSocial
没有可公开验证的官方 OpenAPI，当前 mock 明确是 Mastodon 兼容子集；更多交易所主动
快照与 source revision 语义待补。

### 7.2 Evaluator

已实现：旁路消费 Datafeed，按 `available_at` 防未来信息；LLM 五态概率、版本、有效期、
参数、事实引用和推断分离；snapshot/case 持久化和 MQTT 发布。

缺口：真实模型质量、概率校准、反馈闭环和 pgvector 案例检索质量未验收；当前完整 E2E
使用明确的 OpenAI mock，不证明收益或智能水平。

### 7.3 Strategy

已实现：`on_hyperparameters(ctx, update)`、`on_data(ctx, event)` 返回
`Effects{intents,requests,timers}`；只读上下文、事务草稿 state、单实例串行、checkpoint
fencing；Snapshot/Compute/Notification/Export 请求与结果；受限 Rust/Python worker；
异常回滚且禁止直下单。

缺口：Redis 热缓存、通用增量分片 worker、任意 Python 自动并行和所有模型的分片等价
尚未实现。Strategy SDK/Python 是用户冻结边界，除非需求明确，不要为了格式化或重命名
修改 `src/component/strategy/sdk` 和 runner 的 Python 目录。

### 7.4 Order

已实现：Intent → 持久行情/余额/仓位/精度/硬风控 → Wallet 预占 → prepared request →
Exchange；ACK/Execution 分离；Unknown 查询；venue observation；ExecutionGroup 与
reduce-only 补偿；Live/Paper/Backtest 模式校验。

缺口：真实多所部分成交、撤单竞态、跨所补偿、全部 venue 费用/结算事实未完成资金级
验收。当前同所模拟补偿不能标作跨所验收。

### 7.5 Wallet

已实现：Funding/Execution Inbox、借贷平衡 Journal/Posting、预占、余额/仓位、组合版本、
Outbox 同事务；执行 ID 去重；乱序终态等待实际成交；精确 quote amount 与成本恢复。

缺口：真实账户全量资金/转账、公司行动、衍生品保证金/强平、长期自动外部对账和完整
多币种估值未完成。

### 7.6 Backtest

已实现：冻结历史、虚拟时钟、固定事件次序/屏障、记录 AI 输出、确定性费用/滑点、限价
挂单后续 tick 撮合、重启去重和隔离回放。

缺口：订单簿排队优先级、真实深度/部分可用流动性、网络延迟与全部衍生品结算模型仍是
有限模拟；批量只用于预热/计算，不能越过逐事件 `on_data`。

### 7.7 Logger

已实现：Python stdout/stderr/异常和 Rust tracing 接入；SQLite WAL 本地耐久缓冲；S3/
MinIO 不可变 chunk；PG `log_chunk` 索引；重复/冲突哈希校验；cursor 读取和重启恢复。

缺口：尚未证明所有生产宿主都完整接入采集；跨 region 对象复制、长期保留/删除、灾难
恢复和大规模吞吐未验收。

### 7.8 Notifier

已实现：Email、Telegram、站内通知；严格模板；幂等 owner、有限重试、投递/已读状态；
Strategy typed request/result。

缺口：没有向真实 SMTP/Telegram 收件人完成生产凭据验收；第三方未知结果不能宣称
exactly-once；QQ 仅为保留设计。

### 7.9 Exporter

已实现：Kafka 幂等 producer、`acks=all`、稳定业务 key、有界 Producer Queue 和持久
Checkpoint；Google Sheets 固定 range PUT；Strategy typed request/result。Retry 依赖
At-least-once 重投并复用业务 key，当前没有独立 Durable Exporter Retry Scheduler。

缺口：真实 Google Sheets、Kafka 多 broker 故障切换未验收；Kafka ACK 与 PG commit
窗口仍要求下游按业务 key 去重。

## 8. Controller、Worker 与租户中间件

Worker API 保存 Docker/Kubernetes endpoint、Secret key 引用、运行配置、作用域、优先级、
容量和 `middleware`。四类中间件只接受 reviewed image 与有界 resources：Redis、
pgvector、TimescaleDB、EMQX。Web 的表单/YAML 模式共用同一 `SaveWorker` DTO，未知字段、
命令、env 和明文 Secret 被拒绝。

Controller 在组件前调和中间件，使用确定性名称、ownership labels 和 spec hash。Docker
需要预创建隔离网络与受控 env-file；Kubernetes 需要预创建 namespace、runtime Secret/
ConfigMap、service account 与日志 PVC。Controller 不创建远端 Secret 或 namespace。

当前最新中间件 reconciler 已通过 Rust 编译、WorkerPlan/manifest 和 `worker_lifecycle`
套件（7 passed、0 failed、1 个真实外部 K3s test ignored）。**尚未在最新源码上补跑四类
中间件的真实 K3s 创建、更新、恢复、停止全生命周期**。这是接手后的最高优先级之一。

## 9. Helm、Bootstrap 与 Grafana

`deploy/helm/sigbot/values.yaml` 只有四个顶层：

- `global`：镜像源、pull secret、调度与存储公共值；
- `sigbot`：原始 typed config、API、Controller、Web、monitoring；
- `middleware`：Authguard、控制面 PostgreSQL、控制面 Redis Cluster；
- `secrets`：统一基础 Secret 引用与 revision。

模板按 `api/controller/web/platform/middleware/gateway/observability` 分层。Authguard 通过
`middleware` wrapper subchart 注入，默认启用，独立拥有 IAM PostgreSQL/Redis Secret。
Sigbot 控制面 PostgreSQL/Redis 不与 Authguard 或租户数据面混库。

Bootstrap 统一使用：

```text
setup-secret.sh
setup-secret-k8s.sh
setup-secret-aws.sh
setup-secret-gcp.sh
setup-secret-vault.sh
sigbot-secrets.env.example
```

所有脚本先构造同一 canonical payload，再同步目标 Secret；失败必须中止安装。Grafana
模板生成 Controller 加九组件共十个 dashboard，避免复制十份近似 JSON。

## 10. S3 “append” 的准确语义

S3 通用 API 不支持对已有对象原地 append。Sigbot 实现的是“逻辑追加日志流”，物理上
每个 chunk 都是新对象：

1. Logger 先在本地 SQLite WAL 中同事务分配序列并保存 payload；使用 `FULL`
   durability 完成后，消费处理才允许返回 ACK。
2. 归档器按连续范围生成不可变 object key，使用 `PutMode::Create` 创建新对象。
3. key 已存在时不覆盖：读取现有对象并比较 SHA-256；一致视为幂等成功，不一致隔离并
   报错。
4. 对象确认后提交 PostgreSQL `log_chunk` 索引和 cursor，再释放本地 payload。
5. 读取端按索引 cursor 拼接多个 chunk；晚到序列不会因为 S3 listing 顺序被跳过。

因此 ACK 只承诺“已进入本地耐久 WAL”，不承诺“已上传 S3”；PG/S3 离线时可断点重试。
不要改成 multipart upload 后长期保留可变对象，也不要在对象上传前删除 WAL payload。

主要源码：

- `src/component/logger/src/s3/s3.rs`
- `src/core/src/modules/logger/storage/sqlite.rs`
- `src/core/src/modules/logger/storage/postgres.rs`
- `src/component/logger/src/s3/`

## 11. Web 与认证验收

Web 已有 Overview、行情、Workflow 画布、数据源、交易所、通知/个人菜单以及“租户运行
资源”入口。Worker 表单和 YAML 编辑器会解析并提交同一强类型结构。

公开/未登录 Playwright 已真实运行并截图；但完整 `web-full` 需要真实 Authguard Hosted
Login、同源 Gateway、Application 域名和 browser storage state。当前缺少这些外部条件，
不能用签名测试夹具或未认证截图替代。生产域名禁止猜测。

## 12. 构建与验证

### 12.1 资源与网络规则

- 每次 Rust build/test 前检查 `target/`；超过 10 GiB 必须先 `cargo clean`。
- 默认 `BUILD_JOBS=2`、`CARGO_INCREMENTAL=0`、debug profile symbols 关闭。
- GFW 下载失败时设置 `HTTPS_PROXY=http://127.0.0.1:8800`。
- K3s E2E 前设置 `KUBECONFIG=/etc/rancher/k3s/k3s.yaml`。
- 需要中间件/Token 的 E2E 可 `source ~/.wl4gshrc.sec`，但禁止读取或打印文件内容。
- 不清理正式命名镜像，特别是 `aliyuncs.com`；只可审慎处理 dangling `<none>` 镜像。

### 12.2 常用命令

```bash
make fmt
make lint
make test
make lint-helm
make e2e-docker E2E_ARGS='--rounds 2'
KUBECONFIG=/etc/rancher/k3s/k3s.yaml make e2e-k8s
```

唯一 case runner 支持 `--scenario` 多值、`--rounds`、`--deployer`、`--timeout`、
`--reuse-build`、`--reuse-environment`、`--skip-build` 和 `--stop-runtime`。不要新增根级
`use-cases/runner.py`、`deploy/tests` 或 `migration/tests`。

### 12.3 当前证据

| 证据 | 状态 | 说明 |
| --- | --- | --- |
| `20261031230803` Docker | 26/26 PASS | 完整主链历史基线，早于最新中间件 reconciler |
| `20261101001131` Kubernetes | 2/2 PASS | prepare/deployment 历史基线，不是最新四中间件全生命周期 |
| `20261101053351` database | PASS | 最终态 PG/SQLite 迁移与约束 |
| `20261101081040` structure/deployment | 2/2 PASS | 最新静态结构与 Helm/部署门禁 |
| `20261101081049` schemas | PASS | schema 契约 |
| `20261101081747` structure | PASS | 最新结构复核 |
| `20261101125053` structure/deployment | 2/2 PASS | 本次交接后的目录、Compose、Helm 与双镜像源复核 |
| Controller `worker_lifecycle` | 7 PASS, 1 ignored | 最新逻辑/清单；ignored 是真实外部 K3s |

报告位于 `use-cases/marketpulse-autonomy-quantification/e2e/reports/`。只统计实际执行的
scenario；ignored、mock、静态 render 和旧二进制不能描述成最新真实验收。

## 13. 已知问题与外部阻塞

### P0：发布前必须完成

1. 在最新源码和产品镜像上运行真实 K3s：四类租户中间件创建/更新/Pod/PVC 恢复/停止、
   九组件至少一条完整 Run、owner 拒绝和 Secret rotation。
2. 提供真实 Authguard Application/Gateway 域名、TLS、回跳路径和浏览器会话，完成
   `web-full` 的登录后 Worker/Workflow 保存、启动、通知和 trace 查询。
3. 使用生产候选最小权限 DB/Secret/RBAC 执行备份恢复和故障演练；当前只有空库最终态
   迁移，不承担旧库升级。

### P1：金融正确性与外部系统

1. 真实 Binance/IBKR/Polymarket 沙盒或受控账户：Unknown、撤单竞态、部分成交、费用、
   跨所 ExecutionGroup 补偿和 Wallet 对账。
2. 真实 SMTP、Telegram、Google Sheets 与 Kafka 多 broker 故障切换。
3. 完整账户资金/转账/公司行动/保证金/强平事实和长期对账。
4. 获得可验证的 TruthSocial 官方 OpenAPI；否则继续明确标注兼容子集。

### P2：规模与功能增强

1. Datafeed 补清算热力图、资金费率、借贷、多空比/量的生产级采集与 UI 数据。
2. Evaluator 建立真实案例反馈、pgvector 召回质量与概率校准评估。
3. Strategy/Compute 增加 Redis 热缓存、增量状态、分片调度和逐模型等价测试。
4. Logger 增加跨 region 复制、retention、容量/背压与全宿主接入审计。
5. 完整组合估值、NAV、FX/多资产价格元数据。

### 外部发布阻塞

- MinIO/TimescaleDB Aliyun mirror 的发布验证曾被跳板机 host key 变化/仓库权限阻塞。
  当前目标 IP 是 `47.84.96.66`，但必须由运维确认新 host key；禁止绕过 SSH 校验。
- GitHub Actions、GHCR、OCI chart、Release 权限需要真实远端 run；本地脚本通过不等同于
  发布成功。

## 14. 安全续作顺序

新编码代理建议按以下顺序开始：

1. `git status --short`，确认当前用户改动，不覆盖或 reset。
2. 阅读根 `AGENTS.md`、本文件、架构总览和目标领域 Overview。
3. 检查主机 CPU/内存/磁盘/IO 和 `target/` 大小。
4. 只读核对目标代码与 Core Repository；先证明缺口，再修改。
5. 保持九组件、独立 Controller、单一配置、Authguard、v2 topic 和唯一写入方边界。
6. 修改源码时同步外置镜像测试、迁移/Helm/Web/文档；不要只加未接线的类型。
7. 先跑最小定向测试，再跑 Marketpulse scenario；明确记录 mock/ignored/真实外部边界。
8. 完成后更新本文件的“当前证据/已知问题”，但不要把执行计划写成已完成事实。

## 15. Definition of Done

一次后续变更只有同时满足以下条件才算完成：

- 目录和依赖方向符合平台面/九组件/Core Repository 边界；
- 生产文件不超过 800 行，Core Handler/Storage 不超过 500/250 行；
- 未新增重复 DTO、第二配置入口、自由 SQL、全局用户或第二 correlation ID；
- Secret、日志、错误信息不泄露凭据；
- Rust/Web/Python/Helm 格式、lint 和定向测试通过；
- 对应 Docker/Kubernetes/外部系统真实场景实际执行，或明确标注未执行；
- 新报告保留实际命令、hash、截图和失败证据；
- 代码、迁移、部署、Grafana、UI 和现行文档同步；
- `git diff --check` 通过，生成物、cache、`.venv`、Secret 和 release tgz 未提交。

## 16. 本次交接变更说明

本交接审计以基准提交为起点，并包含尚未提交的文档校准和一个最小部署修正：根 Compose
不再静态启动 Order/Wallet，避免与 Controller 动态部署的同一九组件产生重复 consumer。
继续工作前以 `git status` 为准，保留这些改动；不要用 destructive reset 清除用户工作树。
