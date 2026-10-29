# Runtime v2 交付与验收矩阵

核对日期：2026-10-29（报告目录使用 UTC 时间）。此表按用户的 14 项重构要求及新增 E2E 要求记录当前源码和已执行证据；“已有实现”不自动等于“已完成生产验收”。现有 crate、领域目录和服务入口保留，Messager、Exchange、Logservice 作为共享组件嵌入宿主。表映射、迁移步骤和运行配置见 [迁移说明](runtime-v2-migration_ZH.md)。

## 证据范围

| 编号 | 已执行证据 | 可以证明的范围 |
| --- | --- | --- |
| E1 | [20261028153220 汇总](../../use-cases/marketpulse-autonomy-quantification/e2e/reports/20261028153220/summary.md)：22 个场景，两轮 44/44 PASS，18 张真实 Chromium 截图 | Rust 主链、认证 EMQX、PG、MinIO、Python、恢复、回放、同所双账户补偿；新增真实 Binance/Hyperliquid 主动快照、Workflow owner 与实际 Kafka 验收。当前主链数据库为普通 PG。 |
| E2 | [20261028153532 汇总](../../use-cases/marketpulse-autonomy-quantification/e2e/reports/20261028153532/summary.md)：Kubernetes 准备及真实部署 2/2 PASS | 最新二进制通过默认源码新鲜度检查；实际 Datafeed/Strategy Pod、6 条来源投递及 6 次 Python 回调、幂等更新、Pod 恢复、停止和拒绝接管未管理对象。载体为本机 ABI 匹配的调试镜像。 |
| E3 | [20261028153620 汇总](../../use-cases/marketpulse-autonomy-quantification/e2e/reports/20261028153620/summary.md)：结构与非空旧库升级两轮 4/4 PASS | 每轮 15 项检查：4 份旧基表、合法历史记录、21 份完整迁移、UUID Workflow/Run 映射、金融表保留、精度/FK/不可变约束和二次幂等；临时数据库已清理。 |
| E4 | [20261028160052 双扩展验收](../../use-cases/marketpulse-autonomy-quantification/e2e/reports/20261028160052/summary.md)：1 个场景、2 项实际 Rust 测试通过 | 独立 TimescaleDB 2.31.0 + pgvector 0.8.7；显式 preload/owner bootstrap、21 份迁移、实际 vector 距离和 7 天/3 天/25 小时/365 天分桶等价、迟到修订及未来排除。测试容器/卷已清理，主链实例未替换。 |
| R1 | [87 项测试的逐目标证据](../../use-cases/marketpulse-autonomy-quantification/e2e/reports/20261028153220/verification/rust-results.json)及[初次执行日志](../../use-cases/marketpulse-autonomy-quantification/e2e/reports/20261028153220/verification/rust-initial-run.log) | 31 个目标均有成功证据。初次一项旧 MQTT 夹具 SUBACK 超时，已保留失败，并以同二进制通过 [case 凭据重跑](../../use-cases/marketpulse-autonomy-quantification/e2e/reports/20261028153049/summary.md)；Kubernetes 测试由 E2 执行。后续 Timescale origin 与日志修复另行验证。 |
| B1 | [认证 UI 未通过记录](../../use-cases/marketpulse-autonomy-quantification/e2e/reports/20261028104434/web-full/round-1.json) | 缺少真实 Authguard 浏览器会话；该项保持 FAIL，不以未认证页面截图或签名上下文夹具替代。 |

E1/E2/E3/E4 报告含场景明细、实际执行命令、部分测试二进制 SHA-256 和截图。报告目录通常不纳入 Git，交付时应连同证据一起归档；`/tmp` 日志也是本机临时证据，不能代替已发布的 CI 工件。失败报告保留，不因后续修复而改写。

## 一、14 项重构要求

| 项目 | 当前实际实现 | 实际验收 | 尚未覆盖或明确限制 |
| --- | --- | --- | --- |
| **1. 数据库与唯一写入方** | 保留钱包、旧账本、余额、仓位、净值表；修复钱包 FK；统一 `s_workflow` 与运行表并保存旧 UUID 映射。新增身份绑定、连接、Inbox/Outbox、序列、检查点、隔离原因、预占、双分录账本、行情投递映射、评估、订单执行及观察游标。账本以约束/触发器禁止改写并要求每资产平衡；余额与仓位可由事实及剩余预占重建。 | E1 `database` 验证真实 PG 的不可变 journal/posting、借贷平衡和非空 journal；`pipeline/recovery` 检查真实余额及执行入账。迁移记录覆盖当前 20261028-1 至 -17；E3 验证非空旧数据升级；R1 验证精确金额、成本、PnL 和业务隔离。 | 已验证合成合法历史记录的升级；尚未完成生产全量历史的升级、回退和灾备演练。领域数据库账号的实际最小授权仍须部署配置，代码 owner 不替代数据库权限。v2 分布式事务使用 PG；不宣称 SQLite/Mongo 等价。完整多币种估值/NAV 持续计算未验收，不能直接把不同资产相加。 |
| **2. sys / Authguard / Helm** | 接入 Authguard 官方 Rust adapter 的请求身份及授权上下文，保留业务租户/空间绑定；移除本地重复用户权限 CRUD 与旧认证实现，不采用全局当前用户。Helm 引用 Authguard OCI 子 chart，配置与 Secret 引用分离。 | R1 `authenticated_workflow_api` 使用官方签名上下文、实际 TCP HTTP 和 PG，验证未认证、伪造上下文、跨租户/空间、deny、Workflow 保存与运行状态事务。 | 签名发行方在测试中是明确夹具，不是 Hosted Login。真实 Authguard Application/Gateway 域名、TLS、登录回调、浏览器会话及授权 UI 全链路未通过；B1 明确失败。需要实际域名与有效会话后继续 `web-full`。 |
| **3. Messager** | 唯一 v2 topic 枚举覆盖规定的 17 个 Workflow 后缀与 5 个 Account 后缀；统一 scope/node/run/seq/attempt/时间/traceparent。QoS1、持久会话、真实 SUBACK/PUBACK、手动消费 ACK、事务 Inbox/Outbox、重连、背压、补放。重试保留业务身份并更新 attempt/span。`requestSpanId` 仅在有界 TTL 缓存临时配对，不入库。Strategy 对其他目标、边及 run 的广播持久隔离后 ACK。 | E1 `acl` 使用实际 EMQX MQTT5 验证非法凭据及跨 env/tenant/space 发布、订阅拒绝；`rust-messaging` 验证真实应用补放、attempt、新 span、去重与隔离。R1 验证临时配对和三类 Strategy 路由；E1 `recovery` 验证真实消息重投。 | 尚未执行 broker 进程/磁盘灾难故障、服务角色细粒度 ACL 和持续高负载背压验收。Local 内存客户端不具备 MQTT 等价的持久恢复/广播保证。临时配对缓存重启后允许丢失，恢复不能依赖它。 |
| **4. Exchange** | `IExecutionAdapter` 统一 Live/Paper/Simulated；Binance、IBKR、Polymarket 使用官方/成熟 SDK。统一 `InstrumentSpec`、账户/scope 隔离、精度与限流；网络提交前保存不可改写的加密 prepared submission。成交观察携真实费用与可选实际 `quote_quantity`，Order 同事务保存观察、游标及 Outbox。Polymarket 校验 finalized 链上回执、归属与实际现金/费用。 | R1 包含 SDK 签名体往返、prepared payload 隔离、IBKR 时区、真实 PG venue 租约/游标/重复成交测试，以及实际 Alloy HTTP 客户端对明确 RPC 边界夹具的结算验证。E1 采集真实公开 Binance 行情，执行用产品 SimulatedExchange。 | 未使用真实资金向 Binance/IBKR/Polymarket 下单，未完成交易所实盘到账与费率对账。RPC 夹具测试不证明真实链上成交。Binance/Hyperliquid 主动快照已验证；尚不能宣称 IBKR/Polymarket 公共行情及所有产品类别已完整接通；跨所实际财务交易未验收。 |
| **5. Logservice / MinIO** | 共享 append/read；SQLite WAL FULL 持久缓冲、S3 条件创建不可变对象、PG 追加游标索引；支持去重、校验、断点重试和晚到日志读取。Python stdout/stderr/异常由 Rust 宿主采集。Helm 增加 MinIO；Deployer 要求专属持久卷，禁止用临时盘承诺持久 ACK。 | E1 `logs` 验证真实 Python stdout → Rust WAL → MinIO → PG，核对对象哈希；R1 `archive_persistence` 验证重启、重复、晚到游标及隔离；worker 测试验证异常不产生可提交状态。 | 尚未自动归档所有 Rust `tracing` 日志；共享 append 能力不等于全局采集接线完成。单宿主持久盘损毁、S3 多副本灾备和生产桶保留策略未验收。当前 MinIO 实例使用经官方哈希核验的真实二进制，不能据此宣称该镜像部署已验收。 |
| **6. Deployer / 宿主隔离** | API 内嵌 controller，Docker/Bollard、Kubernetes/kube provider；按不可变 run snapshot 生成单 owner worker。幂等命名/spec hash、更新/停止、健康重启、资源/PID/网络约束、Secret 注入、run 独立 MQTT 会话和持久日志卷。TimescaleDB/pgvector 属于同一 PG 扩展。Rust/Python worker 清空环境、关闭继承 FD、Landlock/seccomp、时间与资源限制。 | R1 6 项 WorkerPlan 测试验证 scope 覆盖防护、边路由、Secret、manifest 单 owner、会话与持久卷约束；2 项真实 worker 测试验证网络/宿主文件限制及异常回滚。E1 确实运行独立 Rust worker。 | E1 主链通过产品 CLI 启动，E2 已实测产品 Kubernetes provider 的部署/更新/恢复/停止；API 内嵌 controller 的完整调度竞争及 Docker provider 生命周期尚未实测。Docker egress 防火墙、Kubelet PID 策略及生产 Secret/PVC 需要实际部署。 |
| **7. Notification 重命名与投递** | 统一 `src/notification` 引用；Email、Telegram、站内通知 provider，版本模板、范围化收件人、持久投递状态和有限重试。Strategy 可通过 typed Effects 发送通知请求，结果回到 `on_data`。 | R1 真实 PG 测试验证站内投递、重复、scope、单 owner、崩溃恢复及严格模板；`service_effects` 验证通知请求与 state 原子提交和结果回调。E1 验证铃铛 UI 可操作。 | 尚未向真实 SMTP/Telegram 收件人执行投递验收；第三方未知结果不能宣称 exactly-once。真实开平仓/爆仓通知在已登录个人中心的完整展示未通过 `web-full`。 |
| **8. Datafeed** | 规范 DatafeedEvent 来源、外部事实 ID、事件/可得时间、外部序列、显式 revision、水位与 canonical delivery。Twitter/TruthSocial 的 REST、MCP、A2A，Binance 公共行情，FED/TREASURY 官方 RSS；历史有限查询、LATEST/AS_OF 持久事实快照、有效期/时差验证与查询结果消息。 | E1 外部 REST/MCP/A2A/WS 对照、实际 Rust 三种 ingestion transport、PG 源 revision/迟到/未来过滤、快照和 compute 回调；`rust-policy` 实际只读访问官方 Fed/Treasury RSS。 | 已通过 Binance/Hyperliquid SDK 主动 LATEST 的实际 API/PG 验收；AS_OF 保持持久历史查询，来源品种必须显式映射，不保证同刻原子快照。两所清算热力图、借贷/资金费率、多空比/量的完整采集管线未实现或未验收。Treasury 当前为发行公告 RSS，不是 Treasury 所有新闻政策。源无显式 revision 时不根据抓取时间造版本。 |
| **9. Evaluator** | 异步旁路消费行情/新闻；查询可得时间内的事实和案例，调用 LLM，发布趋势/震荡/企稳/压制/未知五态概率、参数、版本、有效期与案例引用。源事实和模型推断分开存储；引用必须来自本次输入，禁止未来信息和交易工具。模型完成后的实际时间作为输出可得时间。 | E1 `pipeline` 确认真实 Evaluator → 外部 LLM mock → PG snapshot/Outbox → MQTT/Strategy；`boundaries` 检查概率及事实/推断分离；历史回放使用固定 AI 输出并检查未来信息隔离。 | LLM 响应来自明确 mock，不证明真实模型分析质量、概率校准或收益。可追溯案例读取已实现，但实际历史案例库质量、案例沉淀/反馈及 pgvector 相似检索未验收。 |
| **10. Strategy / SDK / Compute** | 两个类方法返回 `Effects{intents,requests,timers}`；只读上下文、可变事务 state、单实例串行、checkpoint 版本 fencing，禁止直接下单。支持行情/新闻/执行/组合/定时及查询结果回调；Snapshot、Compute、Notification、Export typed 请求与结果。超参版本/五态路由；history/window/group/agg/predict 有界 SQL 计划，TimescaleDB 可用时下推，注册确定性预测模型；历史预热及迟到 revision 检查。 | R1 验证回调串行、异常草稿丢弃、重试一次提交、虚拟 LATEST→AS_OF、未来排除、真实 Python 账户回调/服务 Effects 原子提交；PG 测试验证迟到修订及 sum/count 分片归并等价。E1 实际策略与 snapshot/compute 闭环。 | 当前按请求完整重算有界窗口；未实现 Redis 热缓存、分布式增量分片 worker、通用模型分片调度或任意 Python 自动并行。sum/count 等价测试不等于全部模型分片等价。仓控/rebalance 的具体决策由策略 Effects 表达，不能宣称已交付所有内置策略。 |
| **11. Order / Backtest / ExecutionGroup** | 持久行情、余额/仓位/精度/硬风控 → Wallet 请求预占 → Exchange；ACK 与成交事实分离，Unknown 先对账。ExecutionGroup 等待权威终态、全部实际成交和 Wallet 结算后决定补偿；补偿重新走 reduce_only 完整链。回放有限 keyset 输入、虚拟时钟、固定顺序/AI/费用、持久屏障；普通限价挂单在后续持久 tick 按固定顺序撮合，成交与 Outbox 同事务，滑点按 price tick 不利取整。 | E1 主链实际成交与 Wallet 入账；`groups` 同所双账户真实服务补偿，重启/重投无重复；R1 6 项 coordinator、Unknown 不重下、硬风控、venue polling 测试。`replay` 每轮两个隔离 run 比较相同冻结历史的成交经济结果，共 4 个 run；`rust-resting` 两项真实 PG 测试验证后续 crossing、限价、未来/迟到 tick 及重启去重。 | 多所真实成交、撤单竞态和跨所补偿未验收。同所模拟补偿不能标作跨所验收。模拟器是明确的全量成交模型，未模拟深度队列、部分可用流动性或网络延迟；多独立 query owner 同刻回包的全局确定性仍需额外调度约束。旧缺 owner/冻结条款的挂单不能猜测迁移。 |
| **12. Wallet** | 唯一资金/仓位 writer；真实 Funding/Execution 事实触发 Inbox、平衡账本、预占、余额/仓位、版本化组合与 Outbox 同事务。按执行外部 ID 去重；终态先到时等待实际成交累计齐全才释放。实际 quote amount 用于现金、预占和成本基准，完整平仓分配全部剩余成本；历史资产可从不可变分录恢复。 | E1 不直接种余额，使用真实 FundingFact 开户；主链检查非 funding 的执行 journal、平衡 posting 与 filled 状态；SIGKILL/重复事实不改变金融投影；同所双账户补偿校验真实费用/现金。R1 5 项精度/资产/成本/PnL 测试通过。 | 未完成真实账户全量资金、转账、公司行动、衍生品保证金/强平事实及长期对账演练。投影重建能力不代表完整的自动外部对账调度已验收；账户估值仍受完整价格和资产元数据限制。 |
| **13. Exporter** | 配置化 Kafka Producer、Google Sheets；统一业务消息 key、有限批次、游标、重试/背压，typed request/result。Kafka 强制幂等 producer/acks=all；Sheets 使用官方 ValueRange 的固定 range PUT，凭据来自宿主 env 引用。 | E2、R2 使用真实 Kafka/PG 验证 ACK 后持久状态、重启去重、身份冲突、迟到游标及 scope；R1 `service_effects` 验证导出请求和状态原子提交。 | 未写入真实 Google Sheets，也未验收 Kafka 多 broker 故障切换。Kafka ACK 与 PG commit 之间崩溃可能重复，仍需下游按业务 key 去重；固定 range 不等同于无幂等保障的 append。 |
| **14. Web** | 重做 Overview→市场行情→Workflow→数据源→交易所导航、近期 run/指标、来源/类别筛选、ReactFlow 节点连线、连接/Secret 引用、通知/个人菜单、run/trace/span/parent span 追踪界面及对应受保护 API；缺少真实数据时展示空态。 | E1 `web` 两轮真实 Chromium/Playwright，18 张截图，无页面异常；验证导航、政策源 query ID、通知/个人菜单和真实画布节点/连线，不拦截 API。R1 单独验证认证 HTTP/PG 的 Workflow 事务。 | UI 截图不证明已登录保存、部署启动及 trace 查询全链路；`web-full` 尚未通过。清算/资金费率等 UI 类别存在，但缺完整真实数据管线；不得用空态截图宣称热力图行情已交付。个人通知真实金融事件、连接认证及生产部署 UI 仍需真实会话验收。 |

## 二、新增 E2E 要求

| 用户要求 | 当前实现与证据 | 未满足的边界 |
| --- | --- | --- |
| **2.0 完整 case 项目** | [marketpulse-autonomy-quantification](../../use-cases/marketpulse-autonomy-quantification/e2e/README_CN.md) 沿用相邻 case 的部署/场景/报告结构，统一 `runner.py`，支持多场景、两种部署模式、rounds、reuse-build、timeout 与归属校验后清理。E1/E2 的 `structure` 检查实际目录、Python AST 和配置文件。 | structure 检查不是全仓 Rust 规范审计。Kubernetes mode 有装配实现，但此次真实通过的服务链不能代替 K3s 部署验收。 |
| **2.1 Twitter / TruthSocial mock 与官方 schema** | 两个独立 mock 提供真实 HTTP REST、WebSocket、官方 SDK MCP；另有 A2A gateway。E1 验证各通道读取同一合成 timeline。X 使用已保存的官方完整 OpenAPI 快照，报告版本 **2.173** 与 SHA-256；合成帖子通过 schema 检查。 | 合成 fixture 不是真实最新平台帖子。未取得可公开验证的官方 TruthSocial OpenAPI JSON；现用明确标注的 Mastodon 兼容子集，不能称“官方最新 schema”。“两个平台均按最新官方 schema”这项尚未完全满足。 |
| **2.2 OpenAI mock** | 独立 mock 按评估响应契约提供五态概率、参数、事实引用和推断；真实 Evaluator 发 HTTP 请求并持久化结果。E1 `boundaries/pipeline` 通过。 | 不证明真实 LLM 服务、模型效果或交易智能质量；回放使用已记录输出，不在回放期间重新调用模型。 |
| **2.3 Datafeed 三种接入 → MQTT → Evaluator → Strategy → Order → Wallet** | 实际 Rust Datafeed REST/MCP/A2A transport 对外部 mock 执行；真实 EMQX、PG、Python worker、Order/Wallet 预占和产品 SimulatedExchange。E1 主链从新增输入核对新增 execution、filled order 和非 funding journal；含查询结果、重复/乱序、SIGKILL 重启、应用补放和独立历史回放。 | API key 路径验证的是凭据访问，不是平台预付费购买/结算。E2E 不向真实交易所发送资金交易。通用分布式分片、多 broker/多数据库故障等未包含在这条主链中。 |
| **3. Playwright 真实完整 UI** | E1 两轮实际浏览器操作和截图；不 mock API。已实现独立 `web-full` 场景，要求真实 Authguard browser storage state，缺少则失败。 | 当前仅未认证可访问交互验收通过；B1 的认证 UI 门禁未通过，因此“完整功能 UI E2E”不能标为完成。 |

## 尚需继续交付的重点

Kubernetes provider、管理探针/业务 readiness、按 Workflow 隔离的 Order/Exporter owner、迁移 17、主动多所快照均已有上述实测证据。当前主链 PG 未启用 TimescaleDB/pgvector；E4 已验证独立双扩展实例及显式相同分桶 origin，普通 Case Compose 与 Helm 已同步镜像/数据目录/bootstrap。旧 LLM 调试日志的含密连接 URI 输出已删除并编译通过。Rust tracing 自动采集接线正在补充，完成前不计为已验收。Backtest Service/网络策略当前有计划测试，E2 的真实 Pod 组合为 Datafeed+Strategy，未把它记作 Order→Backtest 实际 HTTP 网络验收。

1. 提供真实 Authguard Application/Gateway 域名及有效浏览器会话，完成 Hosted Login、受保护的连接/Workflow 保存启动、通知与 trace 查询 `web-full`；补验 API controller 调度与 Docker provider 的真实生命周期；Kubernetes provider 已有 E2。
2. 获取合法可用的 TruthSocial 官方 schema，或明确接受当前兼容接口边界。当前证据不能把这一外部缺口标为完成。
3. 扩展已验证的主动快照至更多交易所，补 Hyperliquid 衍生市场指标采集与完整组合估值；UI 需要以这些真实数据再次验收。
4. 补 Redis 热缓存、增量/修订状态、分片 worker 和模型声明语义的分布式等价验收；明确多个 query owner 的确定性调度。
5. 在明确账户、权限及交易范围后另做实盘/沙盒交易所到账、费用、未知结果、撤单和跨所补偿验收；当前模拟与外部边界测试不足以替代。
6. 补全 Rust tracing 归档接线、真实 Email/Telegram/Google Sheets 投递、broker 灾难持久性和生产数据迁移/恢复演练。

当前可以确认“所列真实组件与场景已通过”，不能由 PASS 总数推导全需求完成或给出可测量的“99.9% 真实”比例。外部 mock、产品模拟撮合、纯逻辑测试、真实中间件和实际外网只读调用的范围已分别记录。
