# Runtime v2：领域归属与兼容迁移

此变更保留现有 crate 和 `core/modules/*/{handler,route,store}` 骨架。Messager、Exchange、Logservice 是嵌入宿主进程的共享组件，不建立独立 Deployment。业务节点使用同一 `sigbot` 二进制的既有角色入口；Python 仅在无宿主凭据的独立 worker 中运行。

## 表映射与写入归属

| 领域 | 既有表 | 处理方式 / 新表 | 写入归属 |
|---|---|---|---|
| 身份 | `sys_user` | 保留历史数据；删除本地用户/权限 HTTP 和 repository 写入路径，迁移至 Authguard；`identity_binding` 绑定外部主体 | Authguard 管身份，业务 API 管绑定 |
| 租户 | `sys_tenant` | 保留业务租户绑定 | 业务租户 API |
| 连接 | 无统一表 | 新增 `connection`；配置与 Secret 引用分离 | 连接 API |
| Workflow | `t_workflow` UUID、代码中的 `s_workflow` BIGINT 冲突 | 保留旧表；`workflow_legacy_id` 保存 UUID→BIGINT 映射；统一运行表 `s_workflow` | Workflow API |
| Run | `t_workflow_run` | 保留旧表，旧 UUID 以字符串导入 `workflow_run.run_id` | API 原子创建，Deployer 推进运行状态 |
| 消息 | 旧内存重试 | `messager_inbox`、`messager_outbox`、`workflow_sequence` | 各领域借助共享事务接口写入自己的 consumer/producer 分区 |
| 状态 | 旧进程内状态 | `workflow_checkpoint`，按 scope/workflow/node/run/partition 隔离 | 对应节点单一 owner |
| 行情 | 无规范事实表 | `market_event` 保存来源事实；`market_event_delivery` 保存各节点/run 的规范消息，来源 seq 与消息 seq 分离 | Datafeed |
| 评估 | 无统一可追溯记忆表 | `evaluation_case`、`evaluation_snapshot`；事实、推断、来源分别记录 | Evaluator |
| 订单 | 旧 Signal/Trade 临时对象 | `order_record`、`order_execution`；`execution_group`、`execution_group_leg` 保存多腿协调及补偿进度 | Order |
| 交易所观察 | 无持久轮询 | `order_venue_checkpoint` 保存分页游标与租约版本；`order_venue_observation` 去重实际成交及累计状态 | 嵌入 Order 的 Exchange 适配器，由 Order 提交事务 |
| 模拟撮合 | 旧回测独立交易路径 | `simulated_order`；独立 execution 事实，经同一 Wallet 入账 | Backtest |
| 钱包 | `s_wallets` | 保留；增加 env/space/account 绑定及 portfolio_version | Wallet |
| 原始账本 | `s_ledger` | 保留；修正 FK 到 `s_wallets(id)`，禁止 UPDATE/DELETE，交易所成交 ID 去重 | Wallet |
| 双分录账本 | 无 | `wallet_journal`、`wallet_posting`；每资产借贷平衡，追加更正 | Wallet |
| 预占 | 无 | `wallet_reservation`；包含生产节点、run、seq，余额与预占同事务 | Wallet 领域事务组件 |
| 余额 | `s_balances` | 保留；可用/锁定投影可重建 | Wallet |
| 仓位 | `s_positions` | 保留；按真实执行时间及稳定成交键重建 | Wallet |
| 净值 | `equity_snapshots` | 保留；没有完整价格估值时返回不可用，禁止跨币种直接求和 | Wallet 估值 |
| 日志 | 旧系统日志 | `log_chunk` 仅存索引；宿主 SQLite WAL 缓冲、S3 不可变对象 | Logservice 组件 |
| 通知 | 旧转发入口 | `notification_delivery`、`notification_read`；用户已读状态单独投影 | Notification / 个人已读 API |
| 导出 | 旧占位 writer | `export_checkpoint`；自然业务键、分区游标 | Exporter |

历史 `s_*` 表不为命名统一而破坏性重命名。新增表统一单数 `snake_case`。旧 `t_*` 为迁移来源，不能继续作为运行时双写目标。数值迁移到 `NUMERIC(40,18)`，保留原 `NUMERIC(30,8)` 的 22 位整数空间。

## 迁移顺序

1. 备份数据库和对象存储，暂停旧版本金融写入。迁移身份绑定时不导出或复用旧用户密码。
2. 使用独立 schema owner 运行 `tooling/deploy/migrations/migrate.py`。连接只通过标准 `PG*` 环境或 libpq 配置传入；脚本持有 advisory transaction lock，并记录每份 DDL 的 SHA256。已应用文件变更会拒绝执行。
3. 检查 `workflow_legacy_id`。旧流程以 `legacy` 环境和停止状态导入；明确指定真实 tenant/space/env、连接 Secret 和节点参数后才能运行，不能自动猜测归属。
4. 使用 Wallet 的真实资金事实初始化/对账，不直接修改余额。历史账本和新双分录账本边界需明确；历史余额迁移应追加带来源凭据的 opening funding journal，不能同时再重复回放同批历史成交。
5. 启动 v2 消费者并等待 MQTT SUBACK，再启用 Datafeed；从隔离的 paper/simulated 环境验证后切换生产配置。
6. 旧 v1 消息缺少 env/space/run/seq，不能安全转换成金融写入。兼容入口明确拒绝交易并要求迁移，避免暗中为其制造新身份。

迁移 17 将 Order/Exporter 的消费者与发布者名称统一为 `order:{env}:{tenant}:{space}:{workflow}:{node}`、`export:{env}:{tenant}:{space}:{workflow}:{node}`，执行/状态消费者及 `:reservation`、`:venue` 子键沿用同一范围。执行前暂停旧 Order/Exporter，完成迁移后只启动新版本；不支持新旧写入方混跑。迁移依据消息中的 Workflow 或同范围 `workflow_run` 绑定迁移旧记录；保留 Inbox `done`、Outbox `sent`，只合并规范事实一致的重复键。事实冲突、无绑定或非法显式 Workflow 会中止整个事务，必须先对账，不能删除旧 Inbox 绕过。金融事实 payload 和钱包账本不改写。Broker 中尚未确认的旧消息若缺少 Workflow，仅从同范围持久 `workflow_run` 恢复传输头后再路由与去重；不会以当前 worker 的 Workflow 猜测绑定，也不会覆盖显式 Workflow。旧回放配置只在运行时规范屏障消费者，冻结检查点配置保持原样。账户广播中的其他 Workflow 消息写入本实例独立 `:routing` Inbox，事务提交后 ACK；不能进入正式订单 Inbox。

旧 `appdb.type=TIMESCALEDB` 继续读取 `appdb.timescaledb` 原连接信息，并在配置入口规范成 `POSTGRESQL`；业务仓储与健康检查使用同一个 PostgreSQL 连接。新配置统一使用 `appdb.postgres`，TimescaleDB 与 pgvector 是该数据库的扩展，不是独立金融写入库。

迁移目录按日期与数字序号排序，避免 `-10` 排在 `-7` 之前。空库与升级库使用同一入口；重复执行校验已应用文件的 SHA256 后跳过，不重复改写历史数据。

生产数据库账户不能拥有 schema、trigger 或 superuser 权限。领域分表授权应由部署方按实际角色分配；代码层 ownership 和 tenant 条件不能替代数据库权限边界。当前 v2 持久运行时使用 PostgreSQL；SQLite 继续兼容既有投影，不宣称完整 v2 分布式等价。pgvector 与 TimescaleDB 是同一 PostgreSQL 的扩展，禁止为每个 Workflow 创建独立共享中间件。

## 消息与恢复

规范 topic 来自 `types/modules/messager/topic.rs`，业务代码不得拼装替代 topic。基路径为 `internal/v2/{env}/{tenant_id}/{space_id}`，Workflow 与 Account 分支互相独立。订阅者校验 scope、节点输入边和可选 target_node_id。

持久幂等键包含 scope、生产节点、run_id、seq；不能只用 run_id+seq，因为同一 run 的不同节点都可能从零开始。重试保持业务身份，增加 attempt_no 并创建新 OTel span。`requestSpanId` 等于 producer span_id，只存在 wire 与有界临时配对缓存；数据库 JSON 明确拒绝该字段。重启后配对缓存可以丢失，业务恢复依靠持久键。

MQTT 使用 QoS1、持久会话、手动 ACK。处理器成功返回表示已完成其承诺的持久提交，或者已将任务可靠存入 Inbox/队列；不能在后台 task 尚未落盘时 ACK。Outbox 在收到 PUBACK 后标记 sent。Broker 灾难丢失不能仅依赖“持久会话”字样：保留 sent 行，运维可通过受 scope 限制的 `PostgresMessageStore::replay` 明确补放指定范围，消费者依靠 Inbox 去重。

Order 提交前持久化 submitting，网络结果未知时保留预占并进入对账。接受 ACK 不代表成交；只有真实 execution/funding 事实进入 Wallet。Binance SDK 关闭 POST 自动重试；交易所要求的 client order ID 是现有业务键的确定性编码，不是另一个追踪/关联 ID。Paper/Simulated 通过适配器切换，禁止篡改真实交易所 URL。

Wallet 预占通过 `A/wallet/reservation/{request,result}` 完成。Order 先保存接受的意图和固定预占条款，Wallet 验证条款后同事务提交 Inbox、预占、组合版本、响应 Outbox；Order 收到 reserved 才允许提交。真实终态累计成交量尚未被钱包全部入账时，释放请求保持待处理。结果未知不会释放预占或重新下单。

IBKR 与 Polymarket 使用 SDK 构造、分配或签名提交内容；Order 在网络交易操作之前保存不可改写的 prepared submission。签名内容使用宿主 Secret 提供的版本化 AES-256-GCM 密钥加密，并将 scope、账户和原始业务键绑定为 AAD。恢复时查询已保存的交易所 ID，不重新生成签名交易。新的 `InstrumentSpec` 是资产和精度的唯一契约；v2 成交必须明确 base_asset/quote_asset，不能从股票代码或预测市场 token ID 猜测币种。历史已入账事实可从原不可变分录恢复资产，不修改历史账本。

成交事实的可选 `quote_quantity` 表示交易所确认的实际成交现金额；存在时钱包优先使用它，避免由单位价乘数量引入结算舍入差。缺少字段的历史事实继续使用旧计算。Polymarket 新准备记录还保存链上扫描起点，要求只读 RPC 能验证 finalized 回执；没有扫描起点的旧准备记录不能自动提交，需对账迁移。

Live 成交读取在宿主 Order 中执行，Wallet 的可信 execution_node_ids 必须包含对应 Order 宿主节点。交易所真实费用、成交 ID、时间、累计状态与分页游标一起校验；观察事实、Outbox 和游标在同一事务提交。进程重启通过持久源分区恢复 Order 投影，通过 MQTT 恢复 Wallet，宿主不会订阅自己的派生状态而形成消息循环。未拿到权威费用的成交不能填零费用冒充完整事实。

ExecutionGroup 使用生产节点/run/group_seq 标识业务组，group_seq 是策略消息序列中的显式业务序号，不新增关联 ID。只允许配置的协调节点和同一 Workflow 推进状态。未知结果、未结算的部分成交、未齐全的终态累计量均等待对账；所有相关成交入账且预占解除后才决定 completed、held 或补偿。补偿是按已成交量生成的独立 reduce_only 意图，重新经过真实行情、风控、预占和执行链路；补偿失败进入明确终态，不盲目重复发送。缺失的新鲜行情使组等待，不使用旧价格强行补偿。

## 日志、导出和外部边界

LogArchive append 的 ACK 表示宿主持久卷上 SQLite WAL FULL commit；单宿主磁盘损毁仍可能损失尚未归档部分。归档使用 S3 conditional create + SHA256 校验，再提交 PG 索引，最后释放本地 payload；任何一步重启均可重试。读取按追加 cursor，不按业务 seq，故晚到的小 seq 不会漏读。S3 生产桶应配置版本/保留策略；代码的不可变创建不替代存储管理员权限管理。

启用日志归档的 Workflow worker 必须绑定预先配置的专属持久卷，不能在 emptyDir/tmpfs 上承诺日志持久 ACK。Deployer 校验卷归属，固定 spool 路径，更新/停止时保留卷；缺少卷时拒绝部署。具体字段与卷标签见 [Deployer 配置](../../src/deployer/README.md)。

Kafka 使用成熟 Producer 的幂等发送及 acks=all；跨进程崩溃仍需下游按业务 key 去重。Google Sheets 使用固定 range 的幂等覆盖，不能将无幂等保障的 append 当作 exactly-once。返回结果及游标通过 Inbox/Outbox 持久事务记录。

真实边界与限制由 E2E 报告记录：X 使用官方 OpenAPI 快照；Truth Social 未获取可公开验证的官方 OpenAPI JSON，当前是明确标注来源的兼容 schema，不能标注为“官方最新”。LLM 和社交平台只在外部边界模拟；核心 Rust 服务、PG、EMQX、MinIO 和 Python worker 必须真实运行。Authguard 的 Application/Gateway 验收使用用户提供的真实域名，不能用猜测域名冒充完成。

## 历史 Replay 配置与边界

历史回放沿用现有 Backtest、Strategy、Order、Wallet 和 Datafeed 服务。Backtest 设 `runtime_v2=true`、`replay_v2=true` 并提供 `replay_config`；Strategy 与 Order 的 `runtime_config.virtual_clock=true`，Order 选择 Simulated adapter，Backtest 的 `matching_config.virtual_clock=true`。Datafeed 设 `query_only=true`、`clock_mode=virtual`，仅处理快照和计算请求，不初始化外部行情客户端。回放阶段不启动 Evaluator，直接使用源 run 已持久记录的 AI 版本、概率、分类参数和案例引用。

目标 scope 必须与源 scope 分离，环境使用 `replay`、`backtest` 或 `replay-*`；目标 `workflow_run.replay_of` 显式引用源 run，钱包独立绑定并通过资金事实开户。`replay_config.history` 指定源 scope/run/workflow、sources、from/until/frozen_at；建议冻结已停止采集且完成持久化的源 run。历史 SQL 使用 keyset 分页，单记录最多 64 KiB。`warmup_from`、`warmup_limit` 可在首个决策前分批导入更早行情，只用于预热；不会按整批历史调用策略。完整字段与消费方映射见 [Backtest 配置说明](../../src/backtest/README.md)。

迁移 7 增加 `workflow_timer`、`backtest_checkpoint` 与 AI 实际可得时间；迁移 10 在检查点保存当前已准入的 canonical message，崩溃恢复沿用原 node/run/seq。迁移 11 的 `market_event_delivery` 将外部市场序列与消息序列分开：相同市场事实可以由不同 Workflow 节点投递，各自保留首次消息身份和规范 DTO。迁移 13 修正早期以 PostgreSQL 事务开始时间回填 AI 可得时间的问题；旧记录只向更晚的已知 produced_at 推进，不提前暴露模型输出。这些检查点与映射均不保存 `requestSpanId`。

回放以实际 available_time 排序，同刻按已录评估、定时器、行情、新闻处理。定时器与策略草稿同事务持久化，触发后仍通过 MQTT 进入 `on_data`。SDK 把虚拟时钟下的 LATEST 快照归一为 AS_OF(ctx.now)。推进时钟前，Repeatable Read 屏障逐一检查配置边的 Inbox 和递归生成的 Outbox，同时等待订单提交与钱包执行事实落盘；PUBACK 不能代替消费者完成。需要明确配置每条边的 consumer，尤其执行事实同时送达 Order 与 Wallet、预占 request/result 双向完成、带累计成交量的 OrderStatusFact 送达 Wallet。

当前真实 E2E 将完全相同的冻结历史、AI 输出和撮合参数送入两个隔离 run，逐笔比较成交价格、数量与费用，并验证完成后重启不重复交易。实时异步链路的查询耗时会影响其决策 tick，不能据此要求实时成交等于虚拟时钟回测成交。单 query owner 与串行 Strategy 已实际验证；任意多个独立 query owner 同刻回包的全局顺序仍需要额外的确定性结果调度约束。

迁移 16 保存未成交限价单的原消息、撮合 owner 和固定配置；后续已落盘行情按接受时间/node/seq/account 固定次序撮合，同事务提交成交与 Outbox。早于接受时间的迟到 tick 不能成交，重复 tick 或重启不能再次成交，滑点价格按 InstrumentSpec 的价格步长作不利取整。当前是全量成交模型，未宣称完整深度队列、部分可用流动性或交易所网络延迟模拟。
