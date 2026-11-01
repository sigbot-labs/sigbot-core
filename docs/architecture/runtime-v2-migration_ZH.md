# Runtime 数据模型与领域归属

本项目只维护当前数据模型，不提供旧表、旧 ID、旧消息或旧配置兼容层。认证、租户、用户与权限由 Authguard 负责；Sigbot 仅在业务记录上保存经过验证的租户 UUID。Messager 与 Exchange 是宿主进程内共享 Provider；Logservice 是 Controller 可动态部署的 Workflow 组件。

## 表与唯一写入方

| 领域 | 表 | 唯一写入方 |
| --- | --- | --- |
| 连接 | `cfg_connection` | Core Controller Repository |
| Workflow / Run | `wf_workflow`、`wf_run`、`wf_checkpoint`、`wf_sequence`、`wf_timer` | Core Workflow Repository 保存配置；Controller 通过 Core Repository 写 Run/Worker 生命周期状态 |
| 消息 | `msg_inbox`、`msg_outbox` | Core Messager Repository；领域事务调用它提交自己的分区 |
| 行情 | `mkt_event`、`mkt_delivery` | Datafeed Repository |
| 评估记忆 | `eval_case`、`eval_snapshot` | Evaluator Repository |
| 回测 | `bt_checkpoint`、`bt_order` | Backtest Repository |
| 订单 | `ord_order`、`ord_execution`、`ord_execution_group`、`ord_execution_leg`、`ord_venue_checkpoint`、`ord_venue_observation` | Order Repository |
| 钱包 | `wlt_wallet`、`wlt_ledger`、`wlt_journal`、`wlt_posting`、`wlt_reservation`、`wlt_balance`、`wlt_position`、`wlt_equity_snapshot` | Wallet Repository |
| 日志 | `log_chunk` | Logservice Repository；对象内容写入不可变 S3/MinIO 分块 |
| 通知 | `ntf_delivery`、`ntf_read` | Notifier Repository；Core 只写用户已读投影 |
| 导出 | `exp_checkpoint` | Exporter Repository |

租户、Workflow、Run、账户、钱包、订单和事实 ID 使用 UUIDv7；节点号和消息序列使用有界整数。`run_id` 是公开业务幂等键，不创建额外关联 ID。所有表名使用带领域前缀的单数 `snake_case`，已验收的钱包、账本、余额、仓位和净值表保持不变。

## 约束

- `wf_workflow` 与 `wf_run` 通过 `(env, tenant_id, workflow_id)` 复合外键绑定，Checkpoint、Timer、行情投递、评估、日志和通知继续引用同一作用域。
- `wlt_wallet` 以 `(env, tenant_id, account_id)` 唯一绑定账户。Order 通过同范围 UUID 外键引用钱包，不允许字符串 tenant ID 或类型转换 JOIN。
- `wlt_ledger`、`wlt_journal`、`wlt_posting` 和 `ord_venue_observation` 是不可变事实；更正通过新事实或反向分录完成。
- `wlt_posting` 在事务提交时逐资产校验借贷平衡。余额、仓位和组合版本是可删除后重建的投影。
- Inbox/Outbox 使用 `scope + owner + node_id + run_id + seq` 去重。持久 JSON 禁止保存 `requestSpanId`。
- 外部行情的 `event_at` 不得晚于 `available_at`；评估输出的 `available_at` 不得早于 `as_of`，从而禁止未来信息进入决策。
- 交易模式只有 `live`、`paper`、`backtest`。它与 `dev/sit/uat/prod` 等路由/部署环境完全独立。

## 迁移入口

新项目只有一个最终态基线版本，PostgreSQL 与 SQLite 使用相同领域表集合：

```text
migration/20261031000000/
├── postgres/{init.ddl.sql,init.dml.sql}
└── sqlite/{init.ddl.sql,init.dml.sql}
```

使用 schema owner 执行：

```bash
python3 migration/migrate.py
```

脚本通过 advisory transaction lock 保证单写入方，并在 `mig_version` 保存版本与联合 SHA-256。已应用文件发生漂移会中止。应用进程账户不得拥有 schema、trigger 或 superuser 权限。

SQLite 保持相同业务表集合，用于本地单进程开发；跨进程锁、扩展函数和分布式持久性仍以 PostgreSQL 验收。控制面 PostgreSQL 镜像可同时启用 TimescaleDB 与 pgvector 扩展。租户数据面的 TimescaleDB/pgvector 由 scoped `ctl_worker` 独立声明和调和，不与控制面数据库共享写入职责。

## 消息与恢复

Topic 基路径是 `internal/v2/{env}/{tenant_id}`。Workflow 分支使用 Workflow ID，Account 分支使用 Account ID。生产者与消费者必须从 `sigbot-types` 的 topic 枚举生成路径，不能手工拼接。

MQTT 使用 QoS 1、持久会话和手动 ACK。业务处理完成 Inbox、领域事实/投影及 Outbox 的同一事务后才能 ACK。Outbox 收到 PUBACK 后标记 sent；显式补放保留 `run_id` 和消息序列，递增 `attempt_no` 并创建新 span。

`traceparent` 贯穿 HTTP、MCP、A2A 和 MQTT。`requestSpanId` 等于生产者 span ID，只能作为短时响应缓存键，响应原样回显；不得落库或承担业务恢复职责。

## 交易与回测边界

- Live 将订单发送到真实 Binance、IBKR 或 Polymarket API。
- Paper 将订单发送到实时 Paper 适配器，可由内部撮合或交易所 Demo API 实现。
- Backtest 将订单发送到 Backtest 服务的专用 `BacktestExchange`，使用冻结历史、虚拟时钟、固定费用和确定性排序。

Order 的接受回执不等于成交。网络结果未知时保留预占并查询交易所，不重新生成签名内容或盲目重下。只有权威 Execution/Funding 事实能进入 Wallet。跨所 ExecutionGroup 在所有腿和钱包入账完成后才能结束或生成补偿意图。

Backtest 的 `bt_order` 固定保存原始消息、撮合 owner、InstrumentSpec、费用与滑点参数。迟到且事件时间早于接受时间的行情不能成交；重复行情和重启不能产生第二笔成交。历史预热只用于状态/计算，策略仍按虚拟时间逐事件调用 `on_data`。

## 日志与外部输出

Logservice 的 append ACK 表示本地持久缓冲已完成 WAL 提交；归档按“条件创建 S3/MinIO 对象 → SHA-256 校验 → 提交 PG 索引 → 释放本地 payload”的顺序执行。读取使用追加 cursor，晚到的小业务序列不会被跳过。

Kafka 使用幂等 Producer、`acks=all` 和稳定业务 key；数据库提交窗口仍要求下游去重。Google Sheets 使用固定 range 的 ValueRange PUT，通过 `reqwest` 发送，并支持显式 `proxy_url`。凭据只允许使用中央配置解析后的 Secret key 引用，业务组件不得直接读取环境变量。
