# Marketpulse 自主量化 E2E

沿用相邻 AgentVault case 的部署、场景、报告和清理结构；只模拟 Twitter、TruthSocial、OpenAI 三个外部边界。内部 Datafeed/Evaluator/Strategy/Order/Wallet 必须运行真实 Rust 模块，测试不得代写内部交易业务。

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m playwright install chromium
export MARKETPULSE_MOCK_TOKEN='<本地测试令牌>'
.venv/bin/python runner.py --scenario structure,schemas
.venv/bin/python runner.py --scenario boundaries --timeout 30
```

唯一入口为 `runner.py`，支持重复 `--scenario`、逗号列表、`--rounds`、`--deployer docker|kubernetes`、`--timeout`、`--reuse-build`、`--deploy`、`--cleanup-after-run`、`--cleanup`。默认执行全部场景；真实服务缺失直接失败。环境默认保留；报告写入时间戳目录，未运行项不计通过。在相同端口启动新一轮前，可用 `--stop-runtime` 只停止本 case 拥有的真实服务进程，保留中间件和历史证据。

Docker Compose 部署三个 mock、PG、EMQX 和 MinIO。需设置 `MARKETPULSE_POSTGRES_PASSWORD`、`MARKETPULSE_DATABASE_URL`、`MARKETPULSE_MINIO_ACCESS_KEY`、`MARKETPULSE_MINIO_SECRET_KEY`。应用仓库迁移，构建现有 `sigbot` 与 `sigbot-strategy-worker`，运行 `--scenario runtime,pipeline,recovery,logs,web`。runtime 使用现有 CLI 启动真正的 Rust 服务，发布外部初始资金事实并等待 Wallet 实际入账；不直接写余额。runtime 为每轮生成独立 run_id；只执行 pipeline/recovery/logs 时，可用 `MARKETPULSE_WORKFLOW_RUN_ID` 指定已有隔离运行。PG/MQTT/MinIO 默认分别绑定回环 15439/18883/19000。中间件默认只绑定本地回环；Compose EMQX 强制 JWT 认证；配置至少32字符 `MARKETPULSE_MQTT_SIGNING_KEY`，runner生成有效4小时、仅当前环境/租户/空间的 case 凭据。`acl` 场景真实验证非法凭据、跨环境/租户/空间发布及订阅均被MQTT5拒绝。这些本地broker凭据不用于Authguard UI/API；服务角色级ACL与broker进程崩溃持久性仍需单独验收。Kubernetes 固定使用 `/etc/rancher/k3s/k3s.yaml`，case chart 只部署外部 mock；真实服务和中间件使用产品 chart，配置或转发相应地址。

`config/sigbot/config.yaml` 统一定义地址、身份范围和凭据环境变量名。Mock 请求使用 Bearer token；MCP 使用官方 Python SDK，A2A 使用官方 Python SDK 0.3，WS 使用真实 WebSocket。三种 gateway 返回相同 timeline；这些适配层并不声称是平台原生协议。

X 完整 OpenAPI 从官方 `https://api.x.com/2/openapi.json` 下载；版本、时间、SHA-256 保存在 `config/twitter/provenance.json`。生成的新闻明确为合成测试数据，经过官方 schema 验证，不冒充真实最新帖子。

未查到可公开获取的官方 Truth API OpenAPI JSON。官方 TruthSocial 开源页面提供 Mastodon 源码，2026 年公告的商业 Truth API 另有授权。当前 Truth mock 使用**明确标识的本地 Mastodon 兼容子集**，不声称是最新官方商业接口。详见 provenance 文件；这一差距不能通过自造 schema 宣称已满足。

示例策略由真实 Rust/Python 隔离 worker 执行，等待新闻和评估超参，以真实公开 Binance 行情产生有界意图；实际执行使用产品 SimulatedExchange，不发真实交易。`pipeline` 只往外部 mock 注入重复/乱序新闻，订阅真实 MQTT，证据 fsync 后 ACK，再检查真实 PG 的行情、评估、订单、执行、钱包账本和借贷平衡。`recovery` 对本次 runner 自己启动的 Strategy/Order/Wallet 进程执行 SIGKILL，重启后重投已捕获的真实信号/执行事实，要求金融投影不变；`logs` 校验真实 Python stdout 经 Rust WAL、MinIO 对象及 PG 索引的哈希与隔离范围。只有实际执行成功才记通过；历史回放有下面独立场景，通用模型的分布式分片及真实交易所执行仍需额外证据。

`web` 用真实 Playwright Chromium 导航、操作通知/个人菜单和 Workflow 画布并截图；不拦截或伪造 API。`web-full` 需要来自真实 Authguard 登录的 `MARKETPULSE_BROWSER_STORAGE_STATE`，验证真实保存、启动及 run/trace/span；缺会话直接失败，禁止编造测试登录。`MARKETPULSE_CHROMIUM_PATH` 可复用已经下载的兼容 Chromium。

`replay` 选择此前真实完成的服务链（可用 `MARKETPULSE_REPLAY_SOURCE_RUN_ID` 指定），冻结有限历史及已记录 AI 输出，为每次运行分配独立 `replay-marketpulse-*` 环境并创建新账户、钱包、Workflow 和 run。实际启动 Rust Backtest 回放 owner、Strategy、只查询的 Datafeed、Order、Wallet；核对实际成交价格/数量/费用、钱包借贷平衡、AI 版本/概率及虚拟可得时间，随后重启回放 owner 验证幂等。Python 只负责配置与验收，不实现回放引擎、撮合或记账。历史窗口最多一小时、决策步骤最多1,000；未完整通过的回放报告仍计失败。

`groups` 实际启动7个正式进程，验证同一交易所、两个账户的补偿。第一账户通过外部资金事实真实入账，第二账户无资金；真实策略发送两个分组意图，第二个 Wallet 拒绝预占，coordinator 通过原有风险/订单/执行/钱包链完成唯一反向 reduce_only 意图。验收实际 BUY/SELL、零剩余仓位、精确费用/现金，再强制结束 Strategy、两个 Order 和两个 Wallet 并重投原始信号。这是多账户补偿验收，不冒充跨交易所验收。`rust-groups` 另用明确的合成参与方状态夹具，验证6项真实 PostgreSQL coordinator 契约，覆盖部分/未知成交及补偿失败。

`rust-boundaries` 执行真实 Rust Datafeed REST/MCP/A2A 适配器及 PG 查询集成测试；`rust-messaging` 验证真实 broker 投递、相同 run/seq 且 attempt 递增和新 span 的持久补放、隔离范围及 quarantine ACK。使用 `--reuse-build` 时按实际测试名选择已编译二进制并记录 SHA-256，不调用构建；否则需 Cargo 且不得与其他构建并发。清理仅终止 manifest 中二进制和进程起始时间都仍匹配的本 case 进程，之后删除本 case 部署。默认保留所有验收环境。

Snapshot/compute 回调使用真实共享DTO与Datafeed查询执行器，由PostgreSQL/TimescaleDB完成有界窗口聚合，校验事件时间、可得时间及最新revision；预测只启用明确注册的确定性 `mean-price-v1`，拒绝任意Python自动并行。当前完整重算请求窗口以保证迟到修订正确，未声称已实现Redis热缓存或增量分片worker。ignored真实PG测试覆盖未来排除、迟到修订及sum/count分片归并等价。

`rust-policy` 通过正式 FED/TREASURY RSS adapter 对官方公共接口执行真实只读HTTPS请求，必须读到包含真实时间的条目，网络失败直接失败，不替换成mock；`rust-boundaries` 另验证PG显式源revision及canonical消息身份保留。新增门禁有独立证据，早前完整服务链报告不代表运行过这些新增项。

`rust-market` 经实际SDK读取Binance与Hyperliquid成交事实；`rust-boundaries` 另要求双源主动LATEST落盘/回复原子性、只读已持久事实的AS_OF、重复请求和多品种独立检查点。公网失败仍计失败。原交易场景仍使用其明确的Binance品种；主动双所契约只读取HYPE/USDC，不下订单。

MinIO固定版本及官方哈希见 `config/minio/provenance.json`。公共镜像无法拉取时可通过 `MARKETPULSE_MINIO_IMAGE` 使用可信镜像；本地此次验证使用同版本官方GitHub二进制且已核验官方SHA-256，不将这项镜像部署限制算成容器部署通过。

`kubernetes-prepare --timeout 300` 使用已构建Rust二进制、本机Python3.12、ELF依赖、CA和固定版本mock依赖制作 `FROM scratch` 本地验收载体并导入K3s，记录二进制和文件系统哈希；不运行Cargo构建，只读取metadata依赖关系，不把本机ABI载体冒充可移植release或Docker provider验收。独立 `sigbot-marketpulse-workers` namespace 部署真正Twitter边界及Secret/ConfigMap，并在Pod内检查既有PG/EMQX连通。新闻使用本次部署生成且重启稳定的真实schema合规ID，避免与旧fixture事实碰撞。

准备后source `/tmp/sigbot-marketpulse-workers/test.env`，通过 `--scenario rust-deployment --reuse-build --timeout 360` 运行正式Rust Kubernetes provider测试：要求实际Datafeed落盘和Python回调checkpoint，再验幂等apply、配置更新、Pod恢复、停止及拒绝接管无归属资源。准备通过不等于worker通过；配置/快照文件为0600，秘密不写入镜像或报告。所有Kubernetes命令固定使用 `/etc/rancher/k3s/k3s.yaml`。默认保留；`--cleanup-workers` 只清理该case拥有的namespace和生成配置。

准备报告 `20261028145106` 已实际通过镜像导入、外部边界就绪及Pod到PG/EMQX连通。该报告显式锁定22:45的readiness诊断二进制，不包含随后Order owner及主动快照改动，不算最终worker验收。`20261028144624` 与 `20261028144951` 保留了首轮Debian依赖文件缺失和源码新鲜度拦截的失败记录。缺少Python RECORD的Debian包现使用实际dpkg文件清单。载体默认拒绝旧二进制；只有 `MARKETPULSE_CARRIER_BINARY_SHA256` 可显式指定并记录诊断基线，Rust测试的新鲜度门禁仍保持开启。

2026-10-28 本地证据：报告 `20261028112927` 的 13 个场景连续两轮全部通过（26 次），包含真实 Rust 服务链、崩溃恢复、Python stdout 归档、固定历史回放、双账户补偿和 Chromium UI；`20261028104417` 两个 Rust 场景的三个真实集成测试通过。`20261028104434` 明确记录缺少真实 Authguard 浏览器会话导致认证 UI 失败。之前失败报告全部保留；不能据此声称官方授权 Truth schema、认证 UI 部署及全部生产能力已经完成。

`20261028111408` 两轮独立历史回放通过，每轮24步、12份固定AI输出、2笔相同成交结果，回放owner重启幂等；`20261028112439` 真实同所双账户补偿及崩溃/重投场景通过。日志证据仅覆盖真实 Python stdout 经 Rust 宿主归档，不等于自动收集所有 Rust tracing。上述结果不代替真实认证 UI 门禁。

报告 `20261028140121` 的 19 个场景两轮共38次全部通过，包含18张真实 Chromium 截图与4个隔离回放 run。每个回放场景向两个 run 提供完全相同的冻结历史/AI/撮合参数，逐笔比较成交并核对真实历史 tick；实时异步查询延迟与虚拟时钟不同，不再以二者价格必然相等作为验收条件。`20261028134937` 保留报告解析及该比较口径导致的失败记录。`rust-effects` 验证真实 Python 账户回调及通知/导出的原子提交；`rust-resting` 验证挂单后续 tick 撮合。独立的 `rust-export` 使用回环19092上的真实 Kafka 验证导出、重启去重与乱序游标，不冒充 Google Sheets 或多 broker 故障验收。

`legacy-migration` 在专用 PostgreSQL 16+ 容器中验证非空旧数据升级。默认容器为 `sigbot-migration-check`；`MARKETPULSE_MIGRATION_CONTAINER` 可选择同名前缀的其他独立容器，`MARKETPULSE_MIGRATION_COMMAND` 可选择本机 `podman`、`docker` 或其 `sudo -n` 形式（默认 `sudo -n podman`）。容器需支持本地 Unix socket 上 `postgres` 角色的非交互管理，场景不会读取或要求数据库秘密，也不会创建/删除容器或镜像。它拒绝含其他业务数据库的容器，容器内已有数据库只允许 `postgres`、`sigbot_check_*`、`sigbot_legacy_probe_*`，并固定已核验的容器 ID。

运行 `python runner.py --scenario legacy-migration --rounds 2 --timeout 120 --reuse-build`：正式 `migrate.py` 先应用4份2024/2025基表，导入 `config/sigbot/legacy_migration.sql` 的明确合成合法历史数据，再应用当前全部21份迁移并重跑。15项检查覆盖 UUID Workflow/Run 映射，钱包/账本/余额/仓位/净值字段保留，22位整数精度、外键、旧账本不可变、不伪造新资金事实及重复升级幂等。每轮独立随机临时数据库在成功或异常的 `finally` 中清理；清理失败会使场景失败并保留数据库名供排查。日志、渲染SQL、夹具/迁移/runner SHA-256 和检查结果写入本轮 `reports/<时间>/legacy-migration/`；这不替代生产全量历史数据升级或灾备验收。

`rust-extensions` 使用专用 `deploy/postgres-extensions/docker-compose.yaml`，真实启动 TimescaleDB + pgvector PostgreSQL（1 CPU、1 GiB、回环15440），应用全部迁移并执行独立 Rust `compute_extensions` 测试。只需先统一构建 `cargo test -p sigbot-datafeed --test compute_extensions --no-run`，再运行 `python runner.py --scenario rust-extensions --reuse-build --timeout 180`；场景不会构建其他服务。可用 `MARKETPULSE_EXTENSIONS_COMMAND=podman|docker|"sudo -n podman"|"sudo -n docker"` 选择本机引擎，默认 `sudo -n podman`，需安装其 Compose provider。镜像缺失时按固定digest拉取，拉取前要求至少9 GiB可用磁盘。此fixture限定实际验收的 linux/amd64 子镜像。容器、卷有独立名称与owner标签；不接管无标签资源，成功或失败都会删除本fixture容器和卷，保留报告，不删除镜像。密码由本轮随机生成，仅存临时0600文件并传给测试子进程，结束后清理。

普通 Case Compose 和正式 Helm 已统一固定官方 Timescale HA 多架构 index digest（`config/postgres/provenance.json`记录index与实际amd64 child），但当前运行中的旧plainPG数据库没有被替换。普通Case改用**新的**命名卷 `sigbot-marketpulse-postgres-ha-pg16-v1`，原 `postgres-data` 卷保持原样。不要直接对现有环境执行 `--deploy` 期待自动搬迁：先备份/导出，准备全新HA卷，恢复并对账，再显式切换服务连接。HA `PGDATA=/home/postgres/pgdata/data/pgdata`；不能把旧 `/var/lib/postgresql/data` 挂载原封不动换镜像。

初次空卷由共享 `tooling/deploy/helm/sigbot/bootstrap/postgres-extensions.sql` 安装两扩展。已有非空数据库必须由有扩展owner权限的角色显式执行 `psql --no-psqlrc --set ON_ERROR_STOP=on --file tooling/deploy/helm/sigbot/bootstrap/postgres-extensions.sql`（连接/凭据使用既有安全PG环境或Secret），不依赖init目录重跑。扩展库须已随镜像安装且Timescale已预加载。验收检查真实函数及7天/3天/25小时/365天窗口与PG `date_bin` 同1970 UTC origin等价，包含迟到修订及未来排除；它不证明Evaluator已采用向量相似度检索，原plainPG报告也不追认为扩展验收。

Helm systemdb 按官方HA镜像UID/GID1000设置non-root与fsGroup/OnRootMismatch；渲染已核对，当前独立Compose不代表Kubernetes新PVC权限已实际验收。Bootstrap遇到扩展文件缺失、Timescale未预加载或非安装管理员时明确失败，不静默跳过。

独立双扩展验收报告 `20261028160052` 已通过：实际 TimescaleDB 2.31.0、pgvector 0.8.7，显式owner bootstrap重复执行、21迁移和2项真实Rust函数/计算测试均成功；容器与新卷已清理。报告 `20261028155855` 保留首次timescaledb-tune拒绝20连接配置的失败（其最小值为25，fixture现使用30）。Kubernetes新PVC权限与Evaluator向量检索仍不在此验收范围。
