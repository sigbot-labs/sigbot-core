# Marketpulse 自主量化 E2E

沿用 AgentVault case 的部署、场景、报告结构。Twitter、TruthSocial、OpenAI 是明确的外部 mock；内部 Datafeed、Evaluator、Strategy、Order、Wallet、Backtest 使用正式 Rust 组件和隔离 Python worker。唯一执行入口为 `runner.py`，仓库 Makefile 统一发现和运行所有 case。

## 执行

从仓库根目录运行：

```bash
make build test
make e2e-docker E2E_ARGS='--rounds 2' E2E_TIMEOUT=300
make e2e-k8s E2E_ARGS='--reuse-build' E2E_TIMEOUT=360
# 已初始化且归属本 case 的环境才可复用：
make e2e E2E_ARGS='--reuse-environment --reuse-build --rounds 2'
```

`make e2e` 顺序运行 Docker 和 Kubernetes。平台默认场景见 `config/sigbot/matrix.json`：Docker 27 项、Kubernetes 3 项。`runner.py` 本身支持重复/逗号分隔 `--scenario`、`--rounds`、`--deployer docker|kubernetes`、`--timeout`、`--deploy`、`--reuse-build`、`--cleanup-after-run`。例如：

```bash
cd use-cases/marketpulse-autonomy-quantification/e2e
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m playwright install chromium
.venv/bin/python runner.py --scenario runtime,workflow,recovery,logs,web --reuse-build --timeout 300
.venv/bin/python runner.py --stop-runtime --reuse-build
```

`E2E_PYTHON` 可选择已有解释器；`PLAYWRIGHT_CHROMIUM_EXECUTABLE` 可复用兼容 Chromium，旧 `MARKETPULSE_CHROMIUM_PATH` 仍可读。已配置可执行浏览器时不重复下载。`CONTAINER_CLI=docker|podman|"sudo -n podman"` 统一选择本机引擎，`PODMAN_COMPOSE_PROVIDER` 可指定 Compose v2。构建前执行 10 GiB target 守卫，默认两条编译任务；网络受限时设置 `HTTPS_PROXY=http://127.0.0.1:8800`。

## 配置、秘密与数据保留

`config/sigbot/config.yaml` 定义地址、身份和秘密环境变量名。`CaseEnvironment` 统一加载镜像 catalog，在私有 `.runtime/fixture.env` 保存本地生成的测试秘密（目录 0700、文件 0600），报告不输出秘密。已有环境可显式传入数据库、对象存储、MQTT 及 mock 凭据。部署时拒绝相互冲突的数据库密码或非本 case 的远程数据库地址。MQTT 使用限定 env/tenant、有效 4 小时的 JWT；这不是 Authguard 浏览器身份。

镜像选择与根 Makefile 一致：探测或 `IN_CN_GFW` 决定 `IMAGE_SOURCE=aliyun|global`，大陆配置拒绝非 aliyuncs.com 地址。使用的每个镜像仍须实际可获取。默认回环端口：PostgreSQL 15439、EMQX 18883、MinIO 19000、Kafka 19092、Web 15173、三个 mock 18481–18483。所有容器有明确资源上限。

全新 Case Compose 使用固定官方 Timescale HA index、独立 `sigbot-marketpulse-postgres-ha-pg16-v1` 卷，安装 TimescaleDB/pgvector。现存 plain PG 卷不会自动迁移；先备份、在新卷恢复并对账，再显式切换，不能直接换镜像挂旧数据目录。非空 PG 需要具备扩展 owner 权限的 bootstrap，入口为 `deploy/helm/sigbot/bootstrap/postgres-extensions.sql`。当前保留的本地主链仍为 plain PG，双扩展另有实际隔离场景。

MinIO 固定版本、官方二进制哈希见 `config/minio/provenance.json`。公共镜像不可达时，设置 `MARKETPULSE_MINIO_BINARY` 为该官方二进制；部署器核验哈希后制作有限资源、non-root 的本地载体，并记录来源。它是实际 MinIO 容器，不冒充官方 registry 镜像。已有数据卷的 UID/GID 必须与运行用户一致，禁止用递归改写旧数据权限来掩盖问题。

实时主链使用 **Paper** 模式和独立钱包/账户 **20001**。保留旧钱包 1、旧账本及历史身份；回放创建新的 Backtest 钱包。`scope.env` 为部署隔离标识，不决定交易行为。测试只发布外部 FundingFact，由真实 Wallet 入账，不直接种余额。

## 场景与证据

| 场景 | 实际验证 |
| --- | --- |
| `structure,schemas,infra,acl,database` | 12 个 Provider SPI 契约和目录、单文件上限、官方 X schema/兼容 Truth schema、实际中间件、MQTT5 认证/隔离和数据库不可变约束 |
| `boundaries,rust-boundaries,rust-policy,rust-market` | REST/WS/MCP/A2A 的真实请求与 Rust 接入，实际 PG 版本/水位；Fed/Treasury 和 Binance/Hyperliquid 官方公开接口只读请求，网络失败即失败 |
| `rust-cache` | 独立带认证 Redis 单节点与三主节点集群，各执行 11 项真实测试；无凭据拒绝、TTL、位操作、清理均核对 |
| `rust-contracts` | 真实 PG/HTTP/S3 的认证上下文、管理探针、日志持久化、站内通知与 Strategy 边/run 路由；签名发行方是明确夹具 |
| `rust-extensions` | 独立 TimescaleDB + pgvector、显式 owner bootstrap、全部迁移、实际向量/分桶和迟到修订；容器与新卷清理 |
| `rust-messaging,rust-owners` | MQTT 持久补放、相同 run/seq 的新 attempt/span、当前 owner 归属隔离；未知订单只对账、提交及成交查询的模式隔离 |
| `rust-groups,rust-effects,rust-resting,rust-export,rust-logs` | 正式 PG 协调器、Python 回调原子 Effects、后续 tick 限价撮合、真实 Kafka 及 Rust tracing/WAL/对象存储契约 |
| `runtime,workflow` | 真实 8 个 Rust 服务和 Python worker；重复/乱序新闻 → MQTT → 评估 → 快照/计算 → 策略意图 → 风控/Wallet 预占 → 模拟执行 API → 账本 |
| `recovery,replay,groups,logs` | 本 case 进程 SIGKILL/重投、同一冻结历史在独立 run 的经济结果一致、同所双账户补偿、Python stdout 与 Rust tracing 的不可变对象哈希/追加游标 |
| `web` | 真正 Chromium 导航、通知/个人菜单、Workflow 节点/连线和截图，不拦截 API |
| `kubernetes-prepare,rust-deployment` | ABI 匹配载体导入 K3s、Pod 连通检查；正式 Kubernetes provider 实际创建/更新/恢复/停止 Datafeed/Strategy Pod及归属拒绝 |
| `web-full`（单独门禁） | 真实 Authguard 登录后保存、启动、通知与 run/trace/span；必须配置有效 `MARKETPULSE_BROWSER_STORAGE_STATE`，缺少直接失败 |

主链从公开 Binance 响应获取价格，由产品模拟撮合器执行，不发送真实资金交易。`workflow` 仅注入外部新闻；消息证据 fsync 后 ACK，再核对新增订单/成交/非资金 journal 和借贷平衡。`replay` 冻结实际有限历史及已记录 AI 输出，虚拟时钟、排序/费用固定，每场景比较两个隔离 run，重启 owner 验证幂等。异步实时链和历史回放分别验收，不假设查询延迟相同。Snapshot/Compute 使用真实 PG/Timescale 有界查询；当前不宣称已实现 Redis 热缓存、分布式增量分片或任意 Python 自动并行。

旧 Run 的未发送 Outbox 会真实补放，不能删除账本或把过期行情刷新成当前时间来缩短验收。大量积压可能超过短超时；保留失败证据，等待补放完成后使用新的当前输入验收。订单必须继续拒绝过期市场证据。

`--reuse-build` 根据实际测试名、全部传递依赖及 `.d` 输入检查源码新鲜度，记录执行二进制 SHA-256；ignored 测试必须实际执行才可计通过。Kubernetes 始终使用 `KUBECONFIG=/etc/rancher/k3s/k3s.yaml`。Kubernetes 场景不把本机 ABI 调试载体称为可移植 release 镜像，也不代替整条金融链在 K8s 上的验收。

报告写入 `reports/<UTC时间>/`，默认保留环境和失败记录。`--stop-runtime` 只停止 manifest 中二进制、PID 起始时间均匹配的本 case 进程；`--cleanup-workers` 仅清理有归属的 worker namespace；`--cleanup` 删除本 case 的部署和卷，应在证据归档后使用。Redis/扩展临时 fixture 自动清理，正式命名镜像及其他工作负载保留。最新结果统一见仓库的 [验收矩阵](../../../docs/architecture/runtime-v2-acceptance_ZH.md)。

## 外部限制

X 使用 `https://api.x.com/2/openapi.json` 的官方快照；时间、版本和 SHA-256 见 provenance。所有帖子是 schema 合规的明确合成数据。TruthSocial 未取得公开可核验的官方商业 OpenAPI，当前为明确标识的 Mastodon 兼容子集；不能声称已满足两个平台的最新官方 schema。LLM 是固定外部响应，不能证明模型质量。真实 Authguard Hosted Login、完整认证 UI、实盘/跨所成交和真实 SMTP/Telegram/Sheets 仍有独立验收门禁，源码存在不计通过。
