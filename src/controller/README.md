# Central Workflow Controller

Controller 是独立平台服务。API 负责经过 Authguard 授权的 Worker/Workflow CRUD 与
Run 创建；Controller 使用平台 service principal 扫描待处理 Run，在对应租户 Worker
上调和九个数据面组件及其租户中间件。Controller 不嵌入 API，也不借用用户上下文。

## Run 与 Worker 选择

`POST /api/v1/workflows/{id}/start` 在 Core Repository 中锁定作用域 Workflow，保存
不可变图快照并创建或返回活动 `run_id`。Controller 使用 PostgreSQL 行锁与
`SKIP LOCKED` 保证同一 Run 只有一个部署 owner，再按以下规则选择 `cluster_info`：

1. `env` 和 Authguard 租户必须完全相同；
2. Workflow 专用 Worker 优先于租户共享 Worker；
3. Worker 必须在线且未超过 `max_concurrent_workflows`；
4. 选定的 `cluster_id` 固定到该 Run，用于恢复、更新和停止。

Controller 从不可变图生成 WorkerPlan。当前允许的模块是 `datafeed`、`evaluator`、
`strategy`、`logger`、`order`、`wallet`、`notifier`、`exporter` 和 `backtest`。
订阅者在生产者之前启动；停止在 Provider 确认资源删除后才更新 Run 终态。

## Worker API 与强类型配置

平台 API 暴露：

- `GET/POST /api/v1/workers`
- `GET /api/v1/workers/defaults`
- `DELETE /api/v1/workers/{id}`
- `POST /api/v1/workers/{id}/status`

`auth_config` 是 `deny_unknown_fields` 的 tagged DTO：

- `docker_anonymous`
- `docker_jwt { token_secret }`
- `kubernetes_jwt { token_secret, certificate_authority_secret? }`
- `kubernetes_private_key { client_certificate_secret, private_key_secret,
  certificate_authority_secret }`

字段值是中央 Secret map 中的 key，不是 Secret 明文或任意环境变量表达式。
`configuration` 同样按 Provider 强类型校验：

- Docker：`network`、`environment_file`、可选 `log_volume_prefix`；
- Kubernetes：`namespace`、`runtime_secret`、`runtime_config_map`、
  `service_account`、`secret_revision`、可选日志 PVC 前缀和出站 CIDR/端口。

Web 的表单和 YAML 编辑器提交同一个 `SaveWorker` DTO；未知字段、明文 token/password/
private key、自定义命令或进程环境都会被拒绝。

## 租户中间件

`cluster_info.middleware` 可按租户声明 Redis、pgvector、TimescaleDB 和 EMQX。每个条目
只允许：

```yaml
image: reviewed-registry/example:immutable-tag
resources:
  cpu_millis: 1000
  memory_mb: 1024
  storage_gb: 20
```

Controller 在九组件启动前调用同一 Provider 的 middleware prepare 阶段。资源名称、
标签和规格 hash 都是确定性的；只有带当前 Sigbot ownership 标签的资源可更新或删除。
禁用中间件不会自动删除持久卷/PVC。远端运行 Secret 必须预先存在，Controller 不创建
或读取业务明文 Secret。

控制面 Helm 的 PostgreSQL/Redis 与租户中间件是两个职责域：前者保存平台配置和缓存，
后者服务该租户的数据面 Workflow。不要把租户行情、向量或 MQTT 事实写入控制面库。

## Kubernetes Provider

Kubernetes 使用官方 `kube` client 和 server-side apply。组件 Deployment 固定一个
decision owner，使用确定性名称、`Recreate` 和规格 hash；管理探针只有在消息订阅与
关键消费任务就绪后才报告 ready。Pod 默认无 service-account token、无 root、无额外
capability、只读根文件系统，并配置 request/limit、临时盘和 deny-by-default
NetworkPolicy。DNS 默认允许；远端中间件和交易所出站必须通过 Worker 的批准 CIDR/
端口配置。

Backtest 节点额外获得稳定 ClusterIP Service。Order 的 Paper/Backtest 模式必须声明
唯一的 Backtest execution node；Controller 派生内部 DNS，并只向声明的 Order Pod
开放执行端口。Controller RBAC 仅限所属 namespace 的必要 Deployment、Service、
ConfigMap、Job、StatefulSet、PVC 读取和 NetworkPolicy 权限，不包含 Secret 写权限或
namespace 管理权限。

## Docker Provider

Docker 使用 `bollard`。Controller 要求预先存在且带
`sigbot.io/scope=<env>:<tenant_id>` 标签的隔离网络，拒绝 `host` 和默认 bridge。
容器按规格 hash 幂等替换并检查实际 health；CPU、内存、PID、scratch、capability 和
只读根文件系统受限。Docker 网络出站防火墙由宿主运维负责。

`environment_file` 必须位于 `/run/sigbot/workers/`、权限 0600，并以
`tenant-<tenant_id>-` 开头。文件只作为容器 env-file 使用，不被 shell 执行或打印。
租户中间件使用独立具名卷；停止 Workflow 不会误删租户共享数据。

## 日志持久卷

启用 Strategy/Logger 的本地日志归档时，每个 Worker 必须有独占、支持 SQLite WAL
的持久文件系统：Kubernetes 用 `log_pvc_prefix`，Docker 用 `log_volume_prefix`。
Controller 校验 ownership 并挂载到 `/var/lib/sigbot/logs`。缺失或错误归属会拒绝部署；
替换/停止保留卷，只有归档校验完成后才能由运维回收。不要让多个 Worker 共享一个
NFS SQLite 文件。

## 验证与证据边界

```bash
cargo test -p sigbot-controller --test worker_lifecycle
python3 use-cases/marketpulse-autonomy-quantification/e2e/runner.py \
  --scenario kubernetes-prepare --scenario rust-deployment \
  --deployer kubernetes
```

单元/清单测试覆盖作用域覆盖抵抗、Secret 引用、确定性计划、owner 保护、执行 Service
路由和中间件拓扑。真实 K3s 用例还需要产品镜像、已迁移 PostgreSQL、EMQX、远端
runtime Secret 和 `KUBECONFIG=/etc/rancher/k3s/k3s.yaml`。清单测试通过不等于真实
集群生命周期验收；最新已执行范围和缺口统一记录在
[`docs/development/HARDCOVER.md`](../../docs/development/HARDCOVER.md)。
