# 部署

[English](deployment.md) · [文档索引](../index_CN.md) · [排障](troubleshooting_CN.md)

## 前置依赖

- 在仓库根目录执行，准备 Python 3.12、Docker Compose v2 或 Helm 3/kubectl，以及可访问的目标环境。从源码构建另需[开发工具链](../development/build-and-test.md)。
- 配置真实 Gateway/Application 域名、TLS/HTTPS 回跳、Authguard Policy 和私有 Secret 文件。浏览器 `/auth/login`、`/auth/account/security`、`POST /auth/logout` 走同源 Authguard 路由；HTTP、A2A 和生成的 MCP 均遵循[身份契约](../architecture/platform/api/api_CN.md#iam)。
- 启用 Run 前，预置远端 Worker 隔离网络或 Namespace、Runtime Secret、ServiceAccount/Egress 和持久日志存储，通过 API 登记匹配的租户级 `cluster_info`；后续组件/中间件由 [Controller](../architecture/platform/controller/controller_CN.md) 调和。
- 用 `make image-source` 查看镜像源。示例使用 `IN_CN_GFW=false` / `IMAGE_SOURCE=global`；CN 环境改为 `true` / `aliyun`。`deploy/images.py` 是唯一 Catalog。需要时设置 `HTTPS_PROXY=http://127.0.0.1:8800`；新镜像通过既定跳板拉取流程，例如 `~/mirror-pull.sh <image>`。

<a id="docker"></a>

## Docker Compose

首次部署前，将 `deploy/docker/.env.example` 复制为 `deploy/docker/.env`，私下填写真实 Secret/镜像值并设权限 `600`。单独准备 Authguard/Gateway；Compose 健康不代表浏览器登录可用。

配置就绪后执行：

```bash
set -eu
make build-image IN_CN_GFW=false VERSION=1.0.0
make docker-up IN_CN_GFW=false
make docker-logs
```

`build-image` 构建 Runtime、隔离 Python Worker 与 Web 镜像。其他批准的 Registry 可覆盖 `SIGBOT_IMAGE`/`SIGBOT_WEB_IMAGE`。Rootful Podman 可配置 `CONTAINER_CLI='sudo -n --preserve-env podman'` 和 Compose v2 Provider；构建传递 Proxy、Version、Commit 与受限 Cargo 并发。

Compose 启动控制面 PostgreSQL/Redis、执行迁移，再启动 API、独立 Controller 和 Web；EMQX/MinIO 为本地 Fixture。九个数据面组件依赖已登记 Worker，由 Controller 动态启动；预创建并带归属标签的 Docker 网络/env-file 必须与记录匹配，不回退默认 Bridge。`make docker-down` 停止环境并保留卷，包括 PostgreSQL 的 `storage-v2`。

<a id="kubernetes"></a>

## Kubernetes 与 Helm

先准备真实 Values 和私有 Secret 文件，替换下面两个示例路径；Bootstrap 失败必须中止安装。

```bash
set -eu
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
export IMAGE_SOURCE=global
SIGBOT_SECRET_FILE=/private/sigbot-secrets.env
SIGBOT_VALUES_FILE=/path/to/actual-business-values.yaml
python3 deploy/images.py --source "$IMAGE_SOURCE" --format helm > /tmp/sigbot-image-values.json
make lint-helm
deploy/helm/sigbot/bootstrap/setup-secret-k8s.sh \
  --namespace sigbot --secret-file "$SIGBOT_SECRET_FILE"
helm upgrade --install sigbot deploy/helm/sigbot --namespace sigbot --create-namespace \
  -f /tmp/sigbot-image-values.json -f "$SIGBOT_VALUES_FILE"
```

AWS/GCP/Vault 使用 `bootstrap/setup-secret-{aws,gcp,vault}.sh`，共同遵循 `bootstrap/sigbot-secrets.env.example` 的 Canonical Payload；可选同步 Kubernetes Secret，不另建 Values 体系。Authguard 独立维护 Payload/Bootstrap，需要时向 Kubernetes Bootstrap 传 `--authguard-secret-file`。不要提交填好的 env 文件。

<a id="authguard"></a>

## 部署设置

Chart 顶层只有 `global`、`sigbot`、`middleware`、`secrets`。除非已有兼容的外部 IAM/Gateway，保持 Authguard 启用。`sigbot.api.gateway.hostnames`、Authguard Application Hosts/HTTPS Return URI 和 AuthN/Web 路由使用同一真实 Origin；跨 Namespace Gateway 配置 `allowedRoutes` 和 Reference Grant。轮换 Secret 时递增 `secrets.revision`。

只有安装 Prometheus Operator CRD 后才启用 `sigbot.monitoring.serviceMonitor.enabled`；`sigbot.monitoring.grafana.enabled` 提供 Controller 加九组件仪表盘。管理监听保持私网。[存储与 Bootstrap 边界](../architecture/overview_CN.md#deployment-contracts)与租户中间件分离。

用 `make e2e-docker` / `make e2e-k8s` 验收，并核对[带日期的证据与缺口](../development/handover.md#acceptance)；渲染成功或平台健康不表示租户 Run 或 Hosted Login 全链通过。
