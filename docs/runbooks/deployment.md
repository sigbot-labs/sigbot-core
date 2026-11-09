# Deployment

[中文](deployment_CN.md) · [Documentation index](../index.md) · [Troubleshooting](troubleshooting.md)

## Prerequisites

- Run commands from the repository root with Python 3.12, Docker Compose v2 or Helm 3/kubectl and a reachable target. Source builds additionally require the [development toolchain](../development/build-and-test.md).
- Supply actual Gateway/Application hostnames, TLS/HTTPS return URIs, Authguard policies and private Secret files. Keep browser `/auth/login`, `/auth/account/security` and `POST /auth/logout` on the same-origin Authguard route. [Identity requirements](../architecture/platform/api/api.md#iam) apply to HTTP, A2A and generated MCP.
- Provision the remote Worker's isolated network or namespace, runtime Secrets, ServiceAccount/egress rules and persistent log storage before enabling a Run. Register matching tenant-scoped `cluster_info` through the API. [Controller](../architecture/platform/controller/controller.md) owns subsequent component/middleware reconciliation.
- Use `make image-source` to inspect registry selection. Examples use `IN_CN_GFW=false` / `IMAGE_SOURCE=global`; choose `true` / `aliyun` for the CN profile. `deploy/images.py` is the only image catalog. Set `HTTPS_PROXY=http://127.0.0.1:8800` when required; new images must use the configured jump-host mirror-pull workflow, such as `~/mirror-pull.sh <image>`.

<a id="docker"></a>

## Docker Compose

Before first deployment, copy `deploy/docker/.env.example` to `deploy/docker/.env`, set its real Secret/image values privately and restrict permissions to `600`. Prepare Authguard/Gateway separately; Compose health does not establish an authenticated browser session.

Run this block after configuration is ready:

```bash
set -eu
make build-image IN_CN_GFW=false VERSION=1.0.0
make docker-up IN_CN_GFW=false
make docker-logs
```

`build-image` builds the runtime, isolated Python worker and Web image. Override `SIGBOT_IMAGE`/`SIGBOT_WEB_IMAGE` for an approved registry. Rootful Podman can set `CONTAINER_CLI='sudo -n --preserve-env podman'` with a Compose v2 provider. Builds propagate proxy/version/commit and bounded Cargo jobs.

Compose starts control-plane PostgreSQL/Redis, applies migrations, then starts API, independent Controller and Web. EMQX/MinIO are local fixtures. All nine data-plane components require a registered Worker and are dynamically started by Controller. The pre-created labelled Docker network/env-file must match that Worker; there is no default-bridge fallback. `make docker-down` stops the fixture and retains volumes, including the `storage-v2` PostgreSQL data volume.

<a id="kubernetes"></a>

## Kubernetes and Helm

Prepare real values and private Secret files before running this block; replace the two example paths. Bootstrap failure aborts installation.

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

AWS/GCP/Vault use `bootstrap/setup-secret-{aws,gcp,vault}.sh` with the same canonical payload from `bootstrap/sigbot-secrets.env.example`; optional Kubernetes mirroring does not create another values tree. Authguard has its own payload/bootstrap; pass `--authguard-secret-file` to the Kubernetes bootstrap when appropriate. Never commit populated env files.

<a id="authguard"></a>

## Deployment settings

The chart has four top-level groups: `global`, `sigbot`, `middleware`, `secrets`. Keep Authguard enabled unless an external compatible deployment owns IAM/Gateway. Set `sigbot.api.gateway.hostnames`, Authguard Application hosts/HTTPS return URIs and AuthN/Web routes to the same actual origin; configure `allowedRoutes` and reference grants for cross-namespace Gateways. Secret rotation increments `secrets.revision`.

Enable `sigbot.monitoring.serviceMonitor.enabled` only with Prometheus Operator CRDs; `sigbot.monitoring.grafana.enabled` exposes Controller plus nine component dashboards. Management listeners stay private. [Storage/bootstrap boundaries](../architecture/overview.md#deployment-contracts) remain distinct from tenant middleware.

Use `make e2e-docker` / `make e2e-k8s` for acceptance. Consult the [dated evidence and gaps](../development/handover.md#acceptance); successful rendering or a healthy platform does not prove a full tenant Run or Hosted Login flow.
