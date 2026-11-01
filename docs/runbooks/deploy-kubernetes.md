# Deploy on Kubernetes

Use the existing [Authguard migration runbook](authguard-migration.md) to supply actual application/Gateway domains, routes and Secret references. The chart does not invent business domains or credentials. Bootstrap Secrets before installation and stop on any bootstrap error.

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
export IMAGE_SOURCE=global # Use aliyun for the CN profile selected by make image-source.
python3 deploy/images.py --source "$IMAGE_SOURCE" --format helm > /tmp/sigbot-image-values.json
make lint-helm
# Populate the bootstrap env-file without printing it. Choose exactly one
# provider bootstrap; each validates the same canonical payload and must pass.
deploy/helm/sigbot/bootstrap/setup-secret-k8s.sh \
  --namespace sigbot --secret-file /private/sigbot-secrets.env
helm upgrade --install sigbot deploy/helm/sigbot --namespace sigbot --create-namespace \
  -f /tmp/sigbot-image-values.json -f /path/to/actual-business-values.yaml
```

AWS Secrets Manager, GCP Secret Manager and Vault use the corresponding
`bootstrap/setup-secret-{aws,gcp,vault}.sh` scripts. They maintain one
canonical payload and optionally mirror it to the Kubernetes Secret consumed
by the chart; they do not create a second values tree. Start from
`bootstrap/sigbot-secrets.env.example`. Use AuthGuard's own bootstrap script
for its independent `authguard-secrets` payload, or pass that private file with
`--authguard-secret-file` when using the Kubernetes bootstrap.

The chart is organized by owned domain under `templates/api`, `controller`,
`middleware`, `platform`, `gateway`, `web`, and `observability`. Enable
`sigbot.monitoring.serviceMonitor.enabled` when the Prometheus Operator CRDs exist;
enable `sigbot.monitoring.grafana.enabled` for sidecar-discovered dashboards covering
all nine data-plane components plus the central Controller.

The shared image catalog overrides `sigbot`, `middleware`, and the tenant middleware image catalog through one generated values object. The Aliyun profile rejects foreign runtime/middleware images. Keep the profile values with the release artifact when using a mirror; packaging a chart does not pull or prove the mirror images are available.

TimescaleDB and pgvector are extensions of the same PG instance. Use the new PGDATA volume layout and explicit extension bootstrap, then ordered migrations; do not adopt old financial volumes automatically. API owns the worker controller, and shared components are embedded in service hosts. Enable worker deployment with scoped runtime Secrets, a persistent log volume and bounded resources/network rules. Run `make e2e-k8s` for actual case-owned worker lifecycle acceptance.
