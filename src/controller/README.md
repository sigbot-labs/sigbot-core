# Central Workflow controller

The controller is a platform service deployed independently by Helm when `controller.enabled` is true. One `CentralController` owns scheduling and routes every Workflow to the Docker or Kubernetes provider recorded by its selected `ctl_worker`; there is no process-wide runtime provider switch.

`POST /api/v1/workflows/{id}/start` locks the scoped workflow and atomically inserts a `wf_run` with an immutable graph snapshot before marking the workflow `PENDING`. Repeating start while that run is active returns the same `run_id`. On the first claim, the controller selects an online Worker in the same environment and tenant, preferring a Workflow-specific target over a shared target and enforcing capacity. The selected `worker_id` is pinned to the run for retries, recovery and stop. PostgreSQL row locks and `SKIP LOCKED` prevent concurrent controllers from deploying the same workflow. Stop is complete only after the bound provider has removed every worker; it then marks both workflow and run stopped.

The API manages targets at `/api/v1/workers`. `auth_config` is a tagged JSON object whose `auth_type` is `docker_anonymous`, `docker_jwt`, `kubernetes_jwt` or `kubernetes_private_key`. Authentication fields contain only host Secret environment-variable names; JWT, certificate and private-key values are never stored in `ctl_worker`. Docker endpoints accept `unix`, `http` or `https`; Kubernetes endpoints require `https`. A Worker may be shared within a tenant or bound to one Workflow and has explicit priority, capacity, heartbeat and lifecycle status.

Each node supplies `config.module` and `config.argument_json`. The latter is the existing service argument DTO, either a JSON object or a JSON string, with credentials expressed only as environment-variable references. Before persistence, shared validation rejects plaintext secret maps and custom process environments. At deployment, the controller overwrites scope, workflow, node and run fields, including nested strategy identity and account-service runtime configuration. Graph connections accept an optional `kind` containing the v2 topic enum name. Connections without `kind` use module-based routing. Strategy-to-datafeed edges authorize snapshot/compute requests, and datafeed return edges authorize bounded results.

Kubernetes uses the official `kube` client and server-side apply. Deterministic names, `Recreate`, one replica and stable specification hashes prevent duplicate decision owners. Hosts run without service-account tokens, root privileges, extra capabilities or a writable root filesystem. Requests/limits, management probes, scratch quotas and a deny-by-default NetworkPolicy bound the hosts. DNS is allowed; operators must explicitly configure middleware/venue CIDRs and ports. Python strategy execution uses the separately packaged Rust worker's existing process sandbox. Kubernetes PID limits also require the operator's kubelet `podPidsLimit`; the chart does not change host kubelet policy.

Backtest execution hosts receive a stable, internal ClusterIP Service on their configured `bind_addr` port. The host binds that port on `0.0.0.0`; management stays on its separate port. A Paper or Backtest Order node must identify exactly one Backtest node in its `execution_node_ids`. The controller derives its endpoint from that worker's deterministic DNS name and port, which also resolves on the dedicated Docker network. Kubernetes allows the execution port only from those declared Order workers in the same namespace and permits their corresponding outbound connection. Other ingress remains denied. Service ownership is checked before updates/deletion, and stopping a worker removes its Service and policy after its Deployment terminates. The controller Role therefore includes scoped Service CRUD, without permission to change Secrets or namespaces.

Docker uses the official `bollard` client. It compares the stored specification hash, updates by stop/remove/create, restarts failed hosts, and polls actual container health. CPU, memory, PID count, temporary filesystems and capabilities are bounded. It requires an operator-provisioned network labelled `sigbot.io/scope=<env>:<tenant>`; `host` and the default bridge are rejected. Docker network egress firewall rules are an operator prerequisite, whereas the Python process itself remains unable to open network connections. A mode-0600 runtime env file under `/run/sigbot/workers/<environment_file>.env` supplies credentials to the Rust host; `environment_file` must begin with `tenant-<tenant_id>-`, and the file is never shell-evaluated or logged.

Example configuration (the regular application defaults and Secret environment still apply):

```yaml
authguard:
  service_principal_id: workflow-controller
services:
  controller:
    cron: "0/10 * * * * *"
    images:
      sigbot: registry.example.invalid/sigbot:reviewed-version
    workers:
      enabled: true
      cpu_millis: 1000
      memory_mb: 512
      pids_limit: 64
      reconciliation_timeout_seconds: 45
```

The runtime configuration above describes resource policy, not a target cluster. Targets are created through the Worker API, for example:

```json
{
  "env": "paper",
  "workflow_id": null,
  "name": "tenant-kubernetes",
  "provider": "kubernetes",
  "conn_url": "https://cluster.example.invalid:6443",
  "auth_config": {
    "auth_type": "kubernetes_jwt",
    "token_env": "TENANT_KUBERNETES_TOKEN",
    "certificate_authority_env": "TENANT_KUBERNETES_CA"
  },
  "configuration": {
    "runtime_type": "kubernetes",
    "namespace": "tenant-1",
    "runtime_secret": "sigbot-runtime",
    "runtime_config_map": "sigbot-runtime",
    "service_account": "sigbot-worker",
    "secret_revision": "1",
    "log_pvc_prefix": "tenant-1-logs-",
    "egress_cidrs": ["10.42.0.0/16"],
    "egress_ports": [1883, 5432, 6379, 443]
  },
  "labels": {},
  "priority": 100,
  "max_concurrent_workflows": 16
}
```

The image above is an illustrative configuration value, not a deployment target. Use the actual reviewed image and actual cluster network ranges. Middleware stays under Helm ownership: TimescaleDB and pgvector are extensions in the existing PostgreSQL instance. The runtime controller cannot create namespaces, databases, Secrets, PVCs or shared-library deployments.

After rotating a remote runtime Secret or Docker environment file, increment that Worker's `configuration.secret_revision` or update its typed Docker configuration. The configuration participates in the specification hash, so the controller replaces existing hosts and they load the new credentials. Keep old submission-encryption key versions until all orders that use them are terminal.

Enabling Strategy `log_archive` requires an existing persistent volume for that worker. Set `configuration.log_pvc_prefix` for Kubernetes or `configuration.log_volume_prefix` for Docker; the volume name is that prefix followed by the deterministic worker name. Provision it with label `sigbot.io/worker=<worker-name>` and UID/GID 1000 write access. Each worker gets its own local/block filesystem supporting SQLite WAL; do not use an NFS-shared SQLite file. The controller verifies the volume and mounts it at `/var/lib/sigbot/logs`, rewriting the spool path to `archive.sqlite` there. Missing configuration or an incorrectly owned volume fails deployment. Replacement and stop retain the volume; operators reclaim it only after archived logs are verified. Kubernetes grants the controller `get` on PVCs, never create/delete. Temporary scratch storage remains separate from this durable log buffer.

`cargo test -p sigbot-controller --test worker_plan_test` checks scope override resistance, stable reconciliation plans, typed edge routing, single-owner manifests, execution Service routing and Secret references. `worker_lifecycle` is an explicit integration target for actual Kubernetes containers, PostgreSQL, EMQX and Python execution; its environment is prepared through the Marketpulse case runner. Docker/Kubernetes lifecycle acceptance requires the real daemon/cluster and its configured runtime Secret; pure manifest tests do not establish that acceptance.
