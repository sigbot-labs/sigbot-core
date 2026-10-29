# Workflow service hosts

The deployer is a shared Rust component embedded in the API host when `services.deployer.workers.enabled` is true. It does not receive its own Helm Deployment. Docker and Kubernetes remain the provider implementations under their existing directories. The old `standalone` name is a compatibility alias for Docker isolation; direct service CLI commands remain available for local development.

`POST /api/v1/workflows/{id}/start` locks the scoped workflow and atomically inserts a `workflow_run` with an immutable graph snapshot before marking the workflow `PENDING`. Repeating start while that run is active returns the same `run_id`. The controller scans only its configured environment, tenant and space. PostgreSQL row locks prevent concurrent controllers from deploying the same workflow. Stop is complete only after the provider has removed every worker; it then marks both workflow and run stopped.

Each node supplies `config.module` and `config.argument_json`. The latter is the existing service argument DTO, either a JSON object or a JSON string, with credentials expressed only as environment-variable references. Before persistence, shared validation rejects plaintext secret maps and custom process environments. At deployment, the controller overwrites scope, workflow, node and run fields, including nested strategy identity and account-service runtime configuration. Graph connections accept an optional `kind` containing the v2 topic enum name. Older connections retain module-based routing. Strategy-to-datafeed edges authorize snapshot/compute requests, and datafeed return edges authorize bounded results.

Kubernetes uses the official `kube` client and server-side apply. Deterministic names, `Recreate`, one replica and stable specification hashes prevent duplicate decision owners. Hosts run without service-account tokens, root privileges, extra capabilities or a writable root filesystem. Requests/limits, management probes, scratch quotas and a deny-by-default NetworkPolicy bound the hosts. DNS is allowed; operators must explicitly configure middleware/venue CIDRs and ports. Python strategy execution uses the separately packaged Rust worker's existing process sandbox. Kubernetes PID limits also require the operator's kubelet `podPidsLimit`; the chart does not change host kubelet policy.

Backtest execution hosts receive a stable, internal ClusterIP Service on their configured `bind_addr` port. The host binds that port on `0.0.0.0`; management stays on its separate port. A Paper/Simulated Order node must identify exactly one Backtest node in its `execution_node_ids`. The controller derives its endpoint from that worker's deterministic DNS name and port, which also resolves on the dedicated Docker network. Kubernetes allows the execution port only from those declared Order workers in the same namespace and permits their corresponding outbound connection. Other ingress remains denied. Service ownership is checked before updates/deletion, and stopping a worker removes its Service and policy after its Deployment terminates. The controller Role therefore includes scoped Service CRUD, without permission to change Secrets or namespaces.

Docker uses the official `bollard` client. It compares the stored specification hash, updates by stop/remove/create, restarts failed hosts, and polls actual container health. CPU, memory, PID count, temporary filesystems and capabilities are bounded. It requires an operator-provisioned network labelled `sigbot.io/scope=<env>:<tenant>:<space>`; `host` and the default bridge are rejected. Docker network egress firewall rules are an operator prerequisite, whereas the Python process itself remains unable to open network connections. A mode-0600 runtime env file supplies credentials to the Rust host; it is never shell-evaluated or logged.

Example configuration (the regular application defaults and Secret environment still apply):

```yaml
authguard:
  service_tenant_id: 1
  service_space_id: 1
  service_env: paper
  service_principal_id: workflow-controller
services:
  deployer:
    cron: "0/10 * * * * *"
    images:
      sigbot: registry.example.invalid/sigbot:reviewed-version
    workers:
      enabled: true
      provider: kubernetes
      namespace: sigbot
      runtime_secret: sigbot-workers
      runtime_config_map: sigbot-runtime
      cpu_millis: 1000
      memory_mb: 512
      pids_limit: 64
      reconciliation_timeout_seconds: 45
      egress_cidrs: ["10.42.0.0/16"]
      egress_ports: [1883, 5432, 6379]
```

The image above is an illustrative configuration value, not a deployment target. Use the actual reviewed image and actual cluster network ranges. Middleware stays under Helm ownership: TimescaleDB and pgvector are extensions in the existing PostgreSQL instance. The runtime controller cannot create namespaces, databases, Secrets, PVCs or shared-library deployments.

After rotating a worker Secret or Docker env file, increment `workers.secret_revision` (`secrets.revision` in Helm). It participates in the specification hash, so the controller replaces existing hosts and they load the new credentials. Keep old submission-encryption key versions until all orders that use them are terminal.

Enabling Strategy `log_archive` requires an existing persistent volume for that worker. Set `workers.log_pvc_prefix` (Helm `deployer.worker_log_pvc_prefix`) or `workers.docker_log_volume_prefix`; the volume name is that prefix followed by the deterministic worker name. Provision it with label `sigbot.io/worker=<worker-name>` and UID/GID 1000 write access. Each worker gets its own local/block filesystem supporting SQLite WAL; do not use an NFS-shared SQLite file. The controller verifies the volume and mounts it at `/var/lib/sigbot/logs`, rewriting the spool path to `archive.sqlite` there. Missing configuration or an incorrectly owned volume fails deployment. Replacement and stop retain the volume; operators reclaim it only after archived logs are verified. Kubernetes grants the controller `get` on PVCs, never create/delete. Temporary scratch storage remains separate from this durable log buffer.

`cargo test -p sigbot-deployer --test worker_plan_test` checks scope override resistance, stable reconciliation plans, typed edge routing, single-owner manifests, execution Service routing and Secret references. `worker_lifecycle` is an explicit integration target for actual Kubernetes containers, PostgreSQL, EMQX and Python execution; its environment is prepared through the Marketpulse case runner. Docker/Kubernetes lifecycle acceptance requires the real daemon/cluster and its configured runtime Secret; pure manifest tests do not establish that acceptance.
