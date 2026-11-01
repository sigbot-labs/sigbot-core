# Build and deploy with Docker

Use Docker Compose v2. Select images through the shared catalog; the Makefile detects the CN registry restriction or accepts an explicit override.

```bash
make build-image IN_CN_GFW=false VERSION=1.0.0
cp deploy/docker/.env.example deploy/docker/.env
chmod 600 deploy/docker/.env
# Supply the actual Secret values in .env without committing them.
make docker-up IN_CN_GFW=false
make docker-logs
make docker-down
```

`build-image` builds the runtime (including the isolated Python worker) and Web images. Override `SIGBOT_IMAGE`/`SIGBOT_WEB_IMAGE` for another registry, or choose `IMAGE_SOURCE=aliyun` to require Aliyun addresses throughout. Builds propagate `HTTPS_PROXY`, commit, version and a bounded Cargo job count. A rootful Podman setup can use `CONTAINER_CLI='sudo -n --preserve-env podman'` with a Compose v2 provider; environment preservation carries the selected image references without putting credentials in argv.

Compose starts the control-plane PostgreSQL/Redis, applies the final-state migration, then starts API, the independent Controller and Web. EMQX and MinIO are local development fixtures used by dynamically deployed data-plane components; they are not evidence that production tenant middleware has been reconciled. Order and Wallet are not Compose platform services: all nine data-plane components are started by Controller from persisted Workflow/Worker configuration. Messager and Exchange are embedded libraries; Logservice is one of those nine dynamic components. The new PGDATA layout uses the separate `storage-v2` volume, and `docker-down` retains volumes.

Before `docker-up`, register a Docker `ctl_worker` whose network and environment file match this fixture. The Controller requires the pre-created labelled worker network and will not silently fall back to the default bridge. A healthy API/Controller container without a matching Worker does not imply that any Workflow component has started.

Supply the real Authguard trusted-context configuration and route the API/Web through the actual Gateway/Hosted Login domains described in [the Authguard migration runbook](authguard-migration.md). Compose's health or unauthenticated UI screenshots do not establish a working authenticated browser session. Private management ports are not published to the host.
