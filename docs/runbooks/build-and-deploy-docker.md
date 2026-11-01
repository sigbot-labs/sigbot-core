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

Compose starts one PostgreSQL instance with TimescaleDB/pgvector extensions, applies ordered migrations, and then starts the API. Redis, EMQX, MinIO and Web use the same image catalog and explicit resource limits. The controller and shared Messager/Exchange/Logservice components run inside service hosts. The new PGDATA layout uses the separate `storage-v2` volume; do not adopt or delete old financial volumes automatically. `docker-down` retains volumes.

Supply the real Authguard trusted-context configuration and route the API/Web through the actual Gateway/Hosted Login domains described in [the Authguard migration runbook](authguard-migration.md). Compose's health or unauthenticated UI screenshots do not establish a working authenticated browser session. Private management ports are not published to the host.
