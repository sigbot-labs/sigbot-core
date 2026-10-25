# Build and Deploy with Docker

[Documentation index](../index.md)

## Build the Application Image

The Makefile builds one application image. Override `IMAGE` or `PLATFORM` when
publishing to a different registry or architecture.

```bash
make build-image

# Example override
make build-image \
  IMAGE=registry.example.com/sigbot/sigbot:1.0.0 \
  PLATFORM=linux/amd64
```

Authenticate with the registry explicitly, then push the same image reference:

```bash
docker login registry.example.com
make push-image IMAGE=registry.example.com/sigbot/sigbot:1.0.0
```

The build accepts `FEATURES`, `BUILD_MIRROR_URL`, and `HTTPS_PROXY` as optional
overrides. It does not perform registry login or manage credentials.

## Run with Docker Compose

```bash
# Start, inspect, and stop the stack.
make compose-up
make compose-ps
make compose-down
```

Compose uses `tooling/deploy/docker/docker-compose.yaml` and receives the same
`IMAGE` value used by the image targets. These targets never delete volumes;
destructive data cleanup must be performed explicitly with Docker tooling.
