# Build and test

The root Makefile is the entry point. Rust 1.94, Node 22, Python 3.12, Helm 3 and Docker Compose v2 are required. Native Rust dependencies include C/C++, CMake, pkg-config, OpenSSL/libcurl headers and Python development headers. Build `sigbot` and `sigbot-strategy-worker` together; the worker runs in the host's ABI-compatible runtime.

| Command | Scope |
| --- | --- |
| `make build` | Every Rust workspace module and the Web application |
| `make build-web` | Install locked Web dependencies and build |
| `make build-image` | Runtime/worker image and Web image |
| `make fmt` | Rust and Web formatting checks |
| `make lint` | Workspace Clippy correctness gate, TypeScript and Helm; advisory lints remain visible |
| `make lint-helm` | Dependency check, lint, global and Aliyun image-profile rendering |
| `make test` | All workspace unit tests, migration/build-tool tests and Playwright UI checks |
| `make e2e` | Every use case, Docker followed by Kubernetes |
| `make e2e-docker` | Every use case's Docker scenario matrix |
| `make e2e-k8s` | Every use case's Kubernetes scenario matrix |

```bash
make build CARGO_PROFILE=release BUILD_JOBS=2
make build-rust CARGO_ARGS='--target x86_64-unknown-linux-gnu'
cd web && npx playwright install chromium
cd ../.. && make test
make e2e IN_CN_GFW=false E2E_TIMEOUT=300
```

Make checks `target/` before every Rust build/test/lint task and runs `cargo clean` if it exceeds 10 GiB. Jobs default to two. When the network requires it, set `HTTPS_PROXY=http://127.0.0.1:8800`. Docker builds inherit the proxy and use host networking so the loopback proxy remains reachable. Cross-compilation also requires the target's native libraries and toolchain.

Image/E2E/release tasks probe Docker's registry directly: HTTP 200/401 selects the global catalog; other results select Aliyun. Override with `IN_CN_GFW=true|false` or `IMAGE_SOURCE=aliyun|global`. [images.py](../../deploy/images.py) is the single catalog for base, application, middleware, mock and carrier images. The Aliyun profile rejects foreign image overrides. Mirror selection does not prove that a particular mirror tag has been published; bootstrap should fail on an unavailable image rather than fall back silently.

The E2E matrix discovers each `use-cases/*/e2e/runner.py`, creates a case-local virtualenv and installs its single requirements file. `E2E_PYTHON=/path/to/python` selects an existing interpreter. `E2E_ARGS='--rounds 2 --reuse-build'` passes options to every runner; use `--reuse-environment` only for an already owned and initialized fixture. Docker and Kubernetes run sequentially because cases share bounded host ports. Kubernetes commands always use `/etc/rancher/k3s/k3s.yaml`. Check node readiness and disk pressure before deploying; do not remove scheduler protection to force acceptance. This host has 5% hard disk thresholds plus 10% minimum reclaim, so an active reclamation needs roughly 15% free space before its recovery interval. The local validation preserved executable hashes and freed reconstructable Cargo/package/browser caches; `make build` reinstalls Web dependencies for subsequent work. The current Kubernetes matrix verifies real Datafeed/Strategy Pod lifecycle; its middleware preparation runs through the owned Docker fixture. It does not claim that the entire financial pipeline runs in Kubernetes.

Real external-service/PG tests are explicitly ignored by plain `cargo test`; the case runner executes them with the actual middleware and records that they ran. The old duplicate root integration crate has been removed. See [case instructions](../../use-cases/marketpulse-autonomy-quantification/e2e/README_CN.md).

PR CI checks out the exact head SHA, builds/tests all modules, and builds `dirty-<sha8>` images; forks build without publishing. A PR merged into the default branch triggers a serialized rebuild of its merge SHA for native amd64/arm64, binaries and Helm. Official version/latest tags and the OCI chart are published only after every build succeeds, then checked again; binaries, chart and checksums go to GitHub Release. One PR comment retains CI/release results and every completed Actions step. These workflow definitions require an actual GitHub run to verify registry credentials and comment permissions.
