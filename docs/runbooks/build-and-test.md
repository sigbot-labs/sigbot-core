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
| `make test` | Rust workspace tests, CI helper tests and Playwright UI checks |
| `make e2e` | The Marketpulse case, Docker followed by Kubernetes |
| `make e2e-docker` | The Marketpulse Docker scenario matrix |
| `make e2e-k8s` | The Marketpulse Kubernetes scenario matrix |

```bash
make build CARGO_PROFILE=release BUILD_JOBS=2
make build-rust CARGO_ARGS='--target x86_64-unknown-linux-gnu'
cd web && npx playwright install chromium
cd ../.. && make test
make e2e IN_CN_GFW=false E2E_TIMEOUT=300
```

Make checks `target/` before every Rust build/test/lint task and runs `cargo clean` if it exceeds 10 GiB. Jobs default to two. When the network requires it, set `HTTPS_PROXY=http://127.0.0.1:8800`. Docker builds inherit the proxy and use host networking so the loopback proxy remains reachable. Cross-compilation also requires the target's native libraries and toolchain.

On macOS, Make defaults `SDKROOT` to the SDK bundled with the selected Xcode when available, keeping it compatible with Xcode's linker even if Command Line Tools supplies a newer SDK. An explicitly supplied `SDKROOT` takes precedence. Direct Cargo invocations can set `SDKROOT="$(xcode-select -p)/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"` when using Xcode.

Image/E2E/release tasks probe Docker's registry directly: HTTP 200/401 selects the global catalog; other results select Aliyun. Override with `IN_CN_GFW=true|false` or `IMAGE_SOURCE=aliyun|global`. [images.py](../../deploy/images.py) is the single catalog for base, application, middleware, mock and carrier images. The Aliyun profile rejects foreign image overrides. Mirror selection does not prove that a particular mirror tag has been published; bootstrap should fail on an unavailable image rather than fall back silently.

The only E2E entry point is `use-cases/marketpulse-autonomy-quantification/e2e/runner.py`. It owns its case-local virtualenv, deployment, scenarios, timestamped reports and cleanup. `E2E_PYTHON=/path/to/python` selects an existing interpreter. `E2E_ARGS='--rounds 2 --reuse-build'` passes options to this runner; use `--reuse-environment` only for an already owned and initialized fixture. Docker and Kubernetes run sequentially because they share bounded host resources. Kubernetes commands always use `/etc/rancher/k3s/k3s.yaml`. Check node readiness and disk pressure before deploying; do not remove scheduler protection to force acceptance. The current full Docker report predates the latest tenant-middleware reconciler. The latest Kubernetes evidence covers Datafeed/Strategy Pod lifecycle, but not a fresh real-cluster lifecycle of all four scoped middleware types; see [the handoff](../development/HARDCOVER.md) before claiming that boundary passed.

Real external-service/PG tests are explicitly ignored by plain `cargo test`; the case runner executes selected targets with actual middleware and records that they ran. Migration invariants are part of the Marketpulse `database`/`schemas` scenarios; there is no separate `migration/tests` project. See [case instructions](../../use-cases/marketpulse-autonomy-quantification/e2e/README_CN.md).

PR CI checks out the exact head SHA, builds/tests all modules, and builds `dirty-<sha8>` images; forks build without publishing. A PR merged into the default branch triggers a serialized rebuild of its merge SHA for native amd64/arm64, binaries and Helm. Official version/latest tags and the OCI chart are published only after every build succeeds, then checked again; binaries, chart and checksums go to GitHub Release. One PR comment retains CI/release results and every completed Actions step. These workflow definitions require an actual GitHub run to verify registry credentials and comment permissions.

## Starting a module from YAML

Each process accepts `sigbot <module> --config <path>` (or `-c <path>`). The 12 modules are `api`, `a2a`, `controller`, `datafeed`, `evaluator`, `exporter`, `logger`, `strategy`, `backtest`, `notifier`, `order`, and `wallet`. Their entry types are `SigbotXxxStarter`. The nine tenant starters live under `startup/component/`; the built-in API starters are `startup/api/{http.rs,a2a.rs}`. The optional aggregate command uses root-level `startup/standalone.rs`. HTTP/A2A expose the [ten-domain CRUD contract](../architecture/domain-model_ZH.md). MCP is generated independently by [mcpfather](../architecture/engine/mcp/overview.md) from HTTP OpenAPI; the old server and command are archived. Helm/Compose retain the optional A2A process; Compose uses the `protocols` profile.

```bash
sigbot api -c etc/sigbot.yaml
sigbot a2a -c etc/sigbot.yaml
sigbot controller -c etc/sigbot.yaml
sigbot datafeed -c /etc/sigbot/runtime/sigbot.yaml
```

The loader reads the exact YAML path, rejects missing/invalid files and unknown typed fields, expands `${VARIABLE:default}`, then applies `SIGBOT__...` environment overrides. It does not implicitly load `.env`. Explicit startup paths take precedence over the legacy library fallback `SIGBOT_CFG_PATH`. Secrets remain named references with values supplied through the existing Secret mechanism.

`services.platform` owns operational HTTP/A2A API and Controller settings. `services.component.messager` selects the shared `MQTT` or `LOCAL` transport, while `services.component.exchange` contains only non-secret exchange endpoints. Each of the nine tenant components reads its typed configuration from `services.component.<module>`; `datafeed.providers` can select multiple ingestion providers and otherwise falls back to `datafeed.provider`, while `backtest.provider` selects `KLINE` (default) or `TRADES`. Tenant exchange credentials stay outside this base configuration and continue to be supplied through encrypted tenant records or Secret references. The old base64 configuration, component argument structures, and provider CLI flags have been removed.

The Controller converts each frozen node's persisted input contract into `services.component`, includes the generated content in its specification hash, and launches only `sigbot <module> --config /etc/sigbot/runtime/sigbot.yaml`. Kubernetes mounts an owned ConfigMap; Docker uploads an immutable-per-specification file into an owned configuration volume before starting the process. Both resources are removed with their worker. These files contain Secret references, while Secret values continue to come from the operator-provisioned runtime environment.

The logging component, crate, module, startup command, and configuration references are named `logger` (formerly `logservice`). Existing immutable `log_chunk` business storage is unchanged. Update persisted Workflow node module names before launching existing workflows with the new version.
