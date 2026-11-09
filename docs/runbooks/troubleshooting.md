# Troubleshooting

[中文](troubleshooting_CN.md) · [Deployment](deployment.md) · [Build and test](../development/build-and-test.md)

## Startup, authentication and deployment

| Symptom                                            | Check and action                                                                                                                                                                                                                                             |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Invalid YAML or missing configuration              | Use `sigbot <module> --config <path>`. Unknown/duplicate keys and obsolete aliases fail; `.env` is not loaded implicitly. Use canonical `SIGBOT__...` overrides and central Secret references.                                                               |
| API refuses to bind, 401 or 403                    | Check the [verified identity contract](../architecture/platform/api/api.md#iam): verifier Secret, signature/expiry, action/path/tenant match and domain grants. Deny overrides allow; an unsigned internal header cannot authorize a request.                |
| Login redirect or authenticated Web flow fails     | Check the actual same-origin Gateway/Application hostname, TLS, return URI and Authguard route. `web-full` requires real browser storage state; signed test contexts and public screenshots do not replace Hosted Login.                                     |
| Platform is healthy but no tenant components start | Check the persisted immutable Run, matching `cluster_info`, worker readiness/capacity, ownership labels and [Controller prerequisites](../architecture/platform/controller/controller.md). Stopping one Workflow retains shared middleware data.             |
| Pending Pods or unhealthy worker                   | Check node readiness/disk pressure, namespace, ServiceAccount, runtime Secret, egress rules and PVCs. Do not remove scheduler protection; Controller does not create remote Secrets/namespaces or adopt foreign resources.                                   |
| Image pull fails                                   | Check `make image-source`, actual tag/digest and registry permissions. Aliyun mode rejects foreign images; packaging/rendering does not prove image availability. Use the approved jump-host pull workflow and verify changed SSH host keys with operations. |
| Native Rust build fails                            | Follow the pinned toolchain and native prerequisites in [Build and test](../development/build-and-test.md). On macOS repair Xcode/CLT selection or supply a local SDKROOT; clean stale native/CMake output after changing toolchains.                        |
| Migration checksum drift                           | Stop; an already initialized database with an older baseline needs an explicit upgrade/import plan. Do not edit `mig_version` to bypass drift. Use schema-owner credentials for migration and DML-only runtime accounts.                                     |

Inspect scoped logs and private readiness/metrics. Logs carry run/trace context, never credentials; archive ACK means local SQLite WAL durability, not completed S3 upload. See [Logger recovery](../architecture/component/logger/logger.md#rust-log-capture) and [Messager replay](../architecture/messager/messager.md). Historical PASS totals only prove their [recorded execution scope](../development/handover.md#acceptance).

<a id="cpu"></a>

## CPU profiling

Use the configured private management listener; the examples use `localhost:9001`. Capture the default 99 Hz, five-second protobuf sample and inspect it with pprof:

```bash
curl -X POST -s 'http://localhost:9001/debug/prof/cpu' > /tmp/pprof.out
go tool pprof -top /tmp/pprof.out
```

For a 60-second flamegraph or a 49 Hz, ten-second text sample:

```bash
curl -X POST -s 'http://localhost:9001/debug/prof/cpu?seconds=60&format=flamegraph' > /tmp/pprof.svg
curl -X POST -s 'http://localhost:9001/debug/prof/cpu?seconds=10&frequency=49&format=text' > /tmp/pprof.txt
```

The binary output follows the [pprof protobuf format](https://github.com/google/pprof/blob/master/proto/profile.proto).

<a id="memory"></a>

## Memory profiling

Install jemalloc and the [FlameGraph utility](https://github.com/brendangregg/FlameGraph), then build the profiling feature. Apply the normal target-size/resource guard before a direct Cargo build.

```bash
# macOS
brew install jemalloc
# Ubuntu
sudo apt install libjemalloc-dev
curl https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl > /tmp/flamegraph.pl
cargo build --features=profiling-mem-prof
```

Start the required module with profiling enabled. `lg_prof_interval:28` samples at 2^28 allocated bytes, about 256 MiB:

```bash
MALLOC_CONF=prof:true,lg_prof_interval:28 ./target/debug/sigbot api --config etc/sigbot.yaml
```

Capture the heap profile from another terminal, repeat for a later sample, then compare with a baseline:

```bash
curl -X POST http://localhost:9001/debug/prof/mem > /tmp/sigbot.hprof
jeprof --svg <path_to_sigbot_binary> --base=<baseline_prof> <profile_data> > /tmp/memory.svg
```

Keep profiles and generated graphs outside the source tree. Monitor host CPU, available memory and I/O during capture; stop owned low-priority heavy tasks before the host becomes unhealthy. Never delete named production images or unrelated volumes to make a test pass.
