# Build and Test for Development

[Documentation index](../index.md)

## Discover Commands

```bash
git clone git@github.com:sigbot-projects/sigbot-core.git
cd sigbot-core/
make help
```

The root `Makefile` is the canonical development entry point. Cargo remains
available for low-level troubleshooting, but routine commands should use Make
so the project has one explicit build and test interface.

## Build the Native Binary

The Kafka exporter builds librdkafka from source. Native builds need CMake,
a C/C++ compiler, pkg-config, OpenSSL and libcurl development headers, in
addition to the existing Python development headers. On Debian/Ubuntu these
include `cmake libssl-dev libcurl4-openssl-dev python3-dev`. Build the isolated
Python host alongside the service with
`cargo build -p sigbot-cmd -p sigbot-strategy-runner --bins`; deploy
`sigbot-strategy-worker` next to `sigbot`.

```bash
# Development profile
make build

# Release profile
make build-release

# Optional cross-target or feature build
make build CARGO_PROFILE=release \
  TARGET=x86_64-unknown-linux-gnu \
  FEATURES=profiling-mem-prof,profiling-pprof \
  BUILD_JOBS=4
```

The binary is written below `target/<profile>/sigbot`, or
`target/<target>/<profile>/sigbot` when `TARGET` is set. The Makefile uses the
bundled Swagger UI archive, so this build step does not download that asset.

## Run Tests and Checks

```bash
# Workspace unit tests; `test` is an alias of `test-ut`.
make test
make test-ut

# Docker-backed integration tests; `it` is a short alias.
make test-it
make it IT_FILTER=f40_wallet_ledger_balance

make fmt-check
make clippy
```

Integration tests start their own PostgreSQL, TimescaleDB, Redis, Kafka, MinIO,
and EMQX containers. The Makefile runs them serially because the suites share
fixed host ports. Docker must be available before running `test-it`. See the
[E2E test running guide](../../tests/RUNNING.md) for suite inventory and
container troubleshooting.

Use `make fmt` to apply formatting and `make clean` to remove Cargo build
artifacts.

## Supported Cross-Compilation Targets

```bash

# Show targets supported.
rustup target list | grep -iE 'apple-darwin|x86_64-unknown-linux-gnu'
#aarch64-apple-darwin (installed)
#x86_64-apple-darwin
#x86_64-unknown-linux-gnu
#x86_64-unknown-linux-gnux32

# Build for x86 macOS
make build TARGET=x86_64-apple-darwin

# Build for ARM macOS
make build TARGET=aarch64-apple-darwin

# Build for Generic Linux (Unknown means vendor-less bound)
make build TARGET=x86_64-unknown-linux-gnu
```

## FAQ

### How solve to build with `cargo build --target x86_64-unknown-linux-gnu` in macOS M3 aarch64 failure

- Operation

```bash
# Prerequisites.
rustup target add x86_64-unknown-linux-gnu
# Build to Linux x86_64
make build TARGET=x86_64-unknown-linux-gnu
```

- Error

```log
...
pkg-config has not been configured to support cross-compilation.
 Install a sysroot for the target platform and configure it via
 PKG_CONFIG_SYSROOT_DIR and PKG_CONFIG_PATH, or install a
 cross-compiling wrapper for pkg-config and set it via
 PKG_CONFIG environment variable.
 cargo:warning=Could not find directory of OpenSSL installation, and this -sys crate cannot proceed without this knowledge. If OpenSSL is installed and this crate had trouble findin
g it, you can set the OPENSSL_DIR environment variable for the compilation process. See stderr section below for further information.
 --- stderr
 Could not find directory of OpenSSL installation, and this -sys crate cannot
 proceed without this knowledge. If OpenSSL is installed and this crate had
 trouble finding it, you can set the OPENSSL_DIR environment variable for the
 compilation process.
 Make sure you also have the development packages of openssl installed.
 For example, libssl-dev on Ubuntu or openssl-devel on Fedora.
 If you're in a situation where you think the directory should be found
 automatically, please open a bug at https://github.com/sfackler/rust-openssl
 and include information about your system as well as this message.
 $HOST = aarch64-apple-darwin
 $TARGET = x86_64-unknown-linux-gnu
 openssl-sys = 0.9.106
warning: build failed, waiting for other jobs to finish...
```

- Resolve

```bash
brew install FiloSottile/musl-cross/musl-cross
brew install pkg-config openssl
```
