# SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
#
# Copyleft (c) 2024 James Wong. This file is part of James Wong.
# is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# James Wong is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
#
# IMPORTANT: Any software that fully or partially contains or uses materials
# covered by this license must also be released under the GNU GPL license.
# This includes modifications and derived works.
#

#####################################################
### STAGE 1: Build Base with System Dependencies  ###
#####################################################
ARG RUST_BUILD_IMAGE=docker.io/library/rust:1.94.0-bookworm
FROM ${RUST_BUILD_IMAGE} AS base

ARG BUILD_MIRROR_URL=http://mirrors.aliyun.com
ARG DEBIAN_FRONTEND=noninteractive

# Set up fast APT sources and such as:
#   Debian Official Default: http://deb.debian.org
#   Alibaba Cloud Internal: http://mirrors.cloud.aliyuncs.com
#   Alibaba Cloud External: http://mirrors.aliyun.com
#
RUN rm -f /etc/apt/sources.list.d/debian.sources && \
    echo "deb ${BUILD_MIRROR_URL}/debian bookworm main contrib non-free non-free-firmware" > /etc/apt/sources.list && \
    echo "deb ${BUILD_MIRROR_URL}/debian-security bookworm-security main contrib non-free non-free-firmware" >> /etc/apt/sources.list && \
    echo "deb ${BUILD_MIRROR_URL}/debian bookworm-updates main contrib non-free non-free-firmware" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        pkg-config \
        cmake \
        libssl-dev \
        libcurl4-openssl-dev \
        ca-certificates \
        python3-dev \
        python3 \
        libpython3-dev \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /usr/src/app

#################################################
### STAGE 2: Build Dependencies Cached Layer  ###
#################################################
FROM base AS deps

ARG PROJECT_NAME=sigbot

# Copy only files needed for dependency resolution. (Excluded: 'rust-toolchain.toml' due to containing dev components.)
COPY benchmarks ./benchmarks
COPY src ./src
COPY tests ./tests
COPY tooling/build/deps/swagger-ui-5.17.14.zip ./tooling/build/deps/swagger-ui-5.17.14.zip
COPY Cargo.toml Cargo.lock ./

ENV SWAGGER_UI_DOWNLOAD_URL=file:/usr/src/app/tooling/build/deps/swagger-ui-5.17.14.zip

# Setup dependency caching structure
RUN find src benchmarks tests -type f -not -name "Cargo.toml" -delete && \
    find src -type d -name src -o -path "*/src" | xargs -I{} mkdir -p {} && \
    find benchmarks -type d -name src -o -path "*/src" | xargs -I{} mkdir -p {} && \
    find src -type d -name src | xargs -I{} touch {}/lib.rs && \
    find benchmarks -type d -name src | xargs -I{} touch {}/lib.rs && \
    mkdir -p tests/integration && \
    touch tests/integration/mod.rs tests/integration/integration_all.rs && \
    mkdir -p src/cmd/src/bin && \
    echo "fn main() {}" > src/cmd/src/bin/${PROJECT_NAME}.rs && \
    mkdir -p src/strategy/runner/src/bin && \
    echo "fn main() {}" > src/strategy/runner/src/bin/sigbot-strategy-worker.rs

# Cargo validates explicitly named targets even when the cache build selects bins.
# Preserve every manifest's custom lib/test/bench/example path in this throwaway layer.
RUN python3 -c 'from pathlib import Path; import tomllib; roots = [Path("src"), Path("benchmarks"), Path("tests")]; manifests = [path for root in roots for path in root.rglob("Cargo.toml")]; exec("for manifest in manifests:\n spec = tomllib.loads(manifest.read_text())\n for kind in (\"lib\", \"bin\", \"test\", \"bench\", \"example\"):\n  targets = [spec[kind]] if kind == \"lib\" and kind in spec else spec.get(kind, [])\n  for target in targets:\n   if \"path\" in target:\n    path = manifest.parent / target[\"path\"]\n    path.parent.mkdir(parents=True, exist_ok=True)\n    path.write_text(\"\" if kind == \"lib\" else \"fn main() {}\\n\")")'

# Build dependencies only
RUN cargo build --locked --release -p sigbot-cmd -p sigbot-strategy-runner --bins

################################################
### STAGE 3: Build Application Binary        ###
################################################
FROM base AS builder

ARG PROJECT_NAME=sigbot
ARG BUILD_ARGS=""

# Copy cached dependencies
COPY --from=deps /usr/src/app/target target
COPY --from=deps /usr/local/cargo /usr/local/cargo

# Copy all source code and project files
COPY benchmarks ./benchmarks
COPY build ./build
COPY etc ./etc
COPY src ./src
COPY static ./static
COPY tooling ./tooling
COPY tests ./tests
COPY Cargo.toml Cargo.lock ./

ENV SWAGGER_UI_DOWNLOAD_URL=file:/usr/src/app/tooling/build/deps/swagger-ui-5.17.14.zip

# Check toolchain and build the application
RUN cargo build --locked --release ${BUILD_ARGS} -p sigbot-cmd -p sigbot-strategy-runner --bins && \
    install -m 0755 target/release/${PROJECT_NAME} /usr/local/cargo/bin/${PROJECT_NAME} && \
    install -m 0755 target/release/sigbot-strategy-worker /usr/local/cargo/bin/sigbot-strategy-worker

#################################################
### STAGE 4: Runtime Image                    ###
#################################################
FROM registry.cn-shenzhen.aliyuncs.com/wl4g/debian:bookworm

# Build project metadata labels.
ARG PROJECT_NAME=sigbot
ARG PROJECT_REPO="https://github.com/sigbot-projects/sigbot-core"
ARG PROJECT_DESC="Sigbot - An Open Source Multi-Strategy, AI-driven Fast Trading Bot written in Rust."
ARG PROJECT_MAINTAINER="James Wong<jameswong1376@gmail.com>"
ARG BUILD_COMMIT_ID=""
ARG BUILD_BRANCH=""
ARG BUILD_TAG=""
ARG BUILD_TIME=""
# Build args configuration.
ARG DEBIAN_FRONTEND=noninteractive
ARG BUILD_MIRROR_URL=http://mirrors.aliyun.com
ARG BUILD_ARGS=""

LABEL maintainer="${PROJECT_MAINTAINER}" \
      description="${PROJECT_DESC}" \
      org.opencontainers.image.title=${PROJECT_NAME} \
      org.opencontainers.image.source=${PROJECT_REPO} \
      org.opencontainers.image.revision=${BUILD_COMMIT_ID} \
      org.opencontainers.image.version=${BUILD_TAG} \
      org.opencontainers.image.created=${BUILD_TIME} \
      build.repo.url=${PROJECT_REPO} \
      build.repo.commit=${BUILD_COMMIT_ID} \
      build.repo.branch=${BUILD_BRANCH} \
      build.repo.tag=${BUILD_TAG} \
      build.deps.mirror.url=${BUILD_MIRROR_URL}

# Install runtime dependencies
RUN rm -f /etc/apt/sources.list.d/debian.sources && \
    echo "deb ${BUILD_MIRROR_URL}/debian bookworm main contrib non-free non-free-firmware" > /etc/apt/sources.list && \
    echo "deb ${BUILD_MIRROR_URL}/debian-security bookworm-security main contrib non-free non-free-firmware" >> /etc/apt/sources.list && \
    echo "deb ${BUILD_MIRROR_URL}/debian bookworm-updates main contrib non-free non-free-firmware" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        tini \
        libssl3 \
        ca-certificates \
        python3 \
        python3-pip \
        libpython3.11 \
        && \
    pip3 install --no-cache-dir --break-system-packages \
        polars \
        pandas \
        pyarrow \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -g 1000 ${PROJECT_NAME} && \
    useradd -u 1000 -g ${PROJECT_NAME} ${PROJECT_NAME} && \
    mkdir -p /app/config /app/data /app/static && \
    chown -R ${PROJECT_NAME}:${PROJECT_NAME} /app

# Copy the application binary
COPY --from=builder /usr/local/cargo/bin/${PROJECT_NAME} /usr/local/bin/${PROJECT_NAME}
COPY --from=builder /usr/local/cargo/bin/sigbot-strategy-worker /usr/local/bin/sigbot-strategy-worker

# Copy configuration and static files
COPY --from=builder /usr/src/app/etc /app/config
COPY --from=builder /usr/src/app/static /app/static

# Setup working directory
WORKDIR /app
USER 1000:1000
ENV SIGBOT_CFG_PATH=/app/config/sigbot.yaml

# Set the run entrypoint using tini as init
ENTRYPOINT ["/usr/bin/tini", "-s", "--", "/usr/local/bin/sigbot"]

# Default command if none provided
CMD ["api"]
