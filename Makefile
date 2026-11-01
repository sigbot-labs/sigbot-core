# SPDX-License-Identifier: GPL-3.0-only
SHELL := /bin/bash
.DEFAULT_GOAL := help
CARGO ?= cargo
NPM ?= npm
PYTHON ?= python3
CONTAINER_CLI ?= docker
VERSION ?= $(shell sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)
CARGO_PROFILE ?= dev
BUILD_JOBS ?= 2
CARGO_ARGS ?=
E2E_ARGS ?=
E2E_TIMEOUT ?= 600
RELEASE_DIR ?= dist
HELM_CHART := deploy/helm/sigbot
COMPOSE_FILE := deploy/docker/docker-compose.yaml
COMPOSE_ENV ?= deploy/docker/.env
IMAGE_GOALS := image-source build-image build-runtime-image build-web-image docker-up docker-down docker-logs e2e e2e-docker e2e-k8s release
ifneq ($(strip $(filter $(IMAGE_GOALS),$(MAKECMDGOALS))),)
ifeq ($(origin IN_CN_GFW),undefined)
IN_CN_GFW := $(shell status=$$(curl -sS --noproxy '*' -o /dev/null -w '%{http_code}' --connect-timeout 3 --max-time 5 https://registry-1.docker.io/v2/ 2>/dev/null || true); if [ "$$status" = 200 ] || [ "$$status" = 401 ]; then echo false; else echo true; fi)
endif
else
IN_CN_GFW ?= false
endif
ifeq ($(filter $(IN_CN_GFW),true false),)
$(error IN_CN_GFW must be true or false)
endif
IMAGE_SOURCE ?= $(if $(filter true,$(IN_CN_GFW)),aliyun,global)
ifeq ($(filter $(IMAGE_SOURCE),aliyun global),)
$(error IMAGE_SOURCE must be aliyun or global)
endif
export VERSION IMAGE_SOURCE IN_CN_GFW CONTAINER_CLI
export CARGO_BUILD_JOBS := $(BUILD_JOBS)
export CARGO_INCREMENTAL ?= 0
export CARGO_PROFILE_DEV_DEBUG ?= 0
export CARGO_PROFILE_TEST_DEBUG ?= 0
export SWAGGER_UI_DOWNLOAD_URL := file:$(CURDIR)/src/api/http/src/swagger/swagger-ui-5.17.14.zip
BUILD_PROXY := $(or $(HTTPS_PROXY),$(https_proxy),$(HTTP_PROXY),$(http_proxy))
PROXY_ARGS = $(if $(BUILD_PROXY),--network=host --build-arg HTTPS_PROXY='$(BUILD_PROXY)' --build-arg HTTP_PROXY='$(BUILD_PROXY)',)
IMAGE_SETUP = eval "$$($(PYTHON) deploy/images.py --source '$(IMAGE_SOURCE)' --version '$(VERSION)')"
PROFILE_ARG = $(if $(filter dev,$(CARGO_PROFILE)),,--profile $(CARGO_PROFILE))
.PHONY: help image-source rust-target-guard web-deps web-test-deps build build-rust build-web build-image build-runtime-image build-web-image docker-up docker-down docker-logs fmt lint lint-rust lint-web lint-helm test test-rust test-python test-web e2e e2e-docker e2e-k8s package-chart release clean
.NOTPARALLEL:
E2E_BUILD = $(if $(filter --reuse-build --skip-build,$(E2E_ARGS)),,build)

help:
	@printf '%s\n' 'Sigbot' '' 'Build:' '  make build | build-web | build-image' '  make docker-up | docker-down | docker-logs' '' 'Quality:' '  make fmt | lint | lint-helm' '' 'Test:' '  make test                    All module unit tests' '  make e2e                     Every use case, Docker then Kubernetes' '  make e2e-docker | e2e-k8s     Every use case on the named platform' '' 'Overrides: IN_CN_GFW=true|false IMAGE_SOURCE=aliyun|global CONTAINER_CLI=docker|podman E2E_ARGS="..."'

image-source:
	@echo 'IN_CN_GFW=$(IN_CN_GFW) IMAGE_SOURCE=$(IMAGE_SOURCE)'
	@$(PYTHON) deploy/images.py --source '$(IMAGE_SOURCE)' --version '$(VERSION)'

rust-target-guard:
	@size=$$(du -sk target 2>/dev/null | cut -f1); if [ "$${size:-0}" -gt 10485760 ]; then echo 'target/ exceeds 10 GiB; cargo clean before building'; $(CARGO) clean; fi

build: build-rust build-web
build-rust:
	@$(MAKE) --no-print-directory rust-target-guard
	$(CARGO) build --locked --workspace $(PROFILE_ARG) $(CARGO_ARGS)
web-deps:
	cd web && $(NPM) ci
web-test-deps: web-deps
	@if [ -n "$(PLAYWRIGHT_CHROMIUM_EXECUTABLE)" ] && [ -x "$(PLAYWRIGHT_CHROMIUM_EXECUTABLE)" ]; then \
		echo 'Using PLAYWRIGHT_CHROMIUM_EXECUTABLE=$(PLAYWRIGHT_CHROMIUM_EXECUTABLE)'; \
	else \
		cd web && $(NPM) exec playwright install chromium; \
	fi
build-web: web-deps
	cd web && $(NPM) run build
build-image: build-runtime-image build-web-image
build-runtime-image:
	@$(MAKE) --no-print-directory rust-target-guard
	@set -eu; $(IMAGE_SETUP); $(CONTAINER_CLI) build $(PROXY_ARGS) -f deploy/docker/Dockerfile --build-arg RUST_BUILDER_IMAGE="$$RUST_BUILDER_IMAGE" --build-arg RUNTIME_IMAGE="$$RUNTIME_IMAGE" --build-arg BUILD_MIRROR_URL='$(if $(filter aliyun,$(IMAGE_SOURCE)),http://mirrors.aliyun.com,http://deb.debian.org)' --build-arg BUILD_COMMIT_ID="$$(git rev-parse HEAD)" --build-arg BUILD_TAG='$(VERSION)' -t "$$SIGBOT_IMAGE" .
build-web-image:
	@set -eu; $(IMAGE_SETUP); $(CONTAINER_CLI) build $(PROXY_ARGS) -f web/deploy/Dockerfile --build-arg NODE_IMAGE="$$WEB_NODE_IMAGE" --build-arg WEB_IMAGE="$$WEB_RUNTIME_IMAGE" -t "$$SIGBOT_WEB_IMAGE" .

docker-up:
	@test -f '$(COMPOSE_ENV)' || { echo 'Create $(COMPOSE_ENV) from its example with Secret values first' >&2; exit 1; }
	@set -eu; $(IMAGE_SETUP); SIGBOT_ENV_FILE='$(abspath $(COMPOSE_ENV))' $(CONTAINER_CLI) compose --env-file '$(COMPOSE_ENV)' -f '$(COMPOSE_FILE)' up -d --wait
docker-down:
	@set -eu; $(IMAGE_SETUP); SIGBOT_ENV_FILE='$(abspath $(COMPOSE_ENV))' $(CONTAINER_CLI) compose --env-file '$(COMPOSE_ENV)' -f '$(COMPOSE_FILE)' down
docker-logs:
	@set -eu; $(IMAGE_SETUP); SIGBOT_ENV_FILE='$(abspath $(COMPOSE_ENV))' $(CONTAINER_CLI) compose --env-file '$(COMPOSE_ENV)' -f '$(COMPOSE_FILE)' logs -f

fmt: web-deps
	find src build benchmarks -type f -name '*.rs' ! -path 'src/component/strategy/sdk/*' ! -path 'src/component/strategy/runner/src/python/*' -print0 | xargs -0 rustfmt --edition 2021 --config skip_children=true --check
	cd web && $(NPM) exec prettier -- --check src tests
lint: lint-rust lint-web lint-helm
lint-rust:
	@$(MAKE) --no-print-directory rust-target-guard
	$(CARGO) clippy --locked --workspace --all-targets -- -D clippy::correctness
lint-web: web-deps
	cd web && $(NPM) exec tsc -- --noEmit
lint-helm:
	helm dependency list '$(HELM_CHART)'
	helm lint '$(HELM_CHART)'
	@set -eu; values=$$(mktemp); trap 'rm -f "$$values"' EXIT; for source in global aliyun; do $(PYTHON) deploy/images.py --source "$$source" --version '$(VERSION)' --format helm > "$$values"; helm template sigbot '$(HELM_CHART)' -f "$$values" >/dev/null; done

test: test-rust test-python test-web
test-rust:
	@$(MAKE) --no-print-directory rust-target-guard
	$(CARGO) test --locked --workspace --no-fail-fast $(CARGO_ARGS)
test-python:
	$(PYTHON) -m unittest discover -s .github/scripts -p '*_test.py' -v
test-web: web-test-deps
	cd web && $(NPM) run build && $(NPM) test

e2e: e2e-docker e2e-k8s
e2e-docker: $(E2E_BUILD)
	@set -eu; $(IMAGE_SETUP); $(PYTHON) use-cases/marketpulse-autonomy-quantification/e2e/runner.py --deployer docker --timeout '$(E2E_TIMEOUT)' $(E2E_ARGS)
e2e-k8s: $(E2E_BUILD)
	@set -eu; $(IMAGE_SETUP); export KUBECONFIG=/etc/rancher/k3s/k3s.yaml; $(PYTHON) use-cases/marketpulse-autonomy-quantification/e2e/runner.py --deployer kubernetes --timeout '$(E2E_TIMEOUT)' $(E2E_ARGS)

package-chart: lint-helm
	mkdir -p '$(RELEASE_DIR)'
	helm package '$(HELM_CHART)' --destination '$(RELEASE_DIR)' --version '$(VERSION)' --app-version '$(VERSION)'
release: build-image package-chart
	@set -eu; $(IMAGE_SETUP); $(CONTAINER_CLI) push "$$SIGBOT_IMAGE"; $(CONTAINER_CLI) push "$$SIGBOT_WEB_IMAGE"
	helm push '$(RELEASE_DIR)/sigbot-$(VERSION).tgz' oci://ghcr.io/wl4g/charts
clean:
	$(CARGO) clean
	$(PYTHON) -c 'import shutil; shutil.rmtree("web/dist", ignore_errors=True)'
