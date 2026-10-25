# SPDX-License-Identifier: GPL-3.0-only

SHELL := /bin/bash
.DEFAULT_GOAL := help

CARGO ?= cargo
DOCKER ?= docker

CARGO_PROFILE ?= dev
FEATURES ?=
TARGET ?=
BUILD_JOBS ?=
CARGO_BUILD_ARGS ?=
TEST_FILTER ?=
IT_FILTER ?=
IT_ARGS ?=

VERSION := $(shell sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -n 1)
SWAGGER_UI_ARCHIVE := $(CURDIR)/tooling/build/deps/swagger-ui-5.17.14.zip
export SWAGGER_UI_DOWNLOAD_URL := file:$(SWAGGER_UI_ARCHIVE)

IMAGE ?= docker.io/wl4g/sigbot:$(VERSION)
PLATFORM ?= linux/amd64
BUILD_MIRROR_URL ?= http://mirrors.aliyun.com
DOCKERFILE := tooling/build/docker/Dockerfile
COMPOSE_FILE := tooling/deploy/docker/docker-compose.yaml
DOCKER_BUILD_ARGS ?=

GIT_REPOSITORY := $(shell git config --get remote.origin.url 2>/dev/null || echo unknown)
GIT_COMMIT := $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
GIT_BRANCH := $(shell git branch --show-current 2>/dev/null || echo unknown)

CARGO_PROFILE_ARG = $(if $(filter dev,$(CARGO_PROFILE)),,--profile $(CARGO_PROFILE))
CARGO_FEATURE_ARGS = $(if $(strip $(FEATURES)),--features $(FEATURES),)
CARGO_TARGET_ARG = $(if $(strip $(TARGET)),--target $(TARGET),)
CARGO_JOBS_ARG = $(if $(strip $(BUILD_JOBS)),--jobs $(BUILD_JOBS),)
DOCKER_PROXY_ARGS = $(if $(strip $(HTTPS_PROXY)),--build-arg HTTPS_PROXY=$(HTTPS_PROXY) --build-arg HTTP_PROXY=$(HTTPS_PROXY),)
DOCKER_CARGO_ARGS = --locked $(CARGO_FEATURE_ARGS)

.PHONY: help version build build-release test test-ut test-it it fmt fmt-check clippy clean \
	build-image push-image compose-up compose-down compose-ps

help:
	@printf '%s\n' \
		'SigBot Core — Make targets' \
		'' \
		'  Development:' \
		'    make build                         Build the sigbot binary' \
		'    make build-release                 Build with the release profile' \
		'    make test | make test-ut           Run workspace unit tests' \
		'    make test-it | make it             Run Docker-backed integration tests serially' \
		'    make fmt | make fmt-check | make clippy' \
		'                                       Format and lint the workspace' \
		'    make clean                         Remove Cargo build artifacts' \
		'' \
		'  Container:' \
		'    make build-image                   Build the main application image' \
		'    make push-image                    Push the image; authenticate beforehand' \
		'    make compose-up | compose-ps       Start or inspect the Compose stack' \
		'    make compose-down                  Stop the Compose stack' \
		'' \
		'  Common overrides:' \
		'    CARGO_PROFILE=release FEATURES=a,b TARGET=<triple> BUILD_JOBS=4' \
		'    IT_FILTER=<test-name> IMAGE=<registry/name:tag> PLATFORM=linux/amd64'

version:
	@printf '%s\n' '$(VERSION)'

build:
	$(CARGO) build --locked -p sigbot-cmd --bin sigbot \
		$(CARGO_PROFILE_ARG) $(CARGO_FEATURE_ARGS) $(CARGO_TARGET_ARG) \
		$(CARGO_JOBS_ARG) $(CARGO_BUILD_ARGS)

build-release:
	$(MAKE) build CARGO_PROFILE=release

test: test-ut

test-ut:
	$(CARGO) test --locked --workspace --exclude sigbot-integration-tests $(TEST_FILTER)

# Integration tests own their Docker middleware lifecycle. Keep execution
# serial because the suites share fixed host ports and each starts containers.
test-it:
	$(CARGO) test --locked -p sigbot-integration-tests --lib $(IT_FILTER) -- \
		--include-ignored --nocapture --test-threads=1 $(IT_ARGS)

it: test-it

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --locked --workspace --all-targets -- -D warnings

clean:
	$(CARGO) clean

build-image:
	$(DOCKER) buildx build --load \
		--platform '$(PLATFORM)' \
		--tag '$(IMAGE)' \
		--file '$(DOCKERFILE)' \
		--build-arg PROJECT_REPO='$(GIT_REPOSITORY)' \
		--build-arg BUILD_COMMIT_ID='$(GIT_COMMIT)' \
		--build-arg BUILD_BRANCH='$(GIT_BRANCH)' \
		--build-arg BUILD_TAG='$(VERSION)' \
		--build-arg BUILD_MIRROR_URL='$(BUILD_MIRROR_URL)' \
		--build-arg BUILD_ARGS='$(DOCKER_CARGO_ARGS)' \
		$(DOCKER_PROXY_ARGS) $(DOCKER_BUILD_ARGS) .

push-image:
	$(DOCKER) push '$(IMAGE)'

compose-up:
	SIGBOT_IMAGE='$(IMAGE)' $(DOCKER) compose -f '$(COMPOSE_FILE)' up -d

compose-down:
	SIGBOT_IMAGE='$(IMAGE)' $(DOCKER) compose -f '$(COMPOSE_FILE)' down

compose-ps:
	SIGBOT_IMAGE='$(IMAGE)' $(DOCKER) compose -f '$(COMPOSE_FILE)' ps
