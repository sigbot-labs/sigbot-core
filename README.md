# Sigbot Core

> Sigbot - An Open Source Multi-Strategy, AI-driven Fast Trading Bot written in Rust.

## Introduction

Sigbot is a Rust-based, AI-assisted trading platform. Its shared platform services are the **HTTP/A2A/MCP APIs**, **controller**, **order service**, and **wallet service**. The controller places isolated **datafeed**, **evaluator**, **strategy**, **backtest**, **logservice**, **notifier**, and **exporter** components for each tenant and Workflow on Docker or Kubernetes workers.

## Features

- **Tenant and Workflow isolation**: Authguard supplies platform identity and authorization; MQTT topics, storage ownership, and worker resources are scoped by `env`, `tenant_id`, and `workflow_id`.
- **Deterministic strategy effects**: Strategies emit intents, bounded queries, and timers; they never submit orders directly.
- **Real-time market and news ingestion**: Binance, Twitter/X, Truth Social, Federal Reserve, and Treasury facts retain source, event time, available time, sequence, and revision.
- **Durable asynchronous processing**: EMQX QoS 1 persistent sessions plus transactional Inbox/Outbox provide replay, deduplication, and restart recovery.
- **Three execution modes**: `live` uses real exchange APIs, `paper` uses a simulated exchange API, and `backtest` uses the isolated historical matching service.
- **Authoritative financial state**: Order prechecks, Wallet reservations, immutable double-entry journals, rebuildable balances and positions, and venue reconciliation are separated by write ownership.
- **Traceable evaluation and delivery**: LLM assessments separate facts from inference; logs archive to immutable S3 chunks; Email, Telegram, and in-app notifications retain idempotent delivery state.

## Development

- [Documentation index](./docs/index.md)
- [Development prerequisites](./docs/runbooks/development-prerequisites.md)
- [Build and test](./docs/runbooks/build-and-test.md)

## Deployment

- [Deploy on Docker](./docs/runbooks/build-and-deploy-docker.md)
- [Deploy on Kubernetes](./docs/runbooks/deploy-kubernetes.md)
