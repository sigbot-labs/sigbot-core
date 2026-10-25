# Sigbot Core

> Sigbot - An Open Source Multi-Strategy, AI-driven Fast Trading Bot written in Rust.

## Introduction

Sigbot is a lightweight, Rust-based AI-driven trading bot that dynamically generates strategy code via LLM. It consists of multiple components - **APIServer**, **deployer**, **datafeed-ingestor**, **strategy-runner**, **backtest-runner**, **notification-forwarder**, **order-manager**, **wallet-manager** - working together to provide a robust, multi-tenant trading platform.

## Features

- **Multi-tenant Architecture**: Support for managing tenants, users, and component deployments via Kubernetes or hosted environments;
- **Dynamic Strategy Generation**: AI-powered strategy code generation using LangChain-based AI;
- **Real-time Market Data Processing**: Ingest market data from multiple sources (Binance, Twitter, TruthSocial, Coinmarketcap, etc.) and publish via EMQx/Kafka;
- **Historical Data Archiving**: Async archiving of market data to PostgreSQL/TimescaleDB for backtesting;
- **Live & Backtest Trading**: Execute strategies in both live trading and backtest modes with position management;
- **Multi-channel Notifications**: Forward trading signals and notifications to Email, Telegram, WeChat, and other platforms;
- **Lightweight and Efficient**: Built with Rust async Axum framework for high performance;

## Development

- [Documentation index](./docs/index.md)
- [Development prerequisites](./docs/runbooks/development-prerequisites.md)
- [Build and test](./docs/runbooks/build-and-test.md)

## Deployment

- [Deploy on Docker](./docs/runbooks/build-and-deploy-docker.md)
- [Deploy on Kubernetes](./docs/runbooks/deploy-kubernetes.md)
