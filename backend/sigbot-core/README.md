# SigBot

> Sigbot - An Open Source Multi-Strategy, AI-driven Fast Trading Bot written in Rust.

## Introduction

Sigbot is a lightweight, Rust-based AI-driven trading bot that dynamically generates strategy code via LLM. It consists of multiple components - **api-server**, **deployer**, **datafeed-ingestor**, **strategy-runner**, **backtest-runner**, **notification-forwarder** - working together to provide a robust, multi-tenant trading platform.

## Features

- **Multi-tenant Architecture**: Support for managing tenants, users, and component deployments via Kubernetes or hosted environments;
- **Dynamic Strategy Generation**: AI-powered strategy code generation using LangChain-based AI;
- **Real-time Market Data Processing**: Ingest market data from multiple sources (Binance, Twitter, TruthSocial, Coinmarketcap, etc.) and publish via EMQx/Kafka;
- **Historical Data Archiving**: Async archiving of market data to PostgreSQL/TimescaleDB for backtesting;
- **Live & Backtest Trading**: Execute strategies in both live trading and backtest modes with position management;
- **Multi-channel Notifications**: Forward trading signals and notifications to Email, Telegram, WeChat, and other platforms;
- **Lightweight and Efficient**: Built with Rust async Axum framework for high performance;

## Development

- [Prerequisites for locally Development](./docs/devel/1.prerequisites-for-dev.md)
- [Build and Test for locally Development](./docs/devel/2.build-and-test-for-dev.md)

## Deployment

- [Deploy on Docker](./docs/deploy/build-and-deploy-on-docker.md)
- [Deploy on Kubernetes](./docs/deploy/deploy-on-kubernetes.md)
