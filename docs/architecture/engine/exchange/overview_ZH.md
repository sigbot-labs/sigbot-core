# Exchange

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性内嵌 SDK 架构。各 Provider Capability 不同，必须显式发现。

Exchange 不是独立 daemon，而是 Capability-based Async SDK：Datafeed 组合公共市场
Stream，Order Manager 组合私有交易，Wallet Manager 用于账户对账，Backtest 组合
模拟执行。Provider 细节不能泄漏到固定 Pipeline Contract。

## Capability 模型

`ISigbotExchangeClient` 标识 Venue/Account Adapter，并只暴露其实际实现的 Capability
Port：

| Port | 职责 |
|---|---|
| `ISigbotMarketDataClient` | REST Snapshot、Instrument、Kline、Trade、Funding 与公共 Reference Data |
| `ISigbotTradingClient` | 使用稳定 Client ID 下单、取消、改单与查询 Order |
| `ISigbotAccountClient` | Balance、Position、Fill、Transfer、Fee 与 Account Configuration |
| `ISigbotExchangeStream` | 带 Cursor/Reconnect 语义的公共与私有 WebSocket/Session Stream |

Spot、Margin、Perpetual/Futures、Option、Equity 与 Prediction Market 分别声明 Capability
与 Product Metadata。不支持的操作返回类型化 Capability Error，禁止 Panic 或静默
映射到另一 Market Type。

## Adapter 结构

```text
ISigbotExchangeClient
  -> SigbotBinanceExchangeClient
       -> Spot capabilities
       -> USD-M futures capabilities
  -> SigbotBitgetExchangeClient
  -> SigbotCoinbaseExchangeClient
  -> SigbotIbkrExchangeClient
  -> SigbotSchwabExchangeClient
  -> SigbotSimulatedExchangeClient
```

图中 Provider 名称表达目标 Adapter 边界，不代表每个 Adapter 当前都已实现。Provider
Factory 在 Caller 启动前校验配置，并返回 Capability Descriptor。

## 规范化

Adapter 把 Provider-specific Instrument、Order Type、Status、Precision、Error Code、
Timestamp、Sequence 与 Fill 映射成版本化 SigBot 类型。原始 Payload 可以作为脱敏
Diagnostic Extension 保留，但不能成为业务契约。

Instrument Metadata 拥有 Tick Size、Lot Size、Minimum Notional、Currency、Contract
Multiplier、Margin Mode 与支持的 Order Feature。Price、Quantity、Fee、Balance 全部
使用显式 Decimal 语义。

## Order 与 Stream 语义

- Order Manager 提供稳定 `client_order_id`；Retry 前先按该 ID 对账。
- Placement Acknowledgement、Order State、Trade Fill、Settlement、Fee、Funding 与
  Liquidation 是不同事实。
- 低延迟 Fact 优先来自 Private WebSocket；REST Query 用于 Reconcile Gap、Reconnect
  与不确定结果。
- Stream Event 暴露 Provider Sequence/Cursor 与 Connection Generation，使 Consumer
  能识别 Duplicate、Gap 与 Out-of-order Delivery。
- Rate Limit 按 Provider/Account Credential Scope 共享，不能由每条 Request Path 各自
  重建。

## 安全与可靠性不变量

- Credential 在 Adapter 内部从 Secret Reference 解析，禁止通过公共配置或 Event 返回。
- Signing 严格使用 Provider Timestamp/Nonce Policy 与有界 Clock Offset；Sensitive
  Request/Response Field 必须脱敏。
- Timeout 在 Reconciliation 证明成功或失败前分类为 Unknown Outcome。
- Retry Policy 区分 Safe Read、Idempotent Write 与 Non-idempotent External Side Effect。
- Sandbox/Testnet 与 Production Endpoint 不能误共用 Account ID 或 Credential Object。
- WebSocket Reconnect 必须先恢复 Subscription 与 Gap，再宣告 Stream Healthy。

## 实现对齐

当前源码暴露 `ISigbotExchangeClient` 以及 CEX、DEX、Stock、Prediction-market、
Order-book 子 Trait。已有 Binance、Hyperliquid、IBKR、Polymarket、Kalshi Source
Adapter，但 Factory 当前只构造 Binance 与 IBKR 路径。Binance 主要是 USD-M Futures，
不能宣称 Spot 已完整支持。Bitget、Coinbase 与 Schwab 是目标 Adapter，不是当前实现。
若干已配置 Provider 当前仍可能进入 Unimplemented 或 Panic 路径。

必须对齐到 Capability Discovery、类型化 Unsupported Behavior、完整 REST/WebSocket
Normalization、共享 Rate Limit、稳定外部幂等，以及每个 Provider/Product 组合的
Contract Test。

## 源码地图

- Exchange SDK 与 Provider Adapter：`src/exchange/`
- 共享 Exchange 类型：`src/types/src/modules/exchange/`
