# Exchange

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性内嵌 SDK 架构。各 Provider Capability 不同，必须显式发现。

Exchange 不是独立 daemon，而是 Capability-based Async SDK：Datafeed 组合公共市场
Stream，Order Manager 组合私有交易，Wallet Manager 用于账户对账，Backtest 组合
模拟执行。Provider 细节不能泄漏到固定 Pipeline Contract。

## Capability 模型

`IExchange` 标识 Venue/Account Adapter，并只暴露其实际实现的 Capability
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
IExchange
  -> SigbotBinanceExchangeClient
       -> Spot capabilities
       -> USD-M futures capabilities
  -> SigbotBitgetExchangeClient
  -> SigbotCoinbaseExchangeClient
  -> SigbotIbkrExchangeClient
  -> SigbotSchwabExchangeClient
  -> BacktestExchange
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

真实订单入口为 `ExecutionAdapterFactory` → `LiveExecutionFactory`。它按配置选择
Binance Spot 官方 SDK、IBKR `ibapi` TWS/Gateway SDK、Polymarket 官方 V2/V3 SDK。
Backtest 选择专用历史执行接口，禁止改写 Live 交易所 URL。旧市场数据 Source
接口仍按原目录保留；它们不作为这些真实订单的执行路径。

Binance 使用业务键的稳定编码作为 client order ID，关闭 SDK 写入重试。
IBKR 和 Polymarket 先 `prepare`，随后 Order 仓库在 Wallet 预占成功后持久化
`PreparedSubmission`，最后单次领取提交权。IBKR 保存 SDK order ID、Contract、Order；
Polymarket 保存官方 Solidity ABI 编码、签名与 EIP-712 order hash。重启只读取已保存
负载并对账，不能重新分配 ID、salt 或签名后盲目提交。

准备负载用 OpenSSL AES-256-GCM 加密，AAD 绑定租户、环境、账户、Workflow、
节点、run/seq、交易意图及交易所订单 ID。`prepared_keys.active_key` 选择密钥版本，
`prepared_keys.keys` 映射版本到宿主环境变量名（值为 32 字节十六进制密钥）。轮换时
保留尚有未终结订单的旧密钥。密钥和交易凭据均不进入策略进程。

IBKR 配置显式网关地址、独占 `client_id`、账户环境变量及 SDK Contract 映射，要求
Contract 已有确定的 `contract_id`；每次新连接检查账户归属，禁 SDK 自动重连重发。
其 SDK 的 f64 边界进行 Decimal 往返校验，不能精确保留的数量或价格直接拒绝。
Polymarket 限价使用 GTC；市价意图使用 FOK、固定股数以及风控市场证据中的价格上限/
下限，禁止突破预占现金预算。Polymarket `matched` 只表示撮合受理，不能当链上结算。

Order 轮询适配器的真实成交事实，在同一事务中保存交易所游标、去重记录与 MQTT
Outbox。Binance 使用官方 `myTrades` 的实际成交现金与佣金；IBKR 按执行 ID 连接
官方成交及佣金报告，仅支持股票/ETF。缺少佣金或收到成交更正时必须对账，不填零费率。

Polymarket 必须配置 `settlement: {rpc_url_env: POLYGON_RPC_URL, min_confirmations: 64}`；
环境变量保存 HTTPS Polygon RPC 地址，缺少配置时工厂拒绝启动。下单之前验证链 137、
官方交易所合约、抵押币精度及 finalized 区块读取能力，并将起始区块一同密封持久化。
恢复后按已保存订单哈希分页扫描有界区块范围，每页最多处理 32 条回执，使大订单可
在 RPC 超时内持续推进游标。逐条验证回执成功、区块仍在主链且已
finalized、确认深度达标，以及 maker/token/side/exchange 与订单完全一致。
品种配置使用 `base_asset: outcome:<token_id>`、`quote_asset: pUSD`。
旧 prepared order 没有扫描边界时必须人工对账，不能自动重下。

实际抵押币成交额及手续费直接读取官方
[OrderFilled ABI](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/interfaces/ITrading.sol)
及[抵押币费用定义](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/mixins/Fees.sol)。
`ExecutionFact.quote_quantity` 保留实际现金额，单位价出现无限小数时也不重新相乘
计算入账金额；旧事实缺少该字段时保持原计算兼容。链上成交自然 ID 是交易哈希和
日志序号。Wallet 等对应真实成交及费用到齐后才根据终态释放预占。

结算测试明确使用合成的外部 RPC 回执样本，验证解码、精确金额、回滚/未确认/身份
不符的拒绝；真实 Alloy HTTP 客户端连接本地 Axum 样本服务，验证预检、分页、重投
保持同一成交事实及拒绝错误链。它们及离线 SDK、数据库测试均不代表已执行实盘成交；
测试不发送真实订单。

## 源码地图

- Exchange SDK 与 Provider Adapter：`src/exchange/`
- 共享 Exchange 类型：`src/types/src/exchange/`
