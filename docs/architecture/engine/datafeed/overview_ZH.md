# Datafeed

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 规范性目标架构；当前仅有部分 Provider 骨架。

`SigbotDatafeed` 是共享的市场与新闻摄取 daemon，拥有外部 Source Session、数据
规范化、质量检查与规范 Feed Event 发布。多个 Tenant 和 Strategy 复用相同公共
上游连接，不为每个策略重复创建 Datafeed Pod。

## 逻辑 Binding 与物理复用

每个 `StrategyPipeline` 拥有一个逻辑 `DatafeedBinding`。daemon 把所有活跃 Binding
转换为按规范化 `feed_key` 引用计数的 Source Worker：

```text
feed_key = hash(
  provider,
  credential_scope,
  entitlement_scope,
  market,
  instrument,
  channel,
  interval,
  normalization_version
)
```

`pipeline_id` 刻意不在 Key 中。所有字段一致时，公共市场/新闻 Feed 可以复用；私有
账户 Stream 必须包含 Credential 与 Entitlement Scope，禁止跨安全边界共享。最后
一个 Binding 释放后，Worker 才 Drain 并关闭。

Controller 创建和释放 Binding，不创建重复 daemon。Datafeed 独占 Source Connection
Pool、上游 Subscription Diff、Rate Limit、Retry/Backoff、Health 与 Worker 回收。

## 事件契约

`SigbotFeedEvent` 是不可变规范 Envelope，包含：

- 稳定 Event/Feed ID、Provider、Market、Instrument、Channel 与 Schema Version；
- Source Event Time、Ingest Time、Sequence/Watermark 与 Provenance；
- Trade、Order-book Update、Kline、Ticker、Funding、Social/News Item 或 Sentiment
  Evidence 等类型化 Payload；
- Gap、Duplicate、Late、Correction 与 Source Status 等 Quality Flag。

Provider 原始字段只保存在可选且带版本的 Extension 中。Strategy 与 Evaluator 消费
规范 Payload，不依赖 Provider JSON 形状。UTC 是规范时间基准；Decimal 值保留 Source
Precision。

一个规范事件只向普通 Topic 发布一次。Evaluator 与 Strategy Runner 创建不同
Subscription Group，确保两个角色各收一份；同一角色的 Replica 可以共用本角色
Group。`$share` 永远不属于 Publish Topic。投递语义由
[Messager](../messager/overview_ZH.md)拥有。

## 扩展点

| 角色 | 契约 | 示例 |
|---|---|---|
| Service Root | `SigbotDatafeed` | 共享 daemon 与 Worker Registry |
| 外部 Source | `ISigbotDatafeedSource` | `SigbotBinanceMarketDataSource`、`SigbotTwitterNewsSource`、`SigbotTruthSocialNewsSource` |
| 历史 Adapter | `ISigbotDatafeedSource` | 由 Backtest 组合的 `SigbotHistoricalReplaySource`，不属于 Live daemon |
| Normalization | `ISigbotFeedNormalizer` | Provider 与 Schema-version 专用 Normalizer |

External Source 负责 Provider Transport 与 Authentication；Normalizer 负责
Provider-to-canonical 映射。两者都不能运行 Strategy 或 Order 逻辑。

## 可靠性与安全不变量

- Reconnect 必须从 Provider Cursor 恢复或显式产生 Gap，禁止静默伪造连续性。
- 重复 Source Message 必须得到同一个 Deduplication Key。
- 每个 Source 的 Backpressure Policy 必须显式。只有声明为 Snapshot 的 Stream 可以
  Lossy Coalescing；有序 Trade 或 Account Event 禁止丢弃。
- 公共和私有 Feed 使用不同 Credential、Cache、Log 与 Sharing Boundary；Event 禁止
  包含 Provider Secret。
- 单个 Source 故障与无关 Worker 隔离；Retry 遵守 Provider Rate Limit 与 Circuit
  State。
- 启用历史归档时写入 Datafeed 私有 Market-history Store，而不是核心控制面数据库。

## 实现对齐

当前源码暴露 `SigbotDatafeedIngestor` 与 `IDatafeed`，并有 Binance、
Twitter、TruthSocial Provider 骨架。多个 Subscribe/Process 路径为空或未实现；事件
尚未转换成上述规范契约，发布时可能保留未展开 Topic Placeholder。Source Sharing
Key、逻辑 Binding、Backpressure 与可靠 Fan-out 尚未完成。

目标公开契约为 `SigbotDatafeed` 与 `ISigbotDatafeedSource`。Coinbase 或其他新闻
Source 等扩展，只有存在具体 Adapter 与 Contract Test 后才算支持。

## 源码地图

- Datafeed daemon 与 Provider：`src/component/datafeed/`
- Feed 与 Message 类型：`src/types/src/`
- Messager SDK：`src/messager/`
