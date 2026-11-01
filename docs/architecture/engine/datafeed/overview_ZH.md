# Datafeed

[English](overview.md) · [架构总览](../../overview_ZH.md)

**状态：** 已实现租户/Workflow 数据面组件，具备持久事件时间契约、多种摄取协议和有界查询。

`SigbotDatafeedIngestor` 是 Controller 管理的租户/Workflow 行情与新闻摄取组件，拥有外部
Source Session、规范化、质量检查与规范 Feed Event 发布。当前部署按 tenant/workflow
隔离；未来连接池优化也必须保留这些边界。

## 保留设计：逻辑 Binding 与物理复用

未来 Source 共享优化可以让每个 Workflow 拥有逻辑 Source Binding，再把兼容 Binding
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

此保留 Key 刻意不含 `workflow_id`。所有字段一致时，公共市场/新闻 Feed 可以复用；私有
账户 Stream 必须包含 Credential 与 Entitlement Scope，禁止跨安全边界共享。最后
一个 Binding 释放后，Worker 才 Drain 并关闭。

Controller 从冻结 Workflow Snapshot 创建 Datafeed Node。单实例内由 Datafeed 独占
Source Connection Pool、Rate Limit、Retry/Backoff、Health 和有界历史/查询状态。
跨 Workflow Source 共享属于保留优化，不是当前运行时能力。

## 事件契约

`DatafeedEvent` 是 Durable Envelope 承载的不可变规范 Payload，包含 `source`、稳定
`source_event_id`、`symbol`、`event_type`、事件/可得时间、序列、Source Revision、可选
规范内容和 JSON `raw`。Envelope Version、tenant/Workflow/run/node Scope、Produced Time
与 Trace Context 保留在 `DurableMessage`。

Strategy 与 Evaluator 优先消费规范字段；特定事件类型所需的 Provider Detail 当前仍在
`raw` 中，因此它是有界兼容边界，尚不是覆盖所有行情/新闻形状的完整类型化 Enum。UTC
是规范时间基准；Decimal 解析必须保留 Source Precision。

一个规范事件只向普通 Topic 发布一次。Evaluator 与 Strategy Runner 创建不同
Subscription Group，确保两个角色各收一份；同一角色的 Replica 可以共用本角色
Group。`$share` 永远不属于 Publish Topic。投递语义由
[Messager](../messager/overview_ZH.md)拥有。

## 扩展点

| 角色 | 契约 | 示例 |
|---|---|---|
| Service Root | `SigbotDatafeedIngestor` | 组件启动与 Provider 注册 |
| Source Contract | `IDatafeed` | `BinanceDatafeed`、`TwitterDatafeed`、`TruthSocialDatafeed`、政策 Source |
| Persistence Boundary | `PersistedDatafeedIngestor` | Canonical Fact 与 Outbox |
| Normalization | `NewsNormalizer` | Provider-to-canonical 新闻映射 |
| Query Runtime | `MarketQueryService`、`ComputeExecutor` | LATEST/AS_OF 与有界计算 |

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

`SigbotDatafeedIngestor` 与 `IDatafeed` 已通过唯一 Topic 生成器发布规范化、带版本事件。
已实现 Twitter/X、TruthSocial 的 REST/MCP/A2A，Fed/Treasury RSS、Binance 公共行情、
持久 Revision/Watermark、历史查询、Compute Result 与 LATEST/AS_OF Snapshot；Binance
和 Hyperliquid 还有显式主动快照映射。

剩余缺口是覆盖宽度：完整清算热力图、借贷/资金费率/持仓比、更多 Venue 和生产规模
Source Pooling 不能由现有 Adapter 推导出来。

## 源码地图

- Datafeed 组件与 Provider：`src/component/datafeed/`
- Datafeed Repository：`src/core/src/modules/datafeed/storage/`
- Feed 与 Message 类型：`src/types/src/datafeed/`
- Messager SDK：`src/messager/`
