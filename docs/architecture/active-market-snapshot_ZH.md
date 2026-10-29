# 主动多所行情快照

`ctx.market.snapshot(query)` 仍只生成 Workflow 请求。网络访问在宿主 Datafeed 执行，Python worker 不持有连接或交易凭据。Exchange 提供可复用的只读 SDK 适配器，Datafeed 是市场事实的唯一持久化和发布方。

## 显式启用

在现有 Datafeed 节点 `datafeed_config.properties` 中增加 `active_sources`。该属性承载 JSON，解析为拒绝未知字段的 `Vec<PublicMarketSourceConfig>`；未提供或为空数组时保持原有 PG 查询行为。

```yaml
properties:
  snapshot_input_node_ids: "5"
  active_sources: >-
    [{"source":"binance","provider":"binance_spot",
      "symbols":{"HYPEUSDC":"HYPEUSDC"},
      "timeout_ms":5000,"min_request_interval_ms":1000},
     {"source":"hyperliquid","provider":"hyperliquid_spot",
      "symbols":{"HYPEUSDC":"@107"},
      "timeout_ms":5000,"min_request_interval_ms":1000}]
```

租户、环境、space、Workflow、节点和 run 继续使用现有运行配置。`source` 必须与查询中的来源一致；`symbols` 是查询品种到交易所原生品种的显式映射。上例在 2026-10-28 通过真实 Binance `exchangeInfo` 与 Hyperliquid `spotMeta` 核对，二者为 HYPE/USDC。新的映射必须核对实际资产和计价币，不会自动把现货、永续、包装资产或 USDT/USDC 视为同一种标的。

支持 `binance_spot`、`hyperliquid_spot`、`hyperliquid_perpetual`。Hyperliquid 现货使用元数据中的 `@index`（PURR/USDC 是官方例外），永续使用原生 coin 名。适配器固定使用官方公共端点，不接受交易密钥或任意 URL。Binance 使用已安装的官方 `binance-sdk`；Hyperliquid 使用官方 `InfoClient`、`InfoRequest`、HTTP transport 和响应模型。SDK 0.6 的成交响应遗漏 `tid`，通过组合模型补充该字段，避免使用可能全零的 `hash` 去重。

## 查询及时间语义

- 实时时钟下，配置了 `active_sources` 的 `LATEST` 主动获取每一个 `sources × symbols` 组合的最新真实成交；任一组合未配置、无数据、失败或不满足时间约束，返回 `QueryFailure`，不返回部分成功，不退回旧缓存。
- `AS_OF` 始终只查 PG，并同时限制事件时间与可得时间。虚拟时钟模式不创建主动适配器；SDK 已将回放中的 `LATEST` 冻结为 `AS_OF(clock)`。
- `price` 统一表示 `last_trade`。事件时间取交易所成交时间，可得时间取宿主首次收到该事实的时间；重复获取同事实不会把可得时间刷新为现在。
- 网络完成后，以实际完成时钟验证 `max_age_ms` 与 `max_skew_ms`。低流动性品种可能因为最后成交较旧而失败，不使用抓取时间冒充最新成交时间。
- 多所独立采样，不保证同一时刻的原子快照。最多 32 个主动点、4 个并发读取、整个抓取 15 秒截止时间；每来源有请求间隔与 SDK 超时。该限流属于节点实例，多个实例共享公网 IP 时仍需按交易所总额度配置。

## 持久性与重启

接收请求先用短事务保存 Inbox pending，随后释放数据库事务进行 SDK I/O。成功后在一个事务中保存 `market_event`、原始投递映射、来源检查点、`W/feed/market` Outbox、快照结果 Outbox 和 Inbox done，最后才允许 MQTT ACK。

行情使用原生 trade ID：`{provider}:{venue_symbol}:trade:{id}`，外部 `seq` 为该成交 ID，初始 `revision=0`；这不是新增消息关联 ID。相同事实再次查询复用已保存的事件时间、可得时间与 wire identity；相同事实身份的内容变化必须显式修订，不能覆盖已有事实。响应包含本次实际获取并规范化的事实，不借并发写入的另一条“最新”记录替换它。

SDK 原生成交的检查点分区为 `market:{source}:{symbol}:last_trade`，由已规范化的 provider 与 `price_type` 确定；已有 ticker 行情使用 `market:{source}:{symbol}`。不同品种的成交编号、ticker 毫秒序列不会相互覆盖。既有仅按 source 的市场检查点作为历史投影保留，不再写入；新闻继续使用原 source 分区。批量事实按来源、品种和原生身份排序后获取数据库行锁，查询组合顺序不会改变结果排序。

已完成请求重投直接复用持久结果，不再发起 HTTP。抓取期间崩溃时，原 MQTT 请求尚未 ACK，持久会话重投后可恢复 pending 请求；数据库并发竞争由 Inbox 锁和市场事实键收敛。`requestSpanId` 仍只放在短期回复缓存，不写市场事实、Inbox 或 Outbox。

## 验证入口与范围

`src/exchange/tests/client/public_market_test.rs` 覆盖配置、真实交易编号、未来时间和缺失字段；其中 ignored 测试只读真实 Binance/Hyperliquid API，不需要资金凭据。`src/datafeed/tests/server/market_query_test.rs` 使用真实 PG 验证 AS_OF 不调用主动来源、失败不写部分事实、重复请求及重启；第二个 ignored 测试同时调用两个真实公共 API 并核对原子持久结果。

这些测试需由统一构建和验收入口执行；代码中的 ignored 标记不代表已通过。此功能没有实现深度撮合、流动性承诺、清算热力图或衍生指标。

官方依据：[Binance 公共行情端点](https://developers.binance.com/en/docs/products/spot/faqs/market_data_only)、[Hyperliquid Info 与品种规则](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)、[Hyperliquid 官方 Rust SDK](https://github.com/hyperliquid-dex/hyperliquid-rust-sdk)。
