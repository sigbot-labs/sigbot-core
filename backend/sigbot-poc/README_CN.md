# SignalTrading POC

主要基于 Spring Boot + Binance 的量化交易 POC 项目

## Run

```bash
mvn spring-boot:run
```

或

```bash
java -jar target/signaltrading-poc-0.0.1-SNAPSHOT.jar
```

## Strategies

### 1. [E-MJ-SMA (@摸金探长)](./src/main/java/com/wl4g/signaltrading/poc/strategy/MJSMAStrategyHandler.java)

- 根据群聊交易理念实现（并自研增强）
   - **盈亏比 2:1**：即止盈是止损的2倍，胜率只要大于35%就能赚钱
   - **均线过滤**：均线以下只开空，以上只开多
   - **订单类型**：
     - 开仓：市价单（MARKET）
     - 止损：市价止损单（STOP_MARKET）
       - 移动止盈：如做空当盈利为正时，自动下移止损价，确保在小周期突刺回撤（涨）时能安全止盈（至少保本或小盈利），而不是大盈利只在眼前一晃而过（人性贪婪，总想着跌再多点才止盈），最后小周期涨过成本价后亏损
     - 止盈：限价止盈单（TAKE_PROFIT）
   - **胜率统计**：记录所有交易，计算胜率，支持复盘优化

## Notice

- **测试环境**：建议先在 Binance 测试网环境测试
- **风险控制**：这是 POC 项目，实盘交易请谨慎
- **胜率优化**：根据统计结果调整策略参数
- **价格精度**：不同交易对的价格精度不同，可能需要调整格式化逻辑

## API

### 1. 生成并执行交易

```bash
POST /api/trading/execute?symbol=BTCUSDT
```

自动生成交易信号并执行交易。

### 2. 仅生成交易信号

```bash
GET /api/trading/signal?symbol=BTCUSDT
```

生成交易信号但不执行。

### 3. 执行指定交易信号

```bash
POST /api/trading/execute-signal
Content-Type: application/json

{
  "symbol": "BTCUSDT",
  "side": "BUY",
  "entryPrice": 50000.0,
  "stopLossPrice": 49500.0,
  "takeProfitPrice": 51000.0,
  "quantity": 0.001
}
```

### 4. 获取交易统计

```bash
GET /api/trading/statistics?symbol=BTCUSDT
```

### 5. 获取所有交易记录

```bash
GET /api/trading/records
```

## 参考资料

### Binance API

- 获取交易所基础信息
  - [https://developers.binance.com/docs/zh-CN/derivatives/portfolio-margin-pro/general-info](https://developers.binance.com/docs/zh-CN/derivatives/portfolio-margin-pro/general-info)

```bash
# 交易对 - 限流要求
curl -s 'https://data-api.binance.vision/api/v3/exchangeInfo?symbol=ETHUSDT' | jq -r '.rateLimits'
[
  {
    "rateLimitType": "REQUEST_WEIGHT",
    "interval": "MINUTE",
    "intervalNum": 1,
    "limit": 6000
  },
  {
    "rateLimitType": "ORDERS",
    "interval": "SECOND",
    "intervalNum": 10,
    "limit": 100
  },
  {
    "rateLimitType": "ORDERS",
    "interval": "DAY",
    "intervalNum": 1,
    "limit": 200000
  },
  {
    "rateLimitType": "RAW_REQUESTS",
    "interval": "MINUTE",
    "intervalNum": 5,
    "limit": 61000
  }
]

# 交易对 - 支持的订单类型
curl -s 'https://data-api.binance.vision/api/v3/exchangeInfo?symbol=ETHUSDT' | jq -r '.symbols[0].orderTypes'
#[
#  "LIMIT",
#  "LIMIT_MAKER",
#  "MARKET",
#  "STOP_LOSS",
#  "STOP_LOSS_LIMIT",
#  "TAKE_PROFIT",
#]

# 交易对 - 基础资产精度
curl -s 'https://data-api.binance.vision/api/v3/exchangeInfo?symbol=ETHUSDT' | jq -r '.symbols[0].baseAssetPrecision' 
#8
```
