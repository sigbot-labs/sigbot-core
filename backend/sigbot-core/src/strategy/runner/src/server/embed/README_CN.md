# Sigbot Strategy SDK

高性能量化交易策略 SDK，提供流式和批处理两种执行模式，类似 Flink 的统一架构设计。

## 架构设计

### 执行模式

#### 1. 流式模式 (Streaming Mode)
- **适用场景**: 实盘交易、事件驱动回测
- **特点**: 每根 K 线触发一次，状态缓存，无需重算历史
- **性能**: 低延迟，适合高频交易
- **类似**: TradingView PineScript、banta 状态缓存模式

#### 2. 批处理模式 (Batch Mode)
- **适用场景**: 策略研究、一次性分析
- **特点**: 一次性传入全部数据，并行计算
- **性能**: 高吞吐，适合批量分析
- **类似**: TA-Lib、pandas-ta 批处理模式

## 快速开始

### 流式模式示例

```python
import sigbot_sdk

# 在策略代码中，env 会自动注入
# env.close, env.high, env.low, env.open, env.volume 是 Series 对象

# 计算技术指标
ma5 = sigbot_sdk.sma(env.close, 5)
ma30 = sigbot_sdk.sma(env.close, 30)

# 获取最新值
ma5_val = ma5.get(0)  # 最新值
ma30_val = ma30.get(0)

# 生成交易信号
if ma5_val > ma30_val:
    result = {
        "signal": "long",
        "price": env.close.get(0),
        "quantity": 0.1
    }
else:
    result = None
```

### 批处理模式示例

```python
import sigbot_sdk

# klines 是完整的 K 线列表
# closes, highs, lows, opens, volumes 是价格数组

# 并行计算所有指标
ma5 = sigbot_sdk.sma_batch(closes, 5)
rsi = sigbot_sdk.rsi_batch(closes, 14)

# 生成信号列表
signals = []
for i in range(len(klines)):
    if ma5[i] > ma30[i] and rsi[i] < 70:
        signals.append({
            "index": i,
            "signal": "long",
            "price": closes[i]
        })

result = signals
```

## SDK API 参考

### Series 类型

Series 是流式模式的核心数据结构，维护滚动窗口的状态。

```python
# 创建 Series
series = sigbot_sdk.Series(max_length=1000)

# 添加数据
series.append(100.0)

# 获取值（0 = 最新，-1 = 前一个）
value = series.get(0)  # 最新值
prev_value = series.get(-1)  # 前一个值

# 获取长度
length = series.len()

# 转换为列表
values = series.to_list()
```

### 技术指标

#### 趋势指标

**SMA (Simple Moving Average)**
```python
# 流式模式
ma = sigbot_sdk.sma(series, period=20)

# 批处理模式
ma = sigbot_sdk.sma_batch(prices, period=20)
```

**EMA (Exponential Moving Average)**
```python
# 流式模式
ema = sigbot_sdk.ema(series, period=20)

# 批处理模式
ema = sigbot_sdk.ema_batch(prices, period=20)
```

#### 动量指标

**RSI (Relative Strength Index)**
```python
# 流式模式
rsi = sigbot_sdk.rsi(series, period=14)

# 批处理模式
rsi = sigbot_sdk.rsi_batch(prices, period=14)
```

## 环境变量

策略执行环境会自动注入以下变量：

### 流式模式
- `env.close`: Series - 收盘价序列
- `env.high`: Series - 最高价序列
- `env.low`: Series - 最低价序列
- `env.open`: Series - 开盘价序列
- `env.volume`: Series - 成交量序列
- `env.symbol`: str - 交易对符号
- `env.timeframe`: str - 时间周期
- `env.parameters`: dict - 策略参数

### 批处理模式
- `klines`: list - 完整 K 线列表
- `closes`: list - 收盘价数组
- `highs`: list - 最高价数组
- `lows`: list - 最低价数组
- `opens`: list - 开盘价数组
- `volumes`: list - 成交量数组
- `parameters`: dict - 策略参数

## 性能优化

1. **状态缓存**: 流式模式使用状态缓存，避免重复计算
2. **批量处理**: 批处理模式使用向量化计算
3. **零拷贝**: 使用 PyArrow 实现 Rust ↔ Python 零拷贝数据传递
4. **并行计算**: 支持多指标并行计算（使用 rayon）

## 测试

### 运行单元测试
```bash
cargo test --lib
```

### 运行集成测试
```bash
cargo test --test indicator_tests
```

### 运行对比测试（与 TradingView/TA-Lib 对比）
```bash
cargo test --test comparison
```

## 依赖

### Python 包（预安装）
- `polars`: 高性能数据处理
- `pandas`: 数据分析
- `pyarrow`: 零拷贝数据传递

### Rust 依赖
- `pyo3`: Python 绑定
- `serde`: 序列化
- `anyhow`: 错误处理

## 兼容性

### 指标兼容性矩阵

| 指标 | TradingView | TA-Lib | Pandas-TA | 状态 |
|------|-------------|--------|-----------|------|
| SMA  | ✔          | ✔      | ✔         | ✅   |
| EMA  | ✔          | ✔      | ✔         | ✅   |
| RSI  | ✔          | ✔      | ✔         | ✅   |

## 开发计划

### Phase 1 ✅
- [x] Python 包安装机制
- [x] 流式执行器基础框架
- [x] 核心指标（SMA, EMA, RSI）
- [x] 基础测试框架

### Phase 2 🚧
- [ ] 完整流式执行器实现
- [ ] 批处理执行器实现
- [ ] SDK 模块导出和基础函数
- [ ] TradingView 对比测试

### Phase 3 📋
- [ ] 完整指标库实现
- [ ] 自动化测试和 CI 集成
- [ ] 性能优化和基准测试
- [ ] 文档和使用示例

## 参考

- [banta](https://github.com/banbox/banta) - Banbot 技术分析库
- [TradingView PineScript](https://www.tradingview.com/pine-script-docs/) - PineScript 文档
- [TA-Lib](https://ta-lib.org/) - 技术分析库
- [PyO3](https://pyo3.rs/) - Rust Python 绑定

## 许可证

GNU GENERAL PUBLIC LICENSE Version 3
