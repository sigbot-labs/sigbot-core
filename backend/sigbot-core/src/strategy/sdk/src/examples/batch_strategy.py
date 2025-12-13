"""
批处理策略示例
适用于策略研究和回测分析
"""

import sigbotlib

# klines, closes, highs, lows, opens, volumes 会自动注入

# 并行计算所有指标
ma5 = sigbotlib.sma_batch(closes, 5)
ma30 = sigbotlib.sma_batch(closes, 30)
rsi = sigbotlib.rsi_batch(closes, 14)

# 生成信号列表
signals = []

for i in range(len(klines)):
    kline = klines[i]
    
    # 需要足够的 K 线数据
    if i < 30:
        continue
    
    ma5_val = ma5[i - 30] if i >= 30 else None
    ma30_val = ma30[i - 30] if i >= 30 else None
    rsi_val = rsi[i - 14] if i >= 14 else None
    
    if ma5_val is None or ma30_val is None or rsi_val is None:
        continue
    
    # 交易逻辑
    if ma5_val > ma30_val and rsi_val < 70:
        signals.append({
            "index": i,
            "time": kline["open_time"],
            "signal": "long",
            "side": "buy",
            "price": kline["close_price"],
            "quantity": 0.1,
        })
    elif ma5_val < ma30_val and rsi_val > 30:
        signals.append({
            "index": i,
            "time": kline["open_time"],
            "signal": "close",
            "side": "sell",
            "price": kline["close_price"],
        })

result = {
    "signals": signals,
    "total_signals": len(signals),
    "ma5_last": ma5[-1] if ma5 else None,
    "ma30_last": ma30[-1] if ma30 else None,
    "rsi_last": rsi[-1] if rsi else None,
}
