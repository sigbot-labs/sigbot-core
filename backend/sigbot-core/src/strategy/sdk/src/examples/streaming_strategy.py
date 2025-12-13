"""
流式策略示例
适用于实盘交易和事件驱动回测
"""

import sigbotlib

# env 会自动注入，包含：
# - env.close, env.high, env.low, env.open, env.volume (Series 对象)
# - env.symbol, env.timeframe (字符串)
# - env.parameters (字典)

# 计算技术指标
ma5 = sigbotlib.sma(env.close, 5)
ma30 = sigbotlib.sma(env.close, 30)
rsi = sigbotlib.rsi(env.close, 14)

# 获取最新值
ma5_val = ma5.get(0)
ma30_val = ma30.get(0)
rsi_val = rsi.get(0)
current_price = env.close.get(0)

# 生成交易信号
if ma5_val > ma30_val and rsi_val < 70:
    # 金叉且 RSI 未超买，做多
    result = {
        "signal": "long",
        "side": "buy",
        "price": current_price,
        "quantity": 0.1,
        "stop_loss": current_price * 0.98,  # 2% 止损
        "take_profit": current_price * 1.05,  # 5% 止盈
    }
elif ma5_val < ma30_val and rsi_val > 30:
    # 死叉且 RSI 未超卖，平仓
    result = {
        "signal": "close",
        "side": "sell",
        "price": current_price,
    }
else:
    result = None
