"""
This is a streaming strategy example for real-time trading and event-driven backtesting.

Code specification:
- Explicitly define global variables (e.g: cfg_btcusdc_dict, kline_btcusdc_dict).
- init(): Strategy initialization function, executed once on first call, can set global variables (no parameters).
- on_process(context): Message processing function, called every time new market data is received.

Notes:
- All symbol+timeframe combinations share the same Python module.
- Global variables set in init() can be accessed in all on_process() calls.
"""

from sigbotlib import indicators, risk, signals, utils, data
from sigbotlib import TradingSignal, PyTradeSide, PyOrderType, PyEntryPosition, PyExitPosition

# Explicitly define global variables.
btcusdc_cfg_dict = {}
btcusdc_3m_kline_series = {}
truthsocial_trump_dict = {}


def init():
    """
    Strategy initialization function
    Executed once on first call, used to initialize strategy state and global variables.

    Note: Global variables can be set here, and these variables will maintain their state in all subsequent on_process calls.
    """
    global btcusdc_cfg_dict, btcusdc_3m_kline_series, truthsocial_trump_dict

    # Initialize configuration for example:
    btcusdc_cfg_dict["series_limit"] = 3000

    btcusdc_3m_kline_series["BTCUSDC::3m"] = []

    print("Strategy initialized")


def on_process(context):
    f"""
    Message processing function, Called every time new market data is received

    Args:
        context: Strategy context object, containing:
            - context.kline_data e.g: {"BTCUSDC::3m": [{"open": 100000, "high": 105000, "low": 99000, "close": 102000, "volume": 100000}]}
            - context.market_data: e.g: {"truthsocial::posts::trump": {"2025-09-29T13:04:21.071Z": "In order to make North Carolina, which has completely lost its furniture business to China, and other Countries, GREAT again, I will be imposing substantial Tariffs on any Country that does not make its furniture in the United States. Details to follow!!! President DJT"}}

    Returns:
        TradingSignal object or None if no signal.
        Example:
        TradingSignal.long(
            symbol="BTCUSDC",
            quantity=0.1,
            price=102000.0,  # Optional, None for market order
            stop_loss_price=98000.0,  # Optional
            stop_profit_price=105000.0,  # Optional
            time=0,  # Optional, k-line timestamp
            description="RSI crossover signal"  # Optional
        )
    """
    global btcusdc_cfg_dict, btcusdc_3m_kline_series, truthsocial_trump_dict

    # Extract the latest event-driven K-lines.
    latest_btcusdc_3m_klines = context.kline_data.get("BTCUSDC::3m")

    # Update to global cache.
    if latest_btcusdc_3m_klines:
        btcusdc_3m_kline_series["BTCUSDC::3m"].append(latest_btcusdc_3m_klines)
        # Rolling to histories overflow.
        if len(btcusdc_3m_kline_series["BTCUSDC::3m"]) > btcusdc_cfg_dict["series_limit"]:
            btcusdc_3m_kline_series["BTCUSDC::3m"].pop(0)

    # Update to global cache.
    if context.market_data:
        for key, value in context.market_data.get("truthsocial::posts::trump").items():
            truthsocial_trump_dict[key] = value

    # Check if data is enough
    if len(btcusdc_3m_kline_series["BTCUSDC::3m"]) < 30:
        return None

    # Calculate technical indicators.
    btc_sma3m20r1p = indicators.sma(latest_btcusdc_3m_klines, 20, 1)
    btc_rsi3m1p = indicators.rsi(latest_btcusdc_3m_klines, 1)

    # Get latest values
    btc_latest_val = latest_btcusdc_3m_klines[:-1]

    # Generate trading signals
    if btc_latest_val > btc_sma3m20r1p and btc_rsi3m1p < 70:
        # 金叉且 RSI 未超买，做多
        latest_kline = latest_btcusdc_3m_klines[-1] if latest_btcusdc_3m_klines else {}
        latest_price = latest_kline.get("close", 0) if isinstance(latest_kline, dict) else 0
        return TradingSignal.long(
            symbol="BTCUSDC",
            quantity=0.1,
            price=latest_price if latest_price > 0 else None,  # Limit order at current price, None for market order
            stop_loss_price=latest_price * 0.98 if latest_price > 0 else None,  # 2% stop loss
            stop_profit_price=latest_price * 1.05 if latest_price > 0 else None,  # 5% take profit
            time=0,  # Will be set by the system
            description="RSI crossover long signal"
        )
    elif btc_latest_val < btc_sma3m20r1p and btc_rsi3m1p > 30:
        # 死叉且 RSI 未超卖，做空
        latest_kline = latest_btcusdc_3m_klines[-1] if latest_btcusdc_3m_klines else {}
        latest_price = latest_kline.get("close", 0) if isinstance(latest_kline, dict) else 0
        return TradingSignal.short(
            symbol="BTCUSDC",
            quantity=0.1,
            price=latest_price if latest_price > 0 else None,  # Limit order at current price, None for market order
            stop_loss_price=latest_price * 1.02 if latest_price > 0 else None,  # 2% stop loss
            stop_profit_price=latest_price * 0.95 if latest_price > 0 else None,  # 5% take profit
            time=0,  # Will be set by the system
            description="RSI crossover short signal"
        )
    else:
        return None
