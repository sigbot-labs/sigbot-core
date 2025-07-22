package com.wl4g.signaltrading.poc.service.trading;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * The {@link IExchangeService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
public interface IExchangeService {

    // Get current exchange provider
    ExchangeProvider getProvider();

    // Get current price by currency pair symbol
    Double getCurrentPrice(String symbol);

    // Get klines by currency pair symbol and interval
    List<List<Object>> getKlines(String symbol, String interval, Integer limit);

    // Opening position.
    Long openPosition(TradeSignal signal);

    // Set up stop loss.
    Long setStopLoss(Long originalOrderId, TradeSignal.StopPosition position);

    // Set up stop profit.
    Long setStopProfit(Long originalOrderId, TradeSignal.StopPosition position);

    // Unified format price.
    default Double formatPrice(Double price) {
        return BigDecimal.valueOf(price)
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
    }

    // Unified format quantity.
    default Double formatQuantity(Double quantity) {
        return BigDecimal.valueOf(quantity)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }

    enum ExchangeProvider {
        // The CEX exchanges.
        BINANCE,
        OKX,
        COINBASE,
        BITGET,
        BYBIT,
        KRAKEN,
        // The DEX exchanges.
        HYPERLIQUID,
        LIGHTER
    }

}
