package com.wl4g.signaltrading.poc.service.trading;

import com.binance.connector.client.common.ApiException;
import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration;
import com.wl4g.signaltrading.poc.service.TradeStatisticsService;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;

/**
 * Unified the Trading Service and Integrated the operations below:
 * - Opening: Market and Limited Order
 * - Closing: Manual Order
 * - Stop-Loss: Market and Limited Order
 * - Stop-Profit: Market and Limited Order
 *
 * @author James Wong
 */
@Slf4j
@RequiredArgsConstructor
public class TradingService {
    private final SignalTradingConfiguration.SignalTradingProperties config;
    private final Map<IExchangeService.ExchangeProvider, IExchangeService> exchanges;
    private final TradeStatisticsService statisticsService;

    private IExchangeService getExchange(@NotNull IExchangeService.ExchangeProvider provider) {
        return ofNullable(exchanges.get(provider)).orElseThrow(()
                -> new RuntimeException(format("Unsupported the exchange provider: %s", provider)));
    }

    /**
     * Execution trading by signal.
     *
     * @param signal The Trading signal
     * @return Trade result.
     */
    public TradeResult executeTrade(TradeSignal signal) {
        try {
            log.info("Executing trade with signal: {}", signal);
            final var exchange = getExchange(signal.getProvider());

            // 1. Opening position.
            final var entryOrderId = exchange.openPosition(signal);
            if (isNull(entryOrderId)) {
                return TradeResult.builder()
                        .success(false)
                        .message("Failed to opening position.")
                        .signal(signal)
                        .build();
            }
            log.info(">>> Executed trade with entry orderId: {}", entryOrderId);

            // 2. Set up stop-loss position.
            Long stopLossOrderId = null;
            if (nonNull(signal.getStopLoss())) {
                try {
                    stopLossOrderId = exchange.setStopLoss(entryOrderId, signal.getStopLoss());
                } catch (Exception e) {
                    // Stop-loss failed, but opened position not impact and record error.
                    log.error("Failed to set stop-loss : {}, but opened position orderId: {}", signal.getStopLoss(), entryOrderId, e);
                }
            }

            // 3. Set up take-profit position.
            Long takeProfitOrderId = null;
            if (nonNull(signal.getStopProfit())) {
                try {
                    takeProfitOrderId = exchange.setStopProfit(entryOrderId, signal.getStopProfit());
                } catch (Exception e) {
                    // Stop-profit failed, but opened position not impact and record error.
                    log.error("Failed to set stop-profit : {}, but opened position orderId: {}", signal.getStopProfit(), entryOrderId, e);
                }
            }

            // 4. Record opened trade.
            statisticsService.recordOpen(signal, entryOrderId, stopLossOrderId, takeProfitOrderId);

            return TradeResult.builder()
                    .success(true)
                    .orderId(entryOrderId)
                    .message(format("Executed Trade ::: Open=%d, Stop-Loss=%d, Stop-Profit=%d", entryOrderId, stopLossOrderId, takeProfitOrderId))
                    .signal(signal)
                    .build();

        } catch (ApiException e) {
            log.error("Failed to call trade {}", e.getMessage(), e);
            return TradeResult.builder()
                    .success(false)
                    .message("Failed to call trade: " + e.getMessage())
                    .signal(signal)
                    .build();
        } catch (Exception e) {
            log.error("Failed to execute trade: {}", e.getMessage(), e);
            return TradeResult.builder()
                    .success(false)
                    .message("Failed to execute trade: " + e.getMessage())
                    .signal(signal)
                    .build();
        }
    }

    public Double getCurrentPrice(IExchangeService.ExchangeProvider provider, String symbol) {
        return getExchange(provider).getCurrentPrice(symbol);
    }

    public List<List<Object>> getKlines(IExchangeService.ExchangeProvider provider, String symbol, String interval, Integer limit) {
        return getExchange(provider).getKlines(symbol, interval, limit);
    }

}
