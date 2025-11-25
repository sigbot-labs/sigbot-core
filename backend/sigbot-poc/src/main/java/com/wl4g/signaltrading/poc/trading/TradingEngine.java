package com.wl4g.signaltrading.poc.trading;

import com.binance.connector.client.common.ApiException;
import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import com.wl4g.signaltrading.poc.service.exchange.IExchangeService;
import com.wl4g.signaltrading.poc.strategy.TradeStatisticsService;
import com.wl4g.signaltrading.poc.trading.IExchangeClient.ExchangeProvider;
import com.wl4g.signaltrading.poc.trading.IExchangeClient.IExchangeFactory;
import com.wl4g.signaltrading.poc.trading.types.KlineResult;
import com.wl4g.signaltrading.poc.trading.types.PriceResult;
import com.wl4g.signaltrading.poc.trading.types.TradeResult;
import com.wl4g.signaltrading.poc.trading.types.TradeSignal;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isAnyBlank;

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
@Getter
public class TradingEngine {
    @SuppressWarnings("unused")
    private final SignalTradingProperties config;
    private final Map<ExchangeProvider, IExchangeFactory<? extends IExchangeClient>> factories;
    private final IExchangeService exchangeService;
    private final TradeStatisticsService statisticsService;

    public TradingEngine(@NotNull SignalTradingProperties config,
                         @NotNull List<IExchangeFactory<? extends IExchangeClient>> factories,
                         @NotNull IExchangeService exchangeService,
                         @NotNull TradeStatisticsService statisticsService) {
        this.config = config;
        this.factories = factories.stream().collect(toMap(IExchangeFactory::getProvider, f -> f));
        this.exchangeService = exchangeService;
        this.statisticsService = statisticsService;
    }

    private IExchangeClient getExchangeClient(@NotNull ExchangeInfo exchange) {
        if (isNull(exchange)) {
            throw new IllegalArgumentException("Exchange must not be null.");
        }
        return ofNullable(factories.get(exchange.validate().getProvider()))
                .map(f -> f.getInstance(exchange))
                .orElseThrow(() -> new IllegalArgumentException(format("Unsupported the exchange provider: %s", exchange.getProvider())));
    }

    /**
     * Execution trading by signal.
     *
     * @param signal The Trading signal
     * @return Trade result.
     */
    public TradeResult executeTrade(@NotNull Long exchangeId, @NotNull TradeSignal signal) {
        if (isNull(signal)) {
            throw new IllegalArgumentException("The trade signal must not be null.");
        }
        try {
            log.info("Executing trade with signal: {}", signal);
            final var client = getExchangeClient(exchangeService.get(exchangeId));

            // 1. Opening position.
            final var entryOrderId = client.openPosition(signal);
            if (isNull(entryOrderId)) {
                return TradeResult.builder()
                        .success(false)
                        .message("Failed to opening position.")
                        .signal(signal)
                        .build();
            }
            log.debug("Executed Trade - entry orderId: {}", entryOrderId);

            // 2. Set up stop-loss position.
            Long stopLossOrderId = null;
            if (nonNull(signal.getStopLoss())) {
                try {
                    stopLossOrderId = client.setStopLoss(entryOrderId, signal.getStopLoss());
                    log.debug("Executed set Stop-Loss - orderId: {}", stopLossOrderId);
                } catch (Exception e) {
                    // Stop-loss failed, but opened position not impact and record error.
                    log.error("Failed to set stop-loss : {}, but opened position orderId: {}", signal.getStopLoss(), entryOrderId, e);
                }
            }

            // 3. Set up take-profit position.
            Long takeProfitOrderId = null;
            if (nonNull(signal.getStopProfit())) {
                try {
                    takeProfitOrderId = client.setStopProfit(entryOrderId, signal.getStopProfit());
                    log.debug("Executed set Take-Profit - orderId: {}", takeProfitOrderId);
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
                    .message(format("Executed Trade - Open=%d, Stop-Loss=%d, Stop-Profit=%d", entryOrderId, stopLossOrderId, takeProfitOrderId))
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

    public TradeResult setStopLoss(@NotNull Long exchangeId,
                                   @NotNull Long originalOrderId,
                                   @NotNull TradeSignal.StopPosition stopLoss) {
        if (isNull(stopLoss) || isNull(originalOrderId)) {
            throw new IllegalArgumentException("The stop-loss and original order ID must not be null.");
        }
        try {
            log.info("Executing stop-loss with exchangeId:{}, originalOrderId: {}, stopLoss: {}", exchangeId, originalOrderId, stopLoss);

            // Set up stop-loss position.
            final var stopOrderId = getExchangeClient(exchangeService.get(exchangeId)).setStopLoss(originalOrderId, stopLoss);
            log.debug("Executed set stop-loss - orderId: {}", stopOrderId);

            return TradeResult.builder()
                    .success(true)
                    .orderId(originalOrderId)
                    .message(format("Executed stop-loss order(Open: %d, Stop: %d)", originalOrderId, stopOrderId))
                    .signal(null)
                    .build();

        } catch (ApiException e) {
            log.error("Failed to call stop-loss trade {}", e.getMessage(), e);
            return TradeResult.builder()
                    .success(false)
                    .message("Failed to call stop-loss trade: " + e.getMessage())
                    .signal(null)
                    .build();
        } catch (Exception e) {
            log.error("Failed to execute trade: {}", e.getMessage(), e);
            return TradeResult.builder()
                    .success(false)
                    .message("Failed to execute trade: " + e.getMessage())
                    .signal(null)
                    .build();
        }
    }

    public TradeResult setStopProfit(@NotNull Long exchangeId,
                                     @NotNull Long originalOrderId,
                                     @NotNull TradeSignal.StopPosition stopProfit) {
        if (isNull(stopProfit) || isNull(originalOrderId)) {
            throw new IllegalArgumentException("The stop-profit and original order ID must not be null.");
        }
        try {
            log.info("Executing stop-profit with exchangeId:{}, originalOrderId: {}, stopProfit: {}", exchangeId, originalOrderId, stopProfit);

            // Set up stop-profit position.
            final var stopOrderId = getExchangeClient(exchangeService.get(exchangeId)).setStopProfit(originalOrderId, stopProfit);
            log.debug("Executed set stop-profit - orderId: {}", stopOrderId);

            return TradeResult.builder()
                    .success(true)
                    .orderId(originalOrderId)
                    .message(format("Executed stop-profit order(Open: %d, Stop: %d)", originalOrderId, stopOrderId))
                    .signal(null)
                    .build();

        } catch (ApiException e) {
            log.error("Failed to call stop-profit trade {}", e.getMessage(), e);
            return TradeResult.builder()
                    .success(false)
                    .message("Failed to call stop-profit trade: " + e.getMessage())
                    .signal(null)
                    .build();
        } catch (Exception e) {
            log.error("Failed to execute trade: {}", e.getMessage(), e);
            return TradeResult.builder()
                    .success(false)
                    .message("Failed to execute trade: " + e.getMessage())
                    .signal(null)
                    .build();
        }
    }

    public PriceResult getCurrentPrice(@NotNull Long exchangeId, @NotNull String symbol) {
        return getExchangeClient(exchangeService.get(exchangeId)).getCurrentPrice(symbol);
    }

    public List<KlineResult> getKlines(@NotNull Long exchangeId, @NotNull String symbol, @NotBlank String interval, @NotNull Integer limit) {
        if (isAnyBlank(symbol, interval)) {
            throw new IllegalArgumentException("Symbol and interval must not be blank.");
        }
        return getExchangeClient(exchangeService.get(exchangeId)).getKlines(symbol, interval, limit);
    }

}
