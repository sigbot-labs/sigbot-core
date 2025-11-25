package com.wl4g.signaltrading.poc.strategy;

import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.service.strategy.IStrategyService;
import com.wl4g.signaltrading.poc.strategy.IStrategyHandler.IStrategyFactory;
import com.wl4g.signaltrading.poc.trading.TradingEngine;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The {@link StrategyEngine}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Slf4j
public class StrategyEngine {
    @SuppressWarnings("unused")
    private final SignalTradingProperties config;
    private final Map<String, IStrategyFactory<? extends IStrategyHandler>> factories;
    private final TradingEngine tradingEngine;
    private final IStrategyService strategyService;
    private final Map<Long, IStrategyHandler> strategyHandlerCaching = new ConcurrentHashMap<>(16);

    public StrategyEngine(@NotNull SignalTradingProperties config,
                          @NotNull List<IStrategyFactory<? extends IStrategyHandler>> factories,
                          @NotNull TradingEngine tradingEngine,
                          @NotNull IStrategyService strategyService) {
        this.config = config;
        // Ensure the factories provider name are not duplicated.
        this.factories = factories.stream().collect(toMap(IStrategyFactory::getProvider, f -> f,
                (f1, f2) -> {
                    throw new IllegalArgumentException(format("Duplicate strategy factory provider name: %s", f1.getProvider()));
                }));
        this.tradingEngine = tradingEngine;
        this.strategyService = strategyService;
    }

    public IStrategyHandler get(@NotBlank Long strategyId) {
        if (isNull(strategyId)) {
            throw new IllegalArgumentException("Strategy Id is required");
        }
        return strategyHandlerCaching.computeIfAbsent(strategyId, k -> {
            final var strategy = strategyService.get(k);
            if (isNull(strategy)) {
                throw new IllegalArgumentException(format("Could not get trade strategy with id: %s", k));
            }
            return ofNullable(factories.get(strategy.validate().getProvider()))
                    .map(f -> f.getInstance(tradingEngine, strategy))
                    .orElseThrow(() -> new IllegalArgumentException(format("Unsupported the strategy provider: %s", strategy.getProvider())));
        });
    }

}
