package com.wl4g.signaltrading.poc.strategy;

import com.wl4g.infra.common.lang.tuples.Tuple4;
import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.TradeStrategy;
import com.wl4g.signaltrading.poc.model.TradeStrategy.StrategyProvider;
import com.wl4g.signaltrading.poc.service.strategy.IStrategyService;
import com.wl4g.signaltrading.poc.service.trading.IExchangeService.ExchangeProvider;
import com.wl4g.signaltrading.poc.service.trading.TradingService;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The {@link StrategyEngine}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Slf4j
@RequiredArgsConstructor
public class StrategyEngine {
    private final SignalTradingProperties config;
    private final TradingService tradingService;
    private final IStrategyService strategyService;
    private final Map<String, IStrategyHandler> strategyHandlers = new ConcurrentHashMap<>(16);

    public IStrategyHandler get(@NotBlank String strategyId, @NotNull ExchangeProvider exchange) {
        if (isBlank(strategyId)) {
            throw new IllegalArgumentException("Strategy Id and exchange must be specified");
        }
        return strategyHandlers.computeIfAbsent(strategyId, k -> {
            final var strategy = strategyService.get(k);
            return strategy.getStrategy().getType().getInitializer().apply(new Tuple4(config, tradingService, exchange, strategy));
        });
    }

}
