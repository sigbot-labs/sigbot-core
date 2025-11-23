package com.wl4g.signaltrading.poc.service.strategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.StrategyInfo;
import lombok.RequiredArgsConstructor;

import java.util.List;

import static java.lang.String.format;

/**
 * The {@link DefaultStrategyService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@RequiredArgsConstructor
public class DefaultStrategyService implements IStrategyService {
    private final SignalTradingProperties config;
    private final List<StrategyInfo> strategies;

    public DefaultStrategyService(SignalTradingProperties config) {
        this.config = config;
        this.strategies = config.getDefaultStrategies();
    }

    @Override
    public StrategyInfo get(Long strategyId) {
        return getAll().stream()
                .filter(s -> s.getId().equals(strategyId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(format("Could not get trade strategy with %s", strategyId)));
    }

    @Override
    public List<StrategyInfo> getAll() {
        return strategies;
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
}
