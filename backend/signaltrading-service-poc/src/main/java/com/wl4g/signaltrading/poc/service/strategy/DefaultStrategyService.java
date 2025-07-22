package com.wl4g.signaltrading.poc.service.strategy;

import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.TradeStrategy;
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

    @Override
    public TradeStrategy get(String id) {
        return getAll().stream()
                .filter(s -> s.getStrategy().getId().equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(format("Could not get trade strategy with %s", id)));
    }

    @Override
    public List<TradeStrategy> getAll() {
        return config.getStrategies();
    }

}
