package com.wl4g.signaltrading.poc.service.strategy;

import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.StrategyInfo;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * The {@link StandardStrategyService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@RequiredArgsConstructor
public class StandardStrategyService implements IStrategyService {
    private final SignalTradingProperties config;

    @Override
    public StrategyInfo get(Long strategyId) {
        throw new UnsupportedOperationException("No implementation");
    }

    @Override
    public List<StrategyInfo> getAll() {
        throw new UnsupportedOperationException("No implementation");
    }
}
