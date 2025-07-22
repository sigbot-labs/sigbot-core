package com.wl4g.signaltrading.poc.service.strategy;

import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.TradeStrategy;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * The {@link PersistStrategyService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@RequiredArgsConstructor
public class PersistStrategyService implements IStrategyService {
    private final SignalTradingProperties config;

    @Override
    public TradeStrategy get(String id) {
        throw new UnsupportedOperationException("No implementation");
    }

    @Override
    public List<TradeStrategy> getAll() {
        throw new UnsupportedOperationException("No implementation");
    }
}
