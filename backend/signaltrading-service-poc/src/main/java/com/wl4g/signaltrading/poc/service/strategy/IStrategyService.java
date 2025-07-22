package com.wl4g.signaltrading.poc.service.strategy;

import com.wl4g.signaltrading.poc.model.TradeStrategy;

import java.util.List;

/**
 * The {@link IStrategyService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
public interface IStrategyService {
    TradeStrategy get(String id);

    List<TradeStrategy> getAll();
}
