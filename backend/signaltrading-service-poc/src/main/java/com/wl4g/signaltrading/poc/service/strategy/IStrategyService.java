package com.wl4g.signaltrading.poc.service.strategy;

import com.wl4g.signaltrading.poc.model.StrategyInfo;

import java.util.List;

/**
 * The {@link IStrategyService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
public interface IStrategyService {
    StrategyInfo get(Long strategyId);

    List<StrategyInfo> getAll();
}
