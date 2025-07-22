package com.wl4g.signaltrading.poc.strategy;

import com.wl4g.signaltrading.poc.model.TradeStrategy.StrategyProvider;
import com.wl4g.signaltrading.poc.service.trading.TradeSignal;
import jakarta.validation.constraints.NotBlank;

/**
 * The {@link IStrategyHandler}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
public interface IStrategyHandler {
    StrategyProvider getProvider();

    TradeSignal makeSignal(@NotBlank String symbol);
}
