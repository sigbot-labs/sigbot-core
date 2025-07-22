package com.wl4g.signaltrading.poc.service.trading;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * The {@link HyperliquidService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Slf4j
@RequiredArgsConstructor
public class HyperliquidService extends DEXService {

    @Override
    public ExchangeProvider getProvider() {
        return ExchangeProvider.HYPERLIQUID;
    }

    @Override
    public Double getCurrentPrice(String symbol) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<List<Object>> getKlines(String symbol, String interval, Integer limit) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Long openPosition(TradeSignal signal) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Long setStopLoss(Long originalOrderId, TradeSignal.StopPosition position) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Long setStopProfit(Long originalOrderId, TradeSignal.StopPosition position) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
