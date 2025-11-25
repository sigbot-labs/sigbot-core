package com.wl4g.signaltrading.poc.service.exchange;

import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

import static java.util.Objects.isNull;

/**
 * The {@link StandardExchangeService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@RequiredArgsConstructor
public class StandardExchangeService implements IExchangeService {
    private final SignalTradingProperties config;

    @Override
    public ExchangeInfo create(ExchangeInfo exchangeInfo) {
        throw new UnsupportedOperationException("Standard exchange service does not support persistence operations");
    }

    @Override
    public ExchangeInfo update(@NotNull Long exchangeId, ExchangeInfo exchangeInfo) {
        throw new UnsupportedOperationException("Standard exchange service does not support persistence operations");
    }

    @Override
    public void delete(@NotNull Long exchangeId) {
        throw new UnsupportedOperationException("Standard exchange service does not support persistence operations");
    }

    @Override
    public ExchangeInfo get(@NotNull Long exchangeId) {
        if (isNull(exchangeId) || exchangeId <= 0) {
            throw new IllegalArgumentException("Exchange ID must not be null and greater than zero.");
        }
        throw new UnsupportedOperationException("No implementation");
    }

    @Override
    public List<ExchangeInfo> getAll() {
        throw new UnsupportedOperationException("No implementation");
    }
}
