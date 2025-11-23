package com.wl4g.signaltrading.poc.service.exchange;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.isNull;

/**
 * The {@link DefaultExchangeService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@RequiredArgsConstructor
public class DefaultExchangeService implements IExchangeService {
    private final SignalTradingProperties config;
    private final List<ExchangeInfo> exchanges;

    public DefaultExchangeService(SignalTradingProperties config) {
        this.config = config;
        this.exchanges = config.getDefaultExchanges();
    }

    @Override
    public ExchangeInfo get(@NotNull Long exchangeId) {
        if (isNull(exchangeId) || exchangeId <= 0) {
            throw new IllegalArgumentException("Exchange ID must not be null and greater than zero.");
        }
        return getAll().stream()
                .filter(s -> exchangeId.equals(s.getId()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(format("Could not get trade strategy with %s", exchangeId)));
    }

    @Override
    public List<ExchangeInfo> getAll() {
        return exchanges;
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
}
