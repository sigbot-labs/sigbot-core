package com.wl4g.signaltrading.poc.service.exchange;

import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;

/**
 * The {@link IExchangeService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
public interface IExchangeService {
    ExchangeInfo get(@NotNull @Min(0) Long exchangeId);

    List<ExchangeInfo> getAll();
}
