package com.wl4g.signaltrading.poc.strategy;

import com.wl4g.signaltrading.poc.model.StrategyInfo;
import com.wl4g.signaltrading.poc.trading.TradingEngine;
import com.wl4g.signaltrading.poc.trading.types.TradeSignal;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;

import static com.wl4g.infra.common.reflect.ReflectionUtils2.getDeclaredFields;
import static java.util.Objects.nonNull;
import static org.springframework.util.ReflectionUtils.setField;

/**
 * The {@link IStrategyHandler}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
public interface IStrategyHandler {
    TradeSignal makeSignal(@NotNull Long exchangeId, @NotBlank String symbol);

    interface IStrategyFactory<E extends IStrategyHandler> {
        String getProvider();

        E getInstance(TradingEngine engine, StrategyInfo config);

        static <T> T parse(Class<T> targetCls, StrategyInfo sourceConfig) {
            try {
                // pls ensure convert to targetCls from sourceConfig.getConfiguration() with (Convert dot-separated lowercase letters to camelCase for naming fields)
                final var config = targetCls.getConstructor().newInstance();

                Arrays.stream(getDeclaredFields(targetCls))
                        .forEach(f -> {
                            f.setAccessible(true);
                            // Convert dot-separated lowercase letters to camelCase for naming fields
                            // e.g: mainnetSpotApiEndpoint -> mainnet.spot.api.endpoint
                            final var configKey = f.getName().replaceAll("([A-Z])", "-$1").toLowerCase();
                            if (sourceConfig.getParameters().containsKey(configKey)) {
                                final var value = sourceConfig.getParameters().get(configKey);
                                if (nonNull(value)) {
                                    setField(f, config, value);
                                }
                            }
                        });
                return config;
            } catch (Exception ex) {
                throw new IllegalStateException("Failed to parse strategy parameters: " + sourceConfig, ex);
            }
        }
    }
}
