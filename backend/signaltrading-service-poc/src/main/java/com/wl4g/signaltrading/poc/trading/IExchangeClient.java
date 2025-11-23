package com.wl4g.signaltrading.poc.trading;

import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import com.wl4g.signaltrading.poc.trading.types.KlineResult;
import com.wl4g.signaltrading.poc.trading.types.PriceResult;
import com.wl4g.signaltrading.poc.trading.types.TradeSignal;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;

import static com.wl4g.infra.common.reflect.ReflectionUtils2.getDeclaredFields;
import static java.util.Objects.nonNull;
import static org.springframework.util.ReflectionUtils.getAllDeclaredMethods;
import static org.springframework.util.ReflectionUtils.setField;

/**
 * The {@link IExchangeClient}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
public interface IExchangeClient {

    // Get current price by currency pair symbol
    PriceResult getCurrentPrice(String symbol);

    // Get klines by currency pair symbol and interval
    List<KlineResult> getKlines(String symbol, String interval, Integer limit);

    // Opening position.
    Long openPosition(TradeSignal signal);

    // Set up stop loss.
    Long setStopLoss(Long originalOrderId, TradeSignal.StopPosition position);

    // Set up stop profit.
    Long setStopProfit(Long originalOrderId, TradeSignal.StopPosition position);

    // Unified format price.
    default Double formatPrice(Double price) {
        return BigDecimal.valueOf(price)
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
    }

    // Unified format quantity.
    default Double formatQuantity(Double quantity) {
        return BigDecimal.valueOf(quantity)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }

    enum ExchangeProvider {
        // The CEX exchanges.
        BINANCE,
        OKX,
        COINBASE,
        BITGET,
        BYBIT,
        KRAKEN,
        // The DEX exchanges.
        HYPERLIQUID,
        LIGHTER
    }

    interface IExchangeFactory<E extends IExchangeClient> {
        ExchangeProvider getProvider();

        E getInstance(ExchangeInfo config);

        static <T> T parse(Class<T> targetCls, ExchangeInfo sourceConfig) {
            try {
                // pls ensure convert to targetCls from sourceConfig.getConfiguration() with (Convert dot-separated lowercase letters to camelCase for naming fields)
                final var config = targetCls.getConstructor().newInstance();
                Arrays.stream(getDeclaredFields(targetCls)).forEach(f -> {
                    f.setAccessible(true);
                    final var fieldName = f.getName();
                    // Convert dot-separated lowercase letters to camelCase for naming fields
                    // e.g: mainnetSpotApiEndpoint -> mainnet.spot.api.endpoint
                    final var configKey = f.getName().replaceAll("([A-Z])", "-$1").toLowerCase();
                    if (sourceConfig.getConfiguration().containsKey(configKey)) {
                        final var value = sourceConfig.getConfiguration().get(configKey);
                        if (nonNull(value)) {
                            setField(f, config, value);
                        }
                    }
                });
                return config;
            } catch (Exception ex) {
                throw new IllegalStateException("Failed to parse exchange config: " + sourceConfig, ex);
            }
        }
    }

}
