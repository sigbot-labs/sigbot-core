package com.wl4g.signaltrading.poc.trading.cex;

import com.binance.connector.client.common.ApiException;
import com.binance.connector.client.common.configuration.SignatureConfiguration;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.DerivativesTradingUsdsFuturesRestApiUtil;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.api.DerivativesTradingUsdsFuturesRestApi;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.Interval;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.KlineCandlestickDataResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.KlineCandlestickDataResponseItem;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.NewOrderRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.Side;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.SymbolPriceTickerV2Response1;
import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import com.wl4g.signaltrading.poc.trading.IExchangeClient;
import com.wl4g.signaltrading.poc.trading.types.KlineResult;
import com.wl4g.signaltrading.poc.trading.types.PriceResult;
import com.wl4g.signaltrading.poc.trading.types.TradeSignal;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;

import java.util.List;

import static com.wl4g.infra.common.lang.TypeConverts.parseDoubleOrNull;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static org.springframework.util.ReflectionUtils.findField;
import static org.springframework.util.ReflectionUtils.getField;

/**
 * The {@link BinanceClient}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Slf4j
@RequiredArgsConstructor
public class BinanceClient extends CEXClient {
    private final DerivativesTradingUsdsFuturesRestApi futuresApi;

    public BinanceClient(BinanceProperties config) {
        this.futuresApi = buildDerivativesTradingUsdsFuturesRestApi(config);
    }

    private DerivativesTradingUsdsFuturesRestApi buildDerivativesTradingUsdsFuturesRestApi(BinanceProperties config) {
        final var clientConfig = DerivativesTradingUsdsFuturesRestApiUtil.getClientConfiguration();
        clientConfig.setUrl(config.getMainnetDerivativesEndpoint());

        final var signatureConfig = new SignatureConfiguration();
        signatureConfig.setApiKey(config.getApiKey());
        signatureConfig.setPrivateKey(config.getSecretKey());
        clientConfig.setSignatureConfiguration(signatureConfig);

        return new DerivativesTradingUsdsFuturesRestApi(clientConfig);
    }

    @Override
    public PriceResult getCurrentPrice(String symbol) throws ApiException {
        final var response = futuresApi.symbolPriceTickerV2(symbol);
        log.debug("Current price: {}", response.getData());
        try {
            final var field = findField(response.getData().getClass(), "instance");
            assert field != null;
            field.setAccessible(true);
            final var instance = (SymbolPriceTickerV2Response1)
                    getField(requireNonNull(field), response.getData());
            return ofNullable(instance)
                    .map(r -> {
                        return PriceResult.builder()
                                .price(parseDoubleOrNull(r.getPrice()))
                                .currentTime(r.getTime())
                                .build();
                    })
                    .orElseThrow(() -> new IllegalStateException(format("Obtain the '%s' binance price is null", symbol)));
        } catch (Exception e) {
            throw new IllegalStateException(format("Failed to get price field, response type: %s", response.getData().getClass().getName()), e);
        }
    }

    /**
     * <a href="https://github.com/binance/binance-connector-java/blob/master/examples/derivatives-trading-usds-futures/src/main/java/com/binance/connector/client/derivatives_trading_usds_futures/rest/marketdata/KlineCandlestickDataExample.java">Binance KlineCandlestickDataExample.java</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<KlineResult> getKlines(String symbol, String interval, Integer limit) throws ApiException {
        try {
            final var interval0 = Interval.fromValue(interval);
            final var response = futuresApi.klineCandlestickData(symbol, interval0, null, null, Long.valueOf(limit));
            KlineCandlestickDataResponse data = response.getData();
            return (List<KlineResult>) data.stream()
                    .map(k -> (KlineCandlestickDataResponseItem) k)
                    .map(k -> KlineResult.builder()
                            .openTime(Long.parseLong(k.get(0)))
                            .openPrice(Double.parseDouble(k.get(1)))
                            .highPrice(Double.parseDouble(k.get(2)))
                            .lowPrice(Double.parseDouble(k.get(3)))
                            .closePrice(Double.parseDouble(k.get(4)))
                            .volume(Double.parseDouble(k.get(5)))
                            .closeTime(Long.parseLong(k.get(6)))
                            .build())
                    .toList();
        } catch (Exception e) {
            throw new IllegalStateException(format("Failed to get Klines data: symbol=%s, interval=%s, limit=%d", symbol, interval, limit), e);
        }
    }

    @Override
    public Long openPosition(TradeSignal signal) throws ApiException {
        try {
            // https://developers.binance.com/docs/zh-CN/derivatives/coin-margined-futures/trade/rest-api
            final var request = new NewOrderRequest();
            request.symbol(signal.getOpenPos().getSymbol());
            request.side(Side.valueOf(signal.getOpenPos().getSide().name()));
            request.type(signal.getOpenPos().getType().name());
            request.quantity(formatQuantity(signal.getOpenPos().getQuantity()));

            log.info("[OPEN_POS] Opening position - symbol={}, side={}, quantity={}",
                    signal.getOpenPos().getSymbol(), signal.getOpenPos().getSide(), signal.getOpenPos().getQuantity());

            final var response = futuresApi.newOrder(request);
            final var orderResponse = response.getData();
            log.info("[OPEN_POS] Opened position - orderId={}, price={}", orderResponse.getOrderId(), orderResponse.getAvgPrice());

            return orderResponse.getOrderId();
        } catch (Exception e) {
            log.error("Failed to opening market position: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Long setStopLoss(Long originalOrderId, TradeSignal.StopPosition position) throws ApiException {
        try {
            final var request = new NewOrderRequest();
            request.symbol(position.getSymbol());
            request.closePosition("true");
            // The stop-loss direction is opposite to the opened direction.
            request.side(Side.valueOf(position.getSide().name()));
            if (position.getType() == TradeSignal.OrderType.MARKET) {
                request.type("STOP_MARKET");
            } else if (position.getType() == TradeSignal.OrderType.LIMITED) {
                request.type("STOP");
                request.stopPrice(formatPrice(position.getPrice()));
            }
            log.info("[STOP_LOSS] Set up - symbol={}, side={}, price={}", position.getSymbol(), position.getSide().opposite(), position.getPrice());

            final var response = futuresApi.newOrder(request);
            final var orderResponse = response.getData();
            log.info("[STOP_LOSS] Set up success - orderId: {}", orderResponse.getOrderId());

            return orderResponse.getOrderId();
        } catch (Exception e) {
            log.error("[STOP_LOSS] Set failed", e);
            throw e;
        }
    }

    @Override
    public Long setStopProfit(Long originalOrderId, TradeSignal.StopPosition position) throws ApiException {
        try {
            final var request = new NewOrderRequest();
            request.symbol(position.getSymbol());
            request.closePosition("true");
            // The stop-profit direction is opened direction.
            request.side(Side.valueOf(position.getSide().name()));
            if (position.getType() == TradeSignal.OrderType.MARKET) {
                request.type("TAKE_PROFIT_MARKET");
            } else if (position.getType() == TradeSignal.OrderType.LIMITED) {
                request.type("TAKE_PROFIT");
                request.stopPrice(formatPrice(position.getPrice()));
            }
            log.info("[STOP_PROFIT] Set up - symbol={}, side={}, price={}", position.getSymbol(), position.getSide().opposite(), position.getPrice());

            final var response = futuresApi.newOrder(request);
            final var orderResponse = response.getData();
            log.info("[STOP_PROFIT] Set up success - orderId: {}", orderResponse.getOrderId());

            return orderResponse.getOrderId();
        } catch (Exception e) {
            log.error("[STOP_PROFIT] Set up failed", e);
            throw e;
        }
    }

    @RequiredArgsConstructor
    public static class BinanceExchangeFactory implements IExchangeFactory<BinanceClient> {
        @Override
        public IExchangeClient.ExchangeProvider getProvider() {
            return IExchangeClient.ExchangeProvider.BINANCE;
        }

        @Override
        public BinanceClient getInstance(ExchangeInfo config) {
            return new BinanceClient(IExchangeFactory.parse(BinanceProperties.class, config));
        }
    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    @Validated
    public static class BinanceProperties {
        private @NotBlank String mainnetSpotApiEndpoint = "https://api.binance.com";
        private @NotBlank String mainnetSpotWsEndpoint = "wss://fstream.binance.com";
        private @NotBlank String mainnetDerivativesEndpoint = "https://fapi.binance.com";
        private @NotBlank String testnetSpotApiEndpoint = "https://testnet.binance.vision";
        private @NotBlank String testnetSpotWsEndpoint = "wss://testnet.binance.vision";
        private @NotBlank String testnetDerivativesEndpoint = "https://testnet.binancefuture.com";
        // TODO - It's should be stored in the DB with AES512 encryption in the futures finally.
        private String apiKey;
        private String secretKey;
    }
}
