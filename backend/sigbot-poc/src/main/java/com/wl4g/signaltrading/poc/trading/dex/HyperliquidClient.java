package com.wl4g.signaltrading.poc.trading.dex;

import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import com.wl4g.signaltrading.poc.trading.types.KlineResult;
import com.wl4g.signaltrading.poc.trading.types.PriceResult;
import com.wl4g.signaltrading.poc.trading.types.TradeSignal;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;

import java.util.List;

/**
 * The {@link HyperliquidClient}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Slf4j
@RequiredArgsConstructor
public class HyperliquidClient extends DEXClient {

    public HyperliquidClient(HyperliquidProperties config) {
    }

    @Override
    public PriceResult getCurrentPrice(String symbol) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<KlineResult> getKlines(String symbol, String interval, Integer limit) {
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

    @RequiredArgsConstructor
    public static class HyperliquidExchangeFactory implements IExchangeFactory<HyperliquidClient> {
        @Override
        public ExchangeProvider getProvider() {
            return ExchangeProvider.HYPERLIQUID;
        }

        @Override
        public HyperliquidClient getInstance(ExchangeInfo config) {
            return new HyperliquidClient(IExchangeFactory.parse(HyperliquidProperties.class, config));
        }
    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    @Validated
    public static class HyperliquidProperties {
        //private @NotBlank String mainnetSpotApiEndpoint = "https://api.hyperliquid.com";
        //private @NotBlank String mainnetSpotWsEndpoint = "wss://fstream.hyperliquid.com";
        //private @NotBlank String mainnetDerivativesEndpoint = "https://fapi.hyperliquid.com";
        //private @NotBlank String testnetSpotApiEndpoint = "https://testnet.hyperliquid.vision";
        //private @NotBlank String testnetSpotWsEndpoint = "wss://testnet.hyperliquid.vision";
        //private @NotBlank String testnetDerivativesEndpoint = "https://testnet.hyperliquidfuture.com";
        //// TODO - It's should be stored in the DB with AES512 encryption in the futures finally.
        //private String apiKey;
        //private String secretKey;
    }

}
