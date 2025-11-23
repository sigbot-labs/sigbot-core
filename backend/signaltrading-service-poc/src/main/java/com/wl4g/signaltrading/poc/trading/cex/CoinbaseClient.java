package com.wl4g.signaltrading.poc.trading.cex;

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
 * The {@link CoinbaseClient}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Slf4j
@RequiredArgsConstructor
public class CoinbaseClient extends CEXClient {

    public CoinbaseClient(CoinbaseProperties config) {
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
    public static class CoinbaseExchangeFactory implements IExchangeFactory<CoinbaseClient> {
        @Override
        public ExchangeProvider getProvider() {
            return ExchangeProvider.COINBASE;
        }

        @Override
        public CoinbaseClient getInstance(ExchangeInfo config) {
            return new CoinbaseClient(IExchangeFactory.parse(CoinbaseClient.CoinbaseProperties.class, config));
        }
    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    @Validated
    public static class CoinbaseProperties {
        //private @NotBlank String mainnetSpotApiEndpoint = "https://api.Coinbase.com";
        //private @NotBlank String mainnetSpotWsEndpoint = "wss://fstream.coinbase.com";
        //private @NotBlank String mainnetDerivativesEndpoint = "https://fapi.coinbase.com";
        //private @NotBlank String testnetSpotApiEndpoint = "https://testnet.coinbase.vision";
        //private @NotBlank String testnetSpotWsEndpoint = "wss://testnet.coinbase.vision";
        //private @NotBlank String testnetDerivativesEndpoint = "https://testnet.coinbasefuture.com";
        //// TODO - It's should be stored in the DB with AES512 encryption in the futures finally.
        //private String apiKey;
        //private String secretKey;
    }
}
