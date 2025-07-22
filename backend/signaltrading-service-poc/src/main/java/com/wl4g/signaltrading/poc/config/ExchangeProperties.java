package com.wl4g.signaltrading.poc.config;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class ExchangeProperties {
    private BinanceProperties binance = new BinanceProperties();
    private OkxProperties okx = new OkxProperties();

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

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    @Validated
    public static class OkxProperties {
        // TODO - It's should be stored in the DB with AES512 encryption in the futures finally.
        private String apiKey;
        private String secretKey;
        private String passphrase;
        // base uri
    }

}