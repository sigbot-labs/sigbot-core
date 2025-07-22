package com.wl4g.signaltrading.poc.config;

import com.binance.connector.client.common.configuration.SignatureConfiguration;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.DerivativesTradingUsdsFuturesRestApiUtil;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.api.DerivativesTradingUsdsFuturesRestApi;
import com.wl4g.signaltrading.poc.model.TradeStrategy;
import com.wl4g.signaltrading.poc.service.TradeStatisticsService;
import com.wl4g.signaltrading.poc.service.strategy.DefaultStrategyService;
import com.wl4g.signaltrading.poc.service.strategy.IStrategyService;
import com.wl4g.signaltrading.poc.service.strategy.PersistStrategyService;
import com.wl4g.signaltrading.poc.service.trading.IExchangeService;
import com.wl4g.signaltrading.poc.service.trading.TradingService;
import com.wl4g.signaltrading.poc.strategy.StrategyEngine;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toMap;

/**
 * The {@link SignalTradingConfiguration}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Configuration
public class SignalTradingConfiguration {

    // CONFIG

    @Bean
    @ConfigurationProperties(prefix = "signal-trading")
    public SignalTradingProperties signalTradingProperties() {
        return new SignalTradingProperties();
    }

    @Bean
    public TradingService tradingService(SignalTradingProperties config,
                                         TradeStatisticsService statisticsService,
                                         List<IExchangeService> exchanges) {
        return new TradingService(config, exchanges.stream()
                .collect(toMap(IExchangeService::getProvider, e -> e)),
                statisticsService);
    }

    // BINANCE

    @Bean
    public DerivativesTradingUsdsFuturesRestApi derivativesTradingUsdsFuturesRestApi(SignalTradingProperties config) {
        final var clientConfig = DerivativesTradingUsdsFuturesRestApiUtil.getClientConfiguration();
        clientConfig.setUrl(config.getExchange().getBinance().getMainnetDerivativesEndpoint());

        final var signatureConfig = new SignatureConfiguration();
        signatureConfig.setApiKey(config.getExchange().getBinance().getApiKey());
        signatureConfig.setPrivateKey(config.getExchange().getBinance().getSecretKey());
        clientConfig.setSignatureConfiguration(signatureConfig);

        return new DerivativesTradingUsdsFuturesRestApi(clientConfig);
    }

    // STRATEGIES

    @Bean
    @Primary
    public IStrategyService defaultStrategyService(SignalTradingProperties config) {
        return new DefaultStrategyService(config);
    }

    @Bean
    public IStrategyService persistStrategyService(SignalTradingProperties config) {
        return new PersistStrategyService(config);
    }

    @Bean
    public StrategyEngine strategyEngine(SignalTradingProperties config,
                                         TradingService tradingService,
                                         IStrategyService strategyService) {
        return new StrategyEngine(config, tradingService, strategyService);
    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    public static class SignalTradingProperties {
        private ExchangeProperties exchange = new ExchangeProperties();
        private List<TradeStrategy> strategies = Collections.emptyList();
    }

}