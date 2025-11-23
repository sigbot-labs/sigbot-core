package com.wl4g.signaltrading.poc.config;

import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import com.wl4g.signaltrading.poc.model.StrategyInfo;
import com.wl4g.signaltrading.poc.service.exchange.DefaultExchangeService;
import com.wl4g.signaltrading.poc.service.exchange.IExchangeService;
import com.wl4g.signaltrading.poc.service.exchange.StandardExchangeService;
import com.wl4g.signaltrading.poc.service.strategy.DefaultStrategyService;
import com.wl4g.signaltrading.poc.service.strategy.IStrategyService;
import com.wl4g.signaltrading.poc.service.strategy.StandardStrategyService;
import com.wl4g.signaltrading.poc.strategy.IStrategyHandler;
import com.wl4g.signaltrading.poc.strategy.IStrategyHandler.IStrategyFactory;
import com.wl4g.signaltrading.poc.strategy.StrategyEngine;
import com.wl4g.signaltrading.poc.strategy.TradeStatisticsService;
import com.wl4g.signaltrading.poc.strategy.mj.MJSMAStrategyHandler;
import com.wl4g.signaltrading.poc.strategy.mj.MJSMAStrategyHandler.MJSMAStrategyFactory;
import com.wl4g.signaltrading.poc.trading.IExchangeClient;
import com.wl4g.signaltrading.poc.trading.IExchangeClient.IExchangeFactory;
import com.wl4g.signaltrading.poc.trading.TradingEngine;
import com.wl4g.signaltrading.poc.trading.cex.BinanceClient;
import com.wl4g.signaltrading.poc.trading.cex.BinanceClient.BinanceExchangeFactory;
import com.wl4g.signaltrading.poc.trading.cex.BitgetClient;
import com.wl4g.signaltrading.poc.trading.cex.BitgetClient.BitgetExchangeFactory;
import com.wl4g.signaltrading.poc.trading.cex.BybitClient;
import com.wl4g.signaltrading.poc.trading.cex.BybitClient.BybitExchangeFactory;
import com.wl4g.signaltrading.poc.trading.cex.CoinbaseClient;
import com.wl4g.signaltrading.poc.trading.cex.CoinbaseClient.CoinbaseExchangeFactory;
import com.wl4g.signaltrading.poc.trading.cex.OkxClient;
import com.wl4g.signaltrading.poc.trading.cex.OkxClient.OkxServiceFactory;
import com.wl4g.signaltrading.poc.trading.dex.HyperliquidClient;
import com.wl4g.signaltrading.poc.trading.dex.HyperliquidClient.HyperliquidExchangeFactory;
import com.wl4g.signaltrading.poc.trading.dex.LighterClient;
import com.wl4g.signaltrading.poc.trading.dex.LighterClient.LighterExchangeFactory;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.List;

import static java.util.Collections.emptyList;

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
    @ConfigurationProperties(prefix = "trading")
    public SignalTradingProperties signalTradingProperties() {
        return new SignalTradingProperties();
    }

    // EXCHANGES

    @Bean
    @Primary // TODO
    public IExchangeService defaultExchangeService(SignalTradingProperties config) {
        return new DefaultExchangeService(config);
    }

    @Bean
    public IExchangeService standardExchangeService(SignalTradingProperties config) {
        return new StandardExchangeService(config);
    }

    @Bean
    public TradingEngine tradingEngine(SignalTradingProperties config,
                                       List<IExchangeFactory<? extends IExchangeClient>> factories,
                                       IExchangeService exchangeService,
                                       TradeStatisticsService statisticsService) {
        return new TradingEngine(config, factories, exchangeService, statisticsService);
    }

    // CEX EXCHANGES

    @Bean
    public IExchangeFactory<BinanceClient> binanceServiceFactory() {
        return new BinanceExchangeFactory();
    }

    @Bean
    public IExchangeFactory<OkxClient> okxServiceFactory() {
        return new OkxServiceFactory();
    }

    @Bean
    public IExchangeFactory<BitgetClient> bitgetExchangeFactory() {
        return new BitgetExchangeFactory();
    }

    @Bean
    public IExchangeFactory<BybitClient> bybitExchangeFactory() {
        return new BybitExchangeFactory();
    }

    @Bean
    public IExchangeFactory<CoinbaseClient> coinbaseExchangeFactory() {
        return new CoinbaseExchangeFactory();
    }

    // DEX EXCHANGES

    @Bean
    public IExchangeFactory<HyperliquidClient> hyperliquidExchangeFactory() {
        return new HyperliquidExchangeFactory();
    }

    @Bean
    public IExchangeFactory<LighterClient> lighterExchangeFactory() {
        return new LighterExchangeFactory();
    }

    // STRATEGIES

    @Bean
    @Primary // TODO
    public IStrategyService defaultStrategyService(SignalTradingProperties config) {
        return new DefaultStrategyService(config);
    }

    @Bean
    public IStrategyService standardStrategyService(SignalTradingProperties config) {
        return new StandardStrategyService(config);
    }

    @Bean
    public StrategyEngine strategyEngine(SignalTradingProperties config,
                                         List<IStrategyFactory<? extends IStrategyHandler>> factories,
                                         TradingEngine tradingEngine,
                                         IStrategyService strategyService) {
        return new StrategyEngine(config, factories, tradingEngine, strategyService);
    }

    @Bean
    public IStrategyFactory<MJSMAStrategyHandler> mjSmaStrategyFactory() {
        return new MJSMAStrategyFactory();
    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    public static class SignalTradingProperties {
        private List<ExchangeInfo> defaultExchanges = emptyList();
        private List<StrategyInfo> defaultStrategies = emptyList();
    }


}