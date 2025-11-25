package com.wl4g.signaltrading.poc.strategy;

import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.StrategyInfo.MJSMAStrategySpec;
import com.wl4g.signaltrading.poc.strategy.mj.MJSMAStrategyHandler;
import com.wl4g.signaltrading.poc.trading.IExchangeClient.ExchangeProvider;
import com.wl4g.signaltrading.poc.trading.types.TradeSignal;
import com.wl4g.signaltrading.poc.trading.TradingEngine;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

public class MJSMAStrategyHandlerTest {
    private TradingEngine tradingEngine;
    private ExchangeProvider exchangeProvider;
    private MJSMAStrategySpec strategyInfo;
    private MJSMAStrategyHandler handler;

    @Before
    public void setUp() {
        tradingEngine = Mockito.mock(TradingEngine.class);
        exchangeProvider = Mockito.mock(ExchangeProvider.class);
        strategyInfo = Mockito.mock(MJSMAStrategySpec.class);
        handler = new MJSMAStrategyHandler(Mockito.mock(SignalTradingProperties.class),
                tradingEngine, exchangeProvider, strategyInfo);
    }

    @Test
    public void makeSignal_ReturnsNull_WhenCurrentPriceIsNull() {
        Mockito.when(tradingEngine.getCurrentPrice(any(), anyString())).thenReturn(null);
        Mockito.when(strategyInfo.getSymbol()).thenReturn("BTCUSDT");
        assertNull(handler.makeSignal("BTCUSDT"));
    }

    @Test
    public void makeSignal_ReturnsNull_WhenMAFilterEnabledAndMAIsNull() {
        Mockito.when(tradingEngine.getCurrentPrice(any(), anyString())).thenReturn(100.0);
        Mockito.when(strategyInfo.getSymbol()).thenReturn("BTCUSDT");
        Mockito.when(strategyInfo.getUseMaFilter()).thenReturn(true);
        Mockito.when(handler.calculateMA(any(), anyString(), anyInt(), anyString())).thenReturn(null);
        assertNull(handler.makeSignal("BTCUSDT"));
    }

    @Test
    public void makeSignal_ReturnsSignal_WhenMAFilterDisabled() {
        Mockito.when(tradingEngine.getCurrentPrice(any(), anyString())).thenReturn(100.0);
        Mockito.when(strategyInfo.getSymbol()).thenReturn("BTCUSDT");
        Mockito.when(strategyInfo.getUseMaFilter()).thenReturn(false);
        Mockito.when(strategyInfo.getStopLossPercent()).thenReturn(0.01);
        Mockito.when(strategyInfo.getRiskRewardRatio()).thenReturn(2.0);
        Mockito.when(strategyInfo.getQuantity()).thenReturn(1.0);
        TradeSignal signal = handler.makeSignal("BTCUSDT");
        assertNotNull(signal);
        assertEquals("BTCUSDT", signal.getOpenPos().getSymbol());
        assertEquals(1.0, signal.getOpenPos().getQuantity(), 0.0001);
    }

    @Test
    public void calculateMA_ReturnsNull_WhenKlinesIsNullOrInsufficient() {
        Mockito.when(tradingEngine.getKlines(any(), anyString(), anyString(), anyInt())).thenReturn(null);
        assertNull(handler.calculateMA(exchangeProvider, "BTCUSDT", 5, "1m"));
        Mockito.when(tradingEngine.getKlines(any(), anyString(), anyString(), anyInt())).thenReturn(Collections.emptyList());
        assertNull(handler.calculateMA(exchangeProvider, "BTCUSDT", 5, "1m"));
    }

    @Test
    public void calculateMA_ReturnsAverageClosePrice() {
        List<Object> k1 = Arrays.asList(0, 0, 0, 0, "10");
        List<Object> k2 = Arrays.asList(0, 0, 0, 0, "20");
        List<Object> k3 = Arrays.asList(0, 0, 0, 0, "30");
        List<Object> k4 = Arrays.asList(0, 0, 0, 0, "40");
        List<Object> k5 = Arrays.asList(0, 0, 0, 0, "50");
        List<List<Object>> klines = Arrays.asList(k1, k2, k3, k4, k5, k5);
        Mockito.when(tradingEngine.getKlines(any(), anyString(), anyString(), anyInt())).thenReturn(klines);
        Double ma = handler.calculateMA(exchangeProvider, "BTCUSDT", 5, "1m");
        assertEquals(38.0, ma, 0.0001);
    }

    @Test
    public void isPriceAboveMA_ReturnsTrue_WhenPriceAbove() {
        assertTrue(handler.isPriceAboveMA(110.0, 100.0));
    }

    @Test
    public void isPriceAboveMA_ReturnsFalse_WhenPriceOrMAIsNull() {
        assertFalse(handler.isPriceAboveMA(null, 100.0));
        assertFalse(handler.isPriceAboveMA(100.0, null));
    }

    @Test
    public void isPriceBelowMA_ReturnsTrue_WhenPriceBelow() {
        assertTrue(handler.isPriceBelowMA(90.0, 100.0));
    }

    @Test
    public void isPriceBelowMA_ReturnsFalse_WhenPriceOrMAIsNull() {
        assertFalse(handler.isPriceBelowMA(null, 100.0));
        assertFalse(handler.isPriceBelowMA(100.0, null));
    }
}

