package com.wl4g.signaltrading.poc.strategy.mj;

import com.wl4g.signaltrading.poc.model.StrategyInfo;
import com.wl4g.signaltrading.poc.strategy.IStrategyHandler;
import com.wl4g.signaltrading.poc.trading.TradingEngine;
import com.wl4g.signaltrading.poc.trading.types.TradeSignal;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static java.lang.String.format;
import static java.util.Objects.isNull;

/**
 * Simple Moving Average Trade Strategy (@摸金探长)
 * <p>
 * 1. 盈亏比2:1（止盈是止损的2倍）
 * 2. 均线过滤：均线以下只开空，以上只开多
 * 3. 开仓和止损用市价，止盈用限价
 * </p>
 */
@Slf4j
public class MJSMAStrategyHandler implements IStrategyHandler {
    private final TradingEngine tradingEngine;
    private final MJSMAStrategyParameters parameters;

    public MJSMAStrategyHandler(TradingEngine tradingEngine, MJSMAStrategyParameters parameters) {
        this.tradingEngine = tradingEngine;
        this.parameters = parameters;
        log.info("Parameters: {}", parameters);
    }

    /**
     * 生成交易信号
     * 策略：随机开单，但使用均线过滤方向
     *
     * @param symbol 交易对
     * @return 交易信号，如果不符合条件则返回null
     */
    public TradeSignal makeSignal(@NotNull Long exchangeId, @NotNull String symbol) {
        try {
            final var result = tradingEngine.getCurrentPrice(exchangeId, parameters.getSymbol());
            final var currentPrice = result.getPrice();

            // Calculate moving average if needed
            double maValue = 0;
            if (parameters.isUseMaFilter()) {
                maValue = calculateMA(exchangeId, symbol, parameters.getMaPeriods(), parameters.getMaInterval());
            }

            // According to MA filter to determine trade side
            TradeSignal.Direction side;
            if (parameters.isUseMaFilter()) {
                // 均线以下只开空，以上只开多
                if (currentPrice > maValue) {
                    side = TradeSignal.Direction.BUY;  // 价格在均线上，开多
                } else {
                    side = TradeSignal.Direction.SELL; // 价格在均线下，开空
                }
                log.info("MA filter: price={}, ma={}, side={}", currentPrice, maValue, side);
            } else {
                // 不使用均线过滤时，随机选择方向（这里简化为随机）
                // 实际可以根据其他策略决定
                side = Math.random() > 0.5 ? TradeSignal.Direction.BUY : TradeSignal.Direction.SELL;
            }

            // 计算止损和止盈价格（盈亏比2:1）
            Double stopLossPrice;
            double takeProfitPrice;

            if (TradeSignal.Direction.BUY.equals(side)) {
                // 做多：止损在下方，止盈在上方
                stopLossPrice = currentPrice * (1 - parameters.getStopLossPercent());
                takeProfitPrice = currentPrice + (currentPrice - stopLossPrice) * parameters.getRiskRewardRatio();
            } else {
                // 做空：止损在上方，止盈在下方
                stopLossPrice = currentPrice * (1 + parameters.getStopLossPercent());
                takeProfitPrice = currentPrice - (stopLossPrice - currentPrice) * parameters.getRiskRewardRatio();
            }

            final var signal = TradeSignal.builder()
                    .openPos(TradeSignal.OpenPosition.builder()
                            .refTime(result.getCurrentTime())
                            .symbol(symbol)
                            .side(side)
                            .price(currentPrice)
                            .quantity(parameters.getQuantity())
                            .build())
                    .stopLoss(TradeSignal.StopPosition.builder()
                            .refTime(result.getCurrentTime())
                            .symbol(symbol)
                            .side(side.opposite())
                            .quantityPercent(0.5f)
                            .price(stopLossPrice)
                            .build())
                    .stopProfit(TradeSignal.StopPosition.builder()
                            .refTime(result.getCurrentTime())
                            .symbol(symbol)
                            .side(side)
                            .quantityPercent(0.5f)
                            .price(stopLossPrice)
                            .build())
                    .description("MJ-SMA Strategy Signal")
                    .build();

            log.info("Generated signal - symbol={}, side={}, entry={}, stopLoss={}, takeProfit={}, riskReward={}",
                    symbol, side, currentPrice, stopLossPrice, takeProfitPrice, parameters.getRiskRewardRatio());

            return signal;
        } catch (Exception e) {
            log.error("Failed to generate trade signal: symbol={}", symbol, e);
            return null;
        }
    }

    /**
     * 计算移动平均线
     *
     * @param symbol   交易对
     * @param periods  周期数
     * @param interval K线间隔
     * @return 均线值
     */
    public Double calculateMA(@NotNull Long exchangeId, @NotNull String symbol,
                              Integer periods, String interval) {
        try {
            // Obtain K-line data, need periods + 1 data to calculate
            final var klines = tradingEngine.getKlines(exchangeId, symbol, interval, periods + 1);

            if (isNull(klines) || klines.size() < periods) {
                log.warn("Insufficient Kline data, cannot calculate MA: symbol={}, periods={}, actualSize={}",
                        symbol, periods, klines != null ? klines.size() : 0);
                return null;
            }

            // Calculate the average of the close-prices of the last 'periods' K-lines
            double sum = 0.0;
            int count = 0;
            // K-line default format: [openTime, openPrice, highPrice, lowPrice, closePrice, ...]
            // close-price is at index: 4
            // Extract the last periods some data.
            for (int i = klines.size() - periods; i < klines.size(); i++) {
                final var kline = klines.get(i);
                sum += kline.getClosePrice();
                count++;
            }

            if (count == 0) {
                return null;
            }

            double maValue = sum / count;
            log.info("MA: symbol={}, periods={}, maValue={}", symbol, periods, maValue);
            return maValue;
        } catch (Exception e) {
            throw new IllegalStateException(format("Failed to calculate MA: symbol=%s, periods=%d", symbol, periods), e);
        }
    }

    /**
     * 判断价格是否在均线之上
     *
     * @param price   当前价格
     * @param maValue 均线值
     * @return true表示价格在均线之上
     */
    public boolean isPriceAboveMA(Double price, Double maValue) {
        if (price == null || maValue == null) {
            return false;
        }
        return price > maValue;
    }

    /**
     * 判断价格是否在均线之下
     *
     * @param price   当前价格
     * @param maValue 均线值
     * @return true表示价格在均线之下
     */
    public boolean isPriceBelowMA(Double price, Double maValue) {
        if (price == null || maValue == null) {
            return false;
        }
        return price < maValue;
    }

    public static class MJSMAStrategyFactory implements IStrategyFactory<MJSMAStrategyHandler> {
        @Override
        public String getProvider() {
            return "MJSMA";
        }

        @Override
        public MJSMAStrategyHandler getInstance(TradingEngine engine, StrategyInfo config) {
            return new MJSMAStrategyHandler(engine, IStrategyFactory.parse(MJSMAStrategyParameters.class, config));
        }
    }

    @Data
    @NoArgsConstructor
    public static class MJSMAStrategyParameters {
        private String symbol = "BTCUSDC";
        private double quantity = 0.01;
        private double riskRewardRatio = 2;
        private double stopLossPercent = 0.01;
        private boolean useMaFilter = true;
        private int maPeriods = 20;
        private String maInterval = "1m";
    }

}
