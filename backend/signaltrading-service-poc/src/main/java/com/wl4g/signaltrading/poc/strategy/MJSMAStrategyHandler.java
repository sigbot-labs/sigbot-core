package com.wl4g.signaltrading.poc.strategy;

import com.wl4g.signaltrading.poc.config.SignalTradingConfiguration.SignalTradingProperties;
import com.wl4g.signaltrading.poc.model.TradeStrategy.MJSMAStrategyInfo;
import com.wl4g.signaltrading.poc.model.TradeStrategy.StrategyProvider;
import com.wl4g.signaltrading.poc.service.trading.IExchangeService.ExchangeProvider;
import com.wl4g.signaltrading.poc.service.trading.TradeSignal;
import com.wl4g.signaltrading.poc.service.trading.TradingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Simple Moving Average Trade Strategy (@摸金探长)
 * <p>
 * 1. 盈亏比2:1（止盈是止损的2倍）
 * 2. 均线过滤：均线以下只开空，以上只开多
 * 3. 开仓和止损用市价，止盈用限价
 * </p>
 */
@Slf4j
@RequiredArgsConstructor
public class MJSMAStrategyHandler implements IStrategyHandler {
    private final SignalTradingProperties config;
    private final TradingService tradingService;
    private final ExchangeProvider exchange;
    private final MJSMAStrategyInfo strategy;

    @Override
    public StrategyProvider getProvider() {
        return StrategyProvider.MJSMA;
    }

    /**
     * 生成交易信号
     * 策略：随机开单，但使用均线过滤方向
     *
     * @param symbol 交易对
     * @return 交易信号，如果不符合条件则返回null
     */
    public TradeSignal makeSignal(String symbol) {
        try {
            Double currentPrice = tradingService.getCurrentPrice(exchange, strategy.getSymbol());
            if (currentPrice == null) {
                log.error("无法获取当前价格: symbol={}", symbol);
                return null;
            }

            // 计算均线
            Double maValue = null;
            if (strategy.getUseMaFilter()) {
                maValue = calculateMA(exchange, symbol, strategy.getMaPeriods(), strategy.getMaInterval());
                if (maValue == null) {
                    log.warn("无法计算均线，跳过交易: symbol={}", symbol);
                    return null;
                }
            }

            // 根据均线决定方向
            TradeSignal.Direction side;
            if (strategy.getUseMaFilter()) {
                // 均线以下只开空，以上只开多
                if (currentPrice > maValue) {
                    side = TradeSignal.Direction.BUY;  // 价格在均线上，开多
                } else {
                    side = TradeSignal.Direction.SELL; // 价格在均线下，开空
                }
                log.info("均线过滤: price={}, ma={}, side={}", currentPrice, maValue, side);
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
                stopLossPrice = currentPrice * (1 - strategy.getStopLossPercent());
                takeProfitPrice = currentPrice + (currentPrice - stopLossPrice) * strategy.getRiskRewardRatio();
            } else {
                // 做空：止损在上方，止盈在下方
                stopLossPrice = currentPrice * (1 + strategy.getStopLossPercent());
                takeProfitPrice = currentPrice - (stopLossPrice - currentPrice) * strategy.getRiskRewardRatio();
            }

            final var signal = TradeSignal.builder()
                    .provider(exchange)
                    .openPosition(TradeSignal.OpenPosition.builder()
                            .symbol(symbol)
                            .side(side)
                            .price(currentPrice)
                            .quantity(strategy.getQuantity())
                            .build())
                    .stopLoss(TradeSignal.StopPosition.builder()
                            .symbol(symbol)
                            .quantityPercent(0.5f)
                            .price(stopLossPrice)
                            .build())
                    .stopProfit(TradeSignal.StopPosition.builder()
                            .symbol(symbol)
                            .quantityPercent(0.5f)
                            .price(stopLossPrice)
                            .build())
                    .build();

            log.info("Generated Trade signal - symbol={}, side={}, entry={}, stopLoss={}, takeProfit={}, riskReward={}",
                    symbol, side, currentPrice, stopLossPrice, takeProfitPrice, strategy.getRiskRewardRatio());

            return signal;
        } catch (Exception e) {
            log.error("生成交易信号失败: symbol={}", symbol, e);
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
    public Double calculateMA(ExchangeProvider provider,
                              String symbol,
                              Integer periods,
                              String interval) {
        try {
            // 获取K线数据，需要periods+1条数据来计算
            List<List<Object>> klines = tradingService.getKlines(provider, symbol, interval, periods + 1);

            if (klines == null || klines.size() < periods) {
                log.warn("K线数据不足，无法计算均线: symbol={}, periods={}, actualSize={}",
                        symbol, periods, klines != null ? klines.size() : 0);
                return null;
            }

            // 计算最近periods条K线的收盘价平均值
            double sum = 0.0;
            int count = 0;

            // K线数据格式: [开盘时间, 开盘价, 最高价, 最低价, 收盘价, ...]
            // 取最后periods条数据
            for (int i = klines.size() - periods; i < klines.size(); i++) {
                List<Object> kline = klines.get(i);
                if (kline != null && kline.size() > 4) {
                    // 收盘价在索引4
                    String closePriceStr = kline.get(4).toString();
                    sum += Double.parseDouble(closePriceStr);
                    count++;
                }
            }

            if (count == 0) {
                return null;
            }

            double maValue = sum / count;
            log.info("计算均线: symbol={}, periods={}, maValue={}", symbol, periods, maValue);
            return maValue;
        } catch (Exception e) {
            log.error("计算均线失败: symbol={}, periods={}", symbol, periods, e);
            return null;
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

}

