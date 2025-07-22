package com.wl4g.signaltrading.poc.service;

import com.wl4g.signaltrading.poc.service.trading.TradeSignal;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Traded statistics service, record trade history and win rate.
 *
 * @author James Wong
 */
@Slf4j
@Service
public class TradeStatisticsService {
    // Original trade records
    private final List<TradeRecord> tradeRecords = new ArrayList<>();

    // Statistics by trade symbol
    private final ConcurrentHashMap<String, SymbolStatistics> symbolStats = new ConcurrentHashMap<>();

    // Recording open position.
    public void recordOpen(TradeSignal signal, Long entryOrderId, Long stopLossOrderId, Long takeProfitOrderId) {
        final var record = new TradeRecord();
        record.setSignal(signal);
        record.setEntryOrderId(entryOrderId);
        record.setStopLossOrderId(stopLossOrderId);
        record.setTakeProfitOrderId(takeProfitOrderId);
        record.setTimestamp(System.currentTimeMillis());

        synchronized (tradeRecords) {
            tradeRecords.add(record);
        }

        // Update symbol statistics.
        SymbolStatistics stats = symbolStats.computeIfAbsent(signal.getOpenPosition().getSymbol(), k -> new SymbolStatistics());
        stats.totalTrades.incrementAndGet();

        log.info("Recorded - symbol={}, side={}, entryPrice={}, totalTrades={}",
                signal.getOpenPosition().getSymbol(), signal.getOpenPosition().getSide(), signal.getOpenPosition(), stats.totalTrades.get());
    }

    // Update traded result. (take profit and stop loss)
    public void updateResult(Long entryOrderId, boolean isWin) {
        synchronized (tradeRecords) {
            for (final var record : tradeRecords) {
                if (record.getEntryOrderId().equals(entryOrderId)) {
                    record.setResult(isWin ? "WIN" : "LOSS");
                    record.setResultTimestamp(System.currentTimeMillis());
                    final var stats = symbolStats.get(record.getSignal().getOpenPosition().getSymbol());
                    if (stats != null) {
                        if (isWin) {
                            stats.winningTrades.incrementAndGet();
                        } else {
                            stats.losingTrades.incrementAndGet();
                        }
                    }
                    log.info("Updated trade result - orderId={}, result={}, winRate={}",
                            entryOrderId, record.getResult(), getWinRate(record.getSignal().getOpenPosition().getSymbol()));
                    break;
                }
            }
        }
    }

    // Get symbol win rate.
    public double getWinRate(String symbol) {
        SymbolStatistics stats = symbolStats.get(symbol);
        if (stats == null) {
            return 0.0;
        }

        int total = stats.winningTrades.get() + stats.losingTrades.get();
        if (total == 0) {
            return 0.0;
        }

        return (double) stats.winningTrades.get() / total * 100.0;
    }

    // Get symbol total win rate.
    public double getOverallWinRate() {
        int totalWins = 0;
        int totalLosses = 0;

        for (SymbolStatistics stats : symbolStats.values()) {
            totalWins += stats.winningTrades.get();
            totalLosses += stats.losingTrades.get();
        }

        int total = totalWins + totalLosses;
        if (total == 0) {
            return 0.0;
        }

        return (double) totalWins / total * 100.0;
    }

    // Get symbol statistics.
    public StatisticsInfo getStatistics(String symbol) {
        SymbolStatistics stats = symbolStats.get(symbol);
        if (stats == null) {
            return new StatisticsInfo();
        }

        StatisticsInfo info = new StatisticsInfo();
        info.setSymbol(symbol);
        info.setTotalTrades(stats.totalTrades.get());
        info.setWinningTrades(stats.winningTrades.get());
        info.setLosingTrades(stats.losingTrades.get());
        info.setWinRate(getWinRate(symbol));
        info.setOverallWinRate(getOverallWinRate());

        return info;
    }

    // Obtain all trade records.
    public List<TradeRecord> getTradeRecords() {
        synchronized (tradeRecords) {
            return new ArrayList<>(tradeRecords);
        }
    }

    @Data
    public static class TradeRecord {
        private TradeSignal signal;
        private Long entryOrderId;
        private Long stopLossOrderId;
        private Long takeProfitOrderId;
        private Long timestamp;
        private String result; // WIN or LOSS
        private Long resultTimestamp;
    }

    @Data
    public static class SymbolStatistics {
        private final AtomicInteger totalTrades = new AtomicInteger(0);
        private final AtomicInteger winningTrades = new AtomicInteger(0);
        private final AtomicInteger losingTrades = new AtomicInteger(0);
    }

    @Data
    public static class StatisticsInfo {
        private String symbol;
        private int totalTrades;
        private int winningTrades;
        private int losingTrades;
        private double winRate;
        private double overallWinRate;
    }
}

