package com.wl4g.signaltrading.poc.controller;

import com.wl4g.signaltrading.poc.strategy.StrategyEngine;
import com.wl4g.signaltrading.poc.strategy.TradeStatisticsService;
import com.wl4g.signaltrading.poc.trading.TradingEngine;
import com.wl4g.signaltrading.poc.trading.types.TradeSignal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Unified Trading Controller
 *
 * @author James Wong
 */
@Slf4j
@RestController
@RequestMapping("/api/trading")
@RequiredArgsConstructor
public class TradingController {
    private final TradingEngine tradingEngine;
    private final TradeStatisticsService statisticsService;
    private final StrategyEngine strategyEngine;

    // Only generate trade signal (without execution)
    @GetMapping("/signal/generate")
    public ResponseEntity<TradeSignal> generateSignal(@RequestParam Long exchangeId,
                                                      @RequestParam Long strategyId,
                                                      @RequestParam String symbol) {
        final var signal = strategyEngine.get(strategyId).makeSignal(exchangeId, symbol);
        return ResponseEntity.ok(signal);
    }

    // Execute the specified trade signal
    @PostMapping("/execute/signal")
    public ResponseEntity<Map<String, Object>> executeSignal(@RequestParam Long exchangeId,
                                                             @RequestBody TradeSignal signal) {
        try {
            final var result = tradingEngine.executeTrade(exchangeId, signal);

            Map<String, Object> response = new HashMap<>();
            response.put("success", result.isSuccess());
            response.put("message", result.getMessage());
            response.put("orderId", result.getOrderId());
            response.put("signal", signal);
            response.put("statistics", statisticsService.getStatistics(signal.getOpenPos().getSymbol()));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("执行交易信号失败", e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "执行交易信号失败: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }

    /**
     * 生成并执行交易信号
     * 策略：闭着眼睛开单，盈亏比2:1，均线过滤方向
     *
     * @param symbol 交易对（可选，默认使用配置的交易对）
     * @return 交易结果
     */
    @PostMapping("/execute/strategy")
    public ResponseEntity<Map<String, Object>> executeTrade(@RequestParam Long exchangeId,
                                                            @RequestParam Long strategyId,
                                                            @RequestParam String symbol) {
        try {
            // 生成交易信号
            final var signal = strategyEngine.get(strategyId).makeSignal(exchangeId, symbol);
            if (signal == null) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "无法生成交易信号");
                return ResponseEntity.ok(response);
            }

            // 执行交易
            final var result = tradingEngine.executeTrade(exchangeId, signal);

            Map<String, Object> response = new HashMap<>();
            response.put("success", result.isSuccess());
            response.put("message", result.getMessage());
            response.put("orderId", result.getOrderId());
            response.put("signal", signal);
            response.put("statistics", statisticsService.getStatistics(signal.getOpenPos().getSymbol()));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("执行交易失败", e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "执行交易失败: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }


    // Obtain the statistics for performance.
    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getStatistics(
            @RequestParam(required = false) String symbol) {

        final var response = new HashMap<String, Object>();
        if (symbol != null) {
            response.put("statistics", statisticsService.getStatistics(symbol));
        } else {
            response.put("overallWinRate", statisticsService.getOverallWinRate());
            response.put("message", "请指定symbol参数获取详细统计");
        }

        return ResponseEntity.ok(response);
    }

    // Obtain the all trade records for analysis.
    @GetMapping("/records")
    public ResponseEntity<Map<String, Object>> getTradeRecords() {
        final var response = new HashMap<String, Object>();
        response.put("records", statisticsService.getTradeRecords());
        response.put("total", statisticsService.getTradeRecords().size());
        return ResponseEntity.ok(response);
    }
}

