package com.wl4g.signaltrading.poc.controller;

import com.wl4g.signaltrading.poc.service.TradeStatisticsService;
import com.wl4g.signaltrading.poc.service.trading.IExchangeService;
import com.wl4g.signaltrading.poc.service.trading.TradeResult;
import com.wl4g.signaltrading.poc.service.trading.TradeSignal;
import com.wl4g.signaltrading.poc.service.trading.TradingService;
import com.wl4g.signaltrading.poc.strategy.StrategyEngine;
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
    private final TradingService tradingService;
    private final TradeStatisticsService statisticsService;
    private final StrategyEngine strategyEngine;

    /**
     * 生成并执行交易信号
     * 策略：闭着眼睛开单，盈亏比2:1，均线过滤方向
     *
     * @param symbol 交易对（可选，默认使用配置的交易对）
     * @return 交易结果
     */
    @PostMapping("/execute")
    public ResponseEntity<Map<String, Object>> executeTrade(@RequestParam String strategyId,
                                                            @RequestParam String symbol,
                                                            @RequestParam IExchangeService.ExchangeProvider exchange) {
        try {
            // 生成交易信号
            TradeSignal signal = strategyEngine.get(strategyId, exchange).makeSignal(symbol);

            if (signal == null) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "无法生成交易信号");
                return ResponseEntity.ok(response);
            }

            // 执行交易
            TradeResult result = tradingService.executeTrade(signal);

            Map<String, Object> response = new HashMap<>();
            response.put("success", result.isSuccess());
            response.put("message", result.getMessage());
            response.put("orderId", result.getOrderId());
            response.put("signal", signal);
            response.put("statistics", statisticsService.getStatistics(signal.getOpenPosition().getSymbol()));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("执行交易失败", e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "执行交易失败: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }

    /**
     * 仅生成交易信号（不执行）
     */
    @GetMapping("/signal")
    public ResponseEntity<TradeSignal> generateSignal(@RequestParam String strategyId,
                                                      @RequestParam String symbol,
                                                      @RequestParam IExchangeService.ExchangeProvider exchange) {
        TradeSignal signal = strategyEngine.get(strategyId, exchange).makeSignal(symbol);
        return ResponseEntity.ok(signal);
    }

    /**
     * 执行指定的交易信号
     */
    @PostMapping("/execute-signal")
    public ResponseEntity<Map<String, Object>> executeSignal(@RequestBody TradeSignal signal) {
        try {
            TradeResult result = tradingService.executeTrade(signal);

            Map<String, Object> response = new HashMap<>();
            response.put("success", result.isSuccess());
            response.put("message", result.getMessage());
            response.put("orderId", result.getOrderId());
            response.put("signal", signal);
            response.put("statistics", statisticsService.getStatistics(signal.getOpenPosition().getSymbol()));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("执行交易信号失败", e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "执行交易信号失败: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }

    // Obtain the statistics for performance.
    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getStatistics(
            @RequestParam(required = false) String symbol) {

        Map<String, Object> response = new HashMap<>();

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
        Map<String, Object> response = new HashMap<>();
        response.put("records", statisticsService.getTradeRecords());
        response.put("total", statisticsService.getTradeRecords().size());
        return ResponseEntity.ok(response);
    }
}

