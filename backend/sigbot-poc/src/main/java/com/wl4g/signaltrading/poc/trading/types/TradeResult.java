package com.wl4g.signaltrading.poc.trading.types;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Trading Result.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeResult {
    // Trading executed result.
    private boolean success;
    // Trading executed order ID.
    private Long orderId;
    // Trading executed message.
    private String message;
    // Trading signal.
    private TradeSignal signal;
}
