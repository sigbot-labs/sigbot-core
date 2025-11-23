package com.wl4g.signaltrading.poc.trading.types;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Basic K-line (candlestick) info model.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class KlineResult {
    // open time in milliseconds
    private long openTime;
    private double openPrice;
    private double highPrice;
    private double lowPrice;
    private double closePrice;
    // volume
    private double volume;
    // close time in milliseconds (optional)
    private Long closeTime;
}

