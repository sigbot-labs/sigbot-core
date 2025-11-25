package com.wl4g.signaltrading.poc.trading.types;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PriceResult {
    // current time in milliseconds
    private Long currentTime;
    private Double price;
}

