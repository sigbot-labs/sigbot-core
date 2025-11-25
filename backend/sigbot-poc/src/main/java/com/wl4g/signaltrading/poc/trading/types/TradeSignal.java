package com.wl4g.signaltrading.poc.trading.types;

import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.validation.annotation.Validated;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Trading signal.
 */
@Validated
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class TradeSignal {
    // Trading entry position.
    @Valid
    private @NotNull OpenPosition openPos;

    // Trading stop loss.
    @Valid
    private @Nullable StopPosition stopLoss;

    // Trading stop take profit.
    @Valid
    private @Nullable StopPosition stopProfit;

    // Trading signal description.
    private @Nullable String description;

    @SuppressWarnings("unused")
    public void validate() {
        if (isNull(openPos)) {
            throw new IllegalArgumentException("Open position cannot be null");
        }
        openPos.validate();
        if (nonNull(stopLoss)) {
            stopLoss.validate();
        }
        if (nonNull(stopProfit)) {
            stopProfit.validate();
        }
    }

    @Data
    @SuperBuilder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OpenPosition {
        // Trading signal referenced k-line price time.
        private @NotNull Long refTime;
        private @NotBlank String symbol;
        private @NotNull Direction side;
        @Builder.Default
        private @NotNull OrderType type = OrderType.MARKET;
        private @Nullable Double price;
        private @Min(0) double quantity;
        private boolean makerOnly;

        public void validate() {
            if (isNull(refTime)) {
                throw new IllegalArgumentException("Open position order must have referenced time");
            }
            if (isBlank(symbol)) {
                throw new IllegalArgumentException("Open position order must have symbol");
            }
            if (isNull(side)) {
                throw new IllegalArgumentException("Open position order must have side");
            }
            if (quantity <= 0) {
                throw new IllegalArgumentException("Open position order must be quantity > 0");
            }
            if (type == OrderType.MARKET) {
                if (nonNull(price)) {
                    throw new IllegalArgumentException("Market order cannot have price");
                }
            } else if (type == OrderType.LIMITED) {
                if (isNull(price)) {
                    throw new IllegalArgumentException("Limited order must have price");
                }
                if (price <= 0) {
                    throw new IllegalArgumentException("Limited order must be price > 0");
                }
            }
        }
    }

    @Data
    @SuperBuilder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StopPosition {
        // Trading signal referenced k-line price time.
        private @NotNull Long refTime;
        private @NotBlank String symbol;
        private @NotNull Direction side;
        @Builder.Default
        private @NotNull OrderType type = OrderType.MARKET;
        private @Nullable Double price;
        private @Min(0) float quantityPercent;

        public void validate() {
            if (isNull(refTime)) {
                throw new IllegalArgumentException("Stop position order must have referenced time");
            }
            if (isBlank(symbol)) {
                throw new IllegalArgumentException("Stop position order must have symbol");
            }
            if (isNull(side)) {
                throw new IllegalArgumentException("Stop position order must have side");
            }
            if (quantityPercent <= 0) {
                throw new IllegalArgumentException("Stop position order must be quantityPercent > 0");
            }
            if (isNull(type)) {
                throw new IllegalArgumentException("Stop position order must have type");
            } else if (type == OrderType.MARKET) {
                if (nonNull(price)) {
                    throw new IllegalArgumentException("Market order cannot have price");
                }
            } else if (type == OrderType.LIMITED) {
                if (isNull(price)) {
                    throw new IllegalArgumentException("Limited order must have price");
                }
                if (price <= 0) {
                    throw new IllegalArgumentException("Limited order must be price > 0");
                }
            }
        }
    }

    public enum Direction {
        BUY, SELL;

        public Direction opposite() {
            return this == BUY ? SELL : BUY;
        }
    }

    public enum OrderType {
        MARKET, LIMITED
    }

}
