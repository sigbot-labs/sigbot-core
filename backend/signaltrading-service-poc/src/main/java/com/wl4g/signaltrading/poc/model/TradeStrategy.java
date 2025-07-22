package com.wl4g.signaltrading.poc.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wl4g.infra.common.bean.BaseBean;
import com.wl4g.infra.common.lang.tuples.Tuple4;
import com.wl4g.signaltrading.poc.strategy.IStrategyHandler;
import com.wl4g.signaltrading.poc.strategy.MJSMAStrategyHandler;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.function.Function;

import static com.wl4g.infra.common.lang.Assert2.hasTextOf;
import static java.lang.String.format;

/**
 * The {@link TradeStrategy}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TradeStrategy extends BaseBean {
    private @NotBlank String name;
    private String description;
    private @NotNull StrategyInfo strategy;

    //@Schema(oneOf = {MJSMAStrategyInfo.class}, discriminatorProperty = "type")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = true)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = MJSMAStrategyInfo.class, name = "MJSMA"),
    })
    @ToString(callSuper = true)
    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static abstract class StrategyInfo {
        //@Schema(name = "type", implementation = StrategyType.class)
        @JsonProperty(value = "type", access = JsonProperty.Access.WRITE_ONLY)
        @NotNull
        private transient TradeStrategy.StrategyProvider type;
        private String id;
        private String description;
    }

    @Getter
    @AllArgsConstructor
    public enum StrategyProvider {
        MJSMA(p -> new MJSMAStrategyHandler(p.getItem1(), p.getItem2(), p.getItem3(), p.getItem4()));
        private final Function<Tuple4, IStrategyHandler> initializer;

        public static StrategyProvider of(final @NotBlank String type) {
            hasTextOf(type, "type");
            for (StrategyProvider a : values()) {
                if (a.name().equalsIgnoreCase(type)) {
                    return a;
                }
            }
            throw new IllegalArgumentException(format("Invalid strategy type for '%s'", type));
        }
    }

    @Data
    @NoArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class MJSMAStrategyInfo extends StrategyInfo {
        private String symbol;
        private Double quantity;
        private Double riskRewardRatio;
        private Integer maPeriods;
        private String maInterval;
        private Boolean useMaFilter;
        private Double stopLossPercent;
    }
}
