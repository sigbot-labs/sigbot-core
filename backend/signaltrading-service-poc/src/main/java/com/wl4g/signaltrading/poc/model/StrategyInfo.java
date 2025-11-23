package com.wl4g.signaltrading.poc.model;

import com.wl4g.infra.common.bean.BaseBean;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The {@link StrategyInfo}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StrategyInfo extends BaseBean {
    private @NotBlank String name;
    private @NotBlank String provider;
    private String description;
    private @NotNull Map<String, Object> parameters;

    public StrategyInfo validate() {
        if (isNull(getProvider())) {
            throw new IllegalArgumentException("Strategy provider is required");
        }
        if (isBlank(getName())) {
            throw new IllegalArgumentException("Strategy name is required");
        }
        if (isNull(getParameters()) || getParameters().isEmpty()) {
            throw new IllegalArgumentException("Strategy configuration is required");
        }
        return this;
    }

}
