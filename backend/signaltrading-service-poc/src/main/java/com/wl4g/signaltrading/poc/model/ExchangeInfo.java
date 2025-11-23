package com.wl4g.signaltrading.poc.model;

import com.wl4g.infra.common.bean.BaseBean;
import com.wl4g.signaltrading.poc.trading.IExchangeClient.ExchangeProvider;
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
 * The {@link ExchangeInfo}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ExchangeInfo extends BaseBean {
    private @NotNull ExchangeProvider provider;
    private @NotBlank String name;
    private String description;
    private @NotNull Map<String, Object> configuration;

    public ExchangeInfo validate() {
        if (isNull(getProvider())) {
            throw new IllegalArgumentException("Exchange provider is required");
        }
        if (isBlank(getName())) {
            throw new IllegalArgumentException("Exchange name is required");
        }
        if (isNull(getConfiguration()) || getConfiguration().isEmpty()) {
            throw new IllegalArgumentException("Exchange configuration is required");
        }
        return this;
    }
}
