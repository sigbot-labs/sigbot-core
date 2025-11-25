package com.wl4g.signaltrading.poc.model;

import com.wl4g.infra.common.bean.BaseBean;
import com.wl4g.signaltrading.poc.repository.converter.PropertiesConverter;
import com.wl4g.signaltrading.poc.trading.IExchangeClient.ExchangeProvider;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.ColumnTransformer;

import java.util.Properties;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The {@link ExchangeInfo}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Entity
@Table(name = "exchange_info")
@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ExchangeInfo extends BaseBean {
    @Override
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long getId() {
        return super.getId();
    }

    @Override
    public void setId(Long id) {
        super.setId(id);
    }

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private @NotNull ExchangeProvider provider;

    @Column(nullable = false, length = 128)
    private @NotBlank String name;

    @Column(length = 512)
    private String description;

    @ColumnTransformer(write = "?::jsonb")
    @Convert(converter = PropertiesConverter.class)
    @Column(name = "configuration", columnDefinition = "jsonb", nullable = false)
    private @NotNull Properties configuration;

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
