package com.wl4g.signaltrading.poc.model;

import com.wl4g.infra.common.bean.BaseBean;
import com.wl4g.signaltrading.poc.repository.converter.PropertiesConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.util.Properties;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The {@link StrategyInfo}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Entity
@Table(name = "strategy_info")
@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StrategyInfo extends BaseBean {
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

    @Column(nullable = false, length = 128)
    private @NotBlank String name;

    @Column(nullable = false, length = 64)
    private @NotBlank String provider;

    @Column(length = 512)
    private String description;

    @ColumnTransformer(write = "?::jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    @Convert(converter = PropertiesConverter.class)
    @Column(name = "parameters", columnDefinition = "jsonb", nullable = false)
    private @NotNull Properties parameters;

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
