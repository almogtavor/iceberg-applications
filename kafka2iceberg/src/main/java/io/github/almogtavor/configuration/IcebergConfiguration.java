package io.github.almogtavor.configuration;

import io.github.almogtavor.configuration.properties.IcebergProperties;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
public class IcebergConfiguration {
    private final IcebergProperties icebergProperties;

    public String getTableFullName() {
        return icebergProperties.getDatabaseName() + "." + icebergProperties.getTableName();
    }
}
