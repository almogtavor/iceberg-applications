package io.github.dormog.configuration;

import io.github.dormog.configuration.properties.IcebergProperties;
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
