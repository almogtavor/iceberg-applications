package io.github.dormog.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@ConfigurationProperties("spring.iceberg-maintainer")
@Configuration
public class IcebergMaintainerProperties {
    private ActiveTasks activeTasks;
}
