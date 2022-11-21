package io.github.dormog.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@ConfigurationProperties("iceberg-maintainer")
@Configuration
public class IcebergMaintainerProperties {
    private List<ActiveTask> activeTasks;
}
