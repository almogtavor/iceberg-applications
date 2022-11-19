package io.github.dormog.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@ConfigurationProperties("spring.iceberg")
@Configuration
public class IcebergProperties {
    private String tableName;
    private String databaseName;
    /**
     * The catalog type. The options are hive, nessie or hadoop.
     * The Hive catalog is implemented with JDBC and the Hadoop catalog with S3.
     */
    private String catalogType;
}
