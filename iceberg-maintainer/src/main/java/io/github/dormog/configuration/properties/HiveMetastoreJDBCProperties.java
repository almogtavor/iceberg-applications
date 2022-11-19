package io.github.dormog.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@ConfigurationProperties("spring.jdbc")
@Configuration
public class HiveMetastoreJDBCProperties {
    private String uri;
    private String username;
    private String password;
}
