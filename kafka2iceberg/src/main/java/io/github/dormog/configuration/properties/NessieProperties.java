package io.github.dormog.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@ConfigurationProperties("spring.nessie")
@Configuration
public class NessieProperties {
    private String url;
    private String gitRef;
    /**
     * Nessie's authentication type (BASIC, NONE or AWS)
     */
    private String authType;
}
