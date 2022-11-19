package io.github.dormog.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@ConfigurationProperties("spring.s3")
@Configuration
public class S3Properties {
    private String url;
    private Integer port;
    private String accessKey;
    private String secretKey;
    private String bucket;
}
