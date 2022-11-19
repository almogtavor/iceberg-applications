package io.github.dormog.configuration.properties;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Primary
@Configuration
@ConfigurationProperties("spring.kafka.consumer-env")
@EqualsAndHashCode(callSuper = true)
@Data
public class KafkaConsumerProperties extends KafkaProperties {
    private String topic;
    private String tableName;
}
