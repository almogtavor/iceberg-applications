package io.github.dormog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = "io.github.dormog")
@ConfigurationPropertiesScan
public class Kafka2IcebergApplication {
    public static void main(String[] args) {
        SpringApplication.run(Kafka2IcebergApplication.class, args);
    }
}
