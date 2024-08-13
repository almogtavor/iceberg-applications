package io.github.almogtavor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = "io.github.almogtavor")
@ConfigurationPropertiesScan
public class IcebergMaintainerApplication {
    public static void main(String[] args) {
        SpringApplication.run(IcebergMaintainerApplication.class, args);
    }
}
