package io.github.dormog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = "io.github.dormog")
@ConfigurationPropertiesScan
public class IcebergMaintainerApplication {
    public static void main(String[] args) {
        SpringApplication.run(IcebergMaintainerApplication.class, args);
    }
}
