package io.github.dormog.configuration;

import io.github.dormog.configuration.properties.IcebergProperties;
import io.github.dormog.configuration.properties.S3Properties;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.net.URISyntaxException;

@Configuration
@AllArgsConstructor
public class IcebergConfiguration {
    private final IcebergProperties icebergProperties;

    public String getTableFullName() {
        return icebergProperties.getDatabaseName() + "." + icebergProperties.getTableName();
    }
}
