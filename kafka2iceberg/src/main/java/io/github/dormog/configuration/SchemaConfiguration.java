package io.github.dormog.configuration;

import io.github.dormog.model.SamplePojo;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchemaConfiguration {
    @Bean("schemaPojo")
    public Class<SamplePojo> defaultSchemaPojo() {
        return SamplePojo.class;
    }
}
