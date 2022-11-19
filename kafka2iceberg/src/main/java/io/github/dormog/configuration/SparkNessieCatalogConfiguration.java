package io.github.dormog.configuration;

import io.github.dormog.configuration.properties.NessieProperties;
import io.github.dormog.configuration.properties.S3Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(value = "spring.iceberg.catalog-type", havingValue = "nessie")
public class SparkNessieCatalogConfiguration {
    private final Environment environment;
    private static final String WAREHOUSE_PATH = "s3a://" + Minio.TEST_BUCKET + "/my-iceberg-nessie-warehouse";
    private final S3Properties s3Properties;
    private final NessieProperties nessieProperties;
    private final S3Configuration s3Configuration;
    private final Minio minio;

    @Bean
    public SparkSession createSparkSession() {
        boolean isProfileDev = Arrays.asList(environment.getActiveProfiles()).contains("dev");
        if (isProfileDev) {
            minio.start();
        }
        Integer mappedPort = isProfileDev ? minio.getMappedPort() : s3Properties.getPort();
        String s3Endpoint = s3Properties.getUrl() + ":" + mappedPort;
        SparkConf sparkconf = new SparkConf();
        sparkconf.setAppName("Kafka2Iceberg")
                .setMaster("local[2]")
                .set("spark.ui.enabled", "true")
                .set("spark.eventLog.enabled", "false")
                // minio specific setting using minio as S3
                .set("spark.hadoop.fs.s3a.access.key", s3Properties.getAccessKey())
                .set("spark.hadoop.fs.s3a.secret.key", s3Properties.getSecretKey())
                .set("spark.hadoop.fs.s3a.endpoint", s3Configuration.getEndpoint())
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.sql.warehouse.dir", s3Configuration.getWarehousePath())
                // catalog
                .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0,org.projectnessie:nessie-spark-extensions-3.3_2.12:0.44.0")
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
                .set("spark.sql.catalog.spark_catalog.uri", nessieProperties.getUrl())
                .set("spark.sql.catalog.spark_catalog.ref", nessieProperties.getGitRef())
                .set("spark.sql.catalog.spark_catalog.authentication.type", nessieProperties.getAuthType())
                .set("spark.sql.catalog.spark_catalog.catalog-impl", NessieCatalog.class.getName())
                .set("spark.sql.catalog.spark_catalog.warehouse", s3Configuration.getWarehousePath())
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");

        var spark = SparkSession
                .builder()
                .config(sparkconf)
                .getOrCreate();
        log.warn("Spark Version:{}", spark.version());
        return spark;
    }

    @PreDestroy
    public void closeMinio() {
        if (minio != null) {
            minio.stop();
        }
    }
}
