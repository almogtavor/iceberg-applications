package io.github.dormog.configuration;

import io.github.dormog.configuration.properties.HiveMetastoreJDBCProperties;
import io.github.dormog.configuration.properties.S3Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
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

/**
 * A Spark configuration for creating a SparkSession based on a Hive Metastore catalog.
 * This catalog can be implemented using any JDBC driver.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(value = "spring.iceberg.catalog-type", havingValue = "hive")
public class SparkHiveMetastoreCatalogConfiguration {
    private final Environment environment;
    private final Minio minio;
    private String s3Endpoint;
    private final S3Properties s3Properties;
    private final HiveMetastoreJDBCProperties hiveMetastoreJDBCProperties;
    private final S3Configuration s3Configuration;

    @Bean
    public SparkSession createSparkSession() {
        String warehousePath = "s3a://" + s3Properties.getBucket() + "/my-iceberg-warehouse";
        boolean isProfileDev = Arrays.asList(environment.getActiveProfiles()).contains("dev");
        if (isProfileDev) {
            minio.start();
        }
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
                // enable iceberg SQL Extensions
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                // catalog
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
//                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.spark_catalog.warehouse", s3Configuration.getWarehousePath())
                .set("spark.sql.catalog.spark_catalog.catalog-impl", JdbcCatalog.class.getName())
                .set("spark.sql.catalog.spark_catalog.uri", hiveMetastoreJDBCProperties.getUri())
                .set("spark.sql.catalog.spark_catalog.jdbc.verifyServerCertificate", "true")
                .set("spark.sql.catalog.spark_catalog.jdbc.useSSL", "true")
                .set("spark.sql.catalog.spark_catalog.jdbc.user", hiveMetastoreJDBCProperties.getUsername())
                .set("spark.sql.catalog.spark_catalog.jdbc.password", hiveMetastoreJDBCProperties.getPassword());
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
