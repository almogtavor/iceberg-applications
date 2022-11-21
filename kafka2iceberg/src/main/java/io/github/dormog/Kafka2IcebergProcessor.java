package io.github.dormog;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.dormog.configuration.S3Configuration;
import io.github.dormog.configuration.properties.S3Properties;
import io.github.dormog.service.IcebergWriter;
import io.github.dormog.configuration.properties.KafkaConsumerProperties;
import io.github.dormog.model.SamplePojo;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

@Component
@AllArgsConstructor
@Slf4j
public class Kafka2IcebergProcessor implements ApplicationRunner {
    private final SparkSession spark;
    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final IcebergWriter icebergWriter;
    private final S3Configuration s3Configuration;
    private final S3Properties s3Properties;
    @Qualifier("schemaPojo") private final Class<?> dataRepresentationPojo;

    @Override
    public void run(ApplicationArguments args) {
        log.info("Starting Kafka2Iceberg processing");
        s3Configuration.createBucketIfNotExists(s3Properties.getBucket());
        var schema = ExpressionEncoder.javaBean(dataRepresentationPojo).schema();
        log.debug("The schema is: " + schema.json());
        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", String.join(",", kafkaConsumerProperties.getBootstrapServers()))
                .option("subscribe", kafkaConsumerProperties.getTopic())
                .option("startingOffsets", kafkaConsumerProperties.getConsumer().getAutoOffsetReset())
                .load();
        df = df
                .select(Arrays.stream(df.columns()).map(c -> col(c).cast(DataTypes.StringType).alias(c)).toArray(Column[]::new))
                .select(from_json(col("value"), schema).alias("values"), col("key"), col("timestamp"));
        df = df.select("values.*", "key", "timestamp");
        df = df.withColumnRenamed("createdDate", "createdDateTemp");
        df = df.withColumn("createdDate", to_timestamp(
                concat_ws("/", col("createdDateTemp.year"), col("createdDateTemp.month"), col("createdDateTemp.date"), col("createdDateTemp.hours"), col("createdDateTemp.minutes"), col("createdDateTemp.seconds")),
                "yyyy/MM/dd/HH/mm/SSS"));
        df = df.drop("createdDateTemp");
        icebergWriter.writeDataframe(df, spark);
    }
}
