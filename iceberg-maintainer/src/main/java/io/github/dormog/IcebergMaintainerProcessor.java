package io.github.dormog;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.dormog.configuration.properties.KafkaConsumerProperties;
import io.github.dormog.model.SamplePojo;
import io.github.dormog.service.IcebergCompactor;
import io.github.dormog.service.IcebergSnapshotsExpirer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

@Component
@AllArgsConstructor
@Slf4j
public class IcebergMaintainerProcessor implements ApplicationRunner {
    private final SparkSession spark;
    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final ObjectMapper objectMapper;
    private final IcebergCompactor icebergCompactor;
    private final IcebergSnapshotsExpirer icebergSnapshotsExpirer;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("GOT HERE SparkStreamingProcessor");

        var schema = ExpressionEncoder.javaBean(SamplePojo.class).schema();
        System.out.println(schema.json());
        icebergCompactor.execute(spark);
        icebergSnapshotsExpirer.execute(spark);
//        System.exit(0);
//        df = df.withColumnRenamed("createdDate", "createdDateTemp");
//        df = df.withColumn("createdDate", to_timestamp(
//                concat_ws("/", col("createdDateTemp.year"), col("createdDateTemp.month"), col("createdDateTemp.date"), col("createdDateTemp.hours"), col("createdDateTemp.minutes"), col("createdDateTemp.seconds")),
//                "yyyy/MM/dd/HH/mm/SSS"));
//        df = df.drop("createdDateTemp");
//        df.printSchema();
////        df
////                .writeStream()
////                .format("console")
////                .option("truncate","false")
////                .start();
//        df.writeTo("local.db.table").append();
    }
}
