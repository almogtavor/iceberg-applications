package io.github.dormog.service;

import io.github.dormog.configuration.IcebergConfiguration;
import io.github.dormog.configuration.S3Configuration;
import io.github.dormog.configuration.properties.IcebergProperties;
import io.github.dormog.configuration.properties.S3Properties;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.functions.*;


@Slf4j
@Service
@RequiredArgsConstructor
public class IcebergWriter {
    private final IcebergProperties icebergProperties;
    private final IcebergConfiguration icebergConfiguration;

    public void writeDataframe(Dataset<Row> ds, SparkSession spark) {
        try {
            SparkSessionCatalog<V2SessionCatalog> sparkSessionCatalog = null;
            if (spark.sessionState().catalogManager().v2SessionCatalog() instanceof SparkSessionCatalog) {
                sparkSessionCatalog = (SparkSessionCatalog<V2SessionCatalog>) spark.sessionState().catalogManager().v2SessionCatalog();
            } else if (spark.sessionState().catalogManager().v2SessionCatalog() instanceof V2SessionCatalog) {
                log.warn("Invalid sparkSessionCatalog type. There's probably a problem with the catalog environment.");
                SparkSessionCatalog<V2SessionCatalog> v2SessionCatalogSparkSessionCatalog = new SparkSessionCatalog<>();
                v2SessionCatalogSparkSessionCatalog.setDelegateCatalog(spark.sessionState().catalogManager().v2SessionCatalog());
                sparkSessionCatalog = v2SessionCatalogSparkSessionCatalog;
            } else {
                throw new RuntimeException("Could not find SparkSessionCatalog");
            }

            Identifier tableIdentifier = Identifier.of(Namespace.of(icebergProperties.getDatabaseName()).levels(), icebergProperties.getTableName());
            Schema tableSchema = SparkSchemaUtil.convert(ds.schema());
            log.info("Iceberg Table schema is: {}", tableSchema.asStruct());

            Map<String, String> options = Maps.newHashMap();
            Transform[] transforms = {};
            if (!sparkSessionCatalog.namespaceExists(List.of(icebergProperties.getDatabaseName()).toArray(String[]::new))) {
                sparkSessionCatalog.createNamespace(List.of(icebergProperties.getDatabaseName()).toArray(String[]::new), options);
            }
            if (!sparkSessionCatalog.tableExists(tableIdentifier)) {
                options.put(TableProperties.OBJECT_STORE_ENABLED, "true");
                sparkSessionCatalog
                        .createTable(tableIdentifier, SparkSchemaUtil.convert(tableSchema), transforms, options);
            }
            SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);
            log.warn("------------AFTER Dataframe writeTo----------------");
            ds.sortWithinPartitions("createdDate", "age")
                    .writeTo(icebergConfiguration.getTableFullName())
//                    .partitionedBy(col("createdDate"), col("createdDate"))
                    .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
                    .append();
            spark.sql("select * from " + icebergConfiguration.getTableFullName()).show();
        } catch (TableAlreadyExistsException | NoSuchNamespaceException | NoSuchTableException | NamespaceAlreadyExistsException /*| IOException*/ e) {
            e.printStackTrace();
        }
    }
}
