package io.github.almogtavor.service;

import io.github.almogtavor.configuration.IcebergConfiguration;
import io.github.almogtavor.configuration.properties.IcebergProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
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
import org.springframework.stereotype.Service;

import java.util.*;

import static org.apache.spark.sql.functions.col;


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

            Namespace namespace = Namespace.of(icebergProperties.getDatabaseName());
            Identifier tableIdentifier = Identifier.of(namespace.levels(), icebergProperties.getTableName());
            Schema tableSchema = SparkSchemaUtil.convert(ds.schema());
            log.info("Iceberg Table schema is: {}", tableSchema.asStruct());

            Map<String, String> options = Maps.newHashMap();
            Transform[] transforms = {};
            if (icebergProperties.getCatalogType() == IcebergProperties.CatalogTypes.NESSIE) {
                // Ensure the branch exists in Nessie before proceeding
                String branchName = "main";
                String baseBranch = "main";

                if (!namespaceExistsInNessie(spark, branchName)) {
                    // Create a branch if it doesn't exist
                    spark.sql(String.format("CREATE BRANCH IF NOT EXISTS %s IN nessie FROM %s", branchName, baseBranch));
                    log.info("Successfully created Nessie branch: {}", branchName);
                }

                // Switch to the correct branch
                spark.sql(String.format("USE REFERENCE %s IN nessie", branchName));
                log.info("Switched to Nessie branch: {}", branchName);
            } else {
                // Default behavior for other catalogs
                if (!sparkSessionCatalog.namespaceExists(namespace.levels())) {
                    sparkSessionCatalog.createNamespace(namespace.levels(), options);
                    log.info("Successfully created namespace: {}", namespace);
                }
            }
            if (!sparkSessionCatalog.tableExists(tableIdentifier)) {
                options.put(TableProperties.OBJECT_STORE_ENABLED, "true");
                sparkSessionCatalog
                        .createTable(tableIdentifier, SparkSchemaUtil.convert(tableSchema), transforms, options);
            }
            log.warn("------------AFTER Dataframe writeTo----------------");
            ds.sortWithinPartitions("createdDate", "age")
                    .writeTo(icebergConfiguration.getTableFullName())
                    .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
                    .append();
            spark.sql("select * from " + icebergConfiguration.getTableFullName()).show();
        } catch (TableAlreadyExistsException | NoSuchNamespaceException | NoSuchTableException | NamespaceAlreadyExistsException /*| IOException*/ e) {
            e.printStackTrace();
        }
    }

    private boolean namespaceExistsInNessie(SparkSession spark, String branchName) {
        // Check if the branch already exists in Nessie
        try {
            Dataset<Row> result = spark.sql(String.format("LIST REFERENCES IN nessie WHERE ref = '%s'", branchName));
            return !result.isEmpty();
        } catch (Exception e) {
            log.warn("Failed to check if the namespace exists in Nessie: {}", branchName, e);
            return false;
        }
    }
}
