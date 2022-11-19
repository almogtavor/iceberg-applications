package io.github.dormog.service;

import io.github.dormog.configuration.IcebergConfiguration;
import io.github.dormog.configuration.properties.IcebergProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.actions.MigrateTable;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class IcebergCompactor implements ActionExecutor {
    private final IcebergProperties icebergProperties;
    private final IcebergConfiguration icebergConfiguration;

    public void execute(SparkSession spark) {
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
            Map<String, String> options = Maps.newHashMap();
            Transform[] transforms = {};
            if (!sparkSessionCatalog.namespaceExists(List.of(icebergProperties.getDatabaseName()).toArray(String[]::new))) {
                sparkSessionCatalog.createNamespace(List.of(icebergProperties.getDatabaseName()).toArray(String[]::new), options);
            }
            if (!sparkSessionCatalog.tableExists(tableIdentifier)) {
                throw new RuntimeException();
            }
            SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);
//            log.warn("------------AFTER Spark SQL INSERT----------------");
//            spark.sql("INSERT INTO " + tableName + " VALUES (10,'spark sql-insert')");
//            spark.sql("select * from " + tableName).show();
            log.warn("------------AFTER Dataframe writeTo----------------");
            spark.sql("select * from " + icebergConfiguration.getTableFullName()).show();

            compactDataFiles(sparkTable);
            rewriteManifests(sparkTable);
        } catch (NoSuchTableException | NamespaceAlreadyExistsException e) {
            e.printStackTrace();
        }
    }

    /**
     * Iceberg can compact data files in parallel using Spark with the rewriteDataFiles action.
     */
    private void compactDataFiles(SparkTable sparkTable) {
        RewriteDataFiles.Result result = SparkActions
                .get()
                .rewriteDataFiles(sparkTable.table())
                .option("min-input-files", "2")
//                .filter(Expressions.equal("date", "2020-08-18"))
//                .filter(Expressions.equal("name", "bla"))
                .option("target-file-size-bytes", Long.toString(500 * 1024 * 1024)) // 500 MB
                .execute();
        log.info(result.toString());
        log.info("The compaction succeeded. The number of added data files is {}, rewrittenDataFilesCount is {}", result.addedDataFilesCount(),
                result.rewrittenDataFilesCount());
    }

    /**
     * Manifests in the metadata tree are automatically compacted in the order they are added,
     * which makes queries faster when the write pattern aligns with read filters. For example,
     * writing hourly-partitioned data as it arrives is aligned with time range query filters.
     *
     * When a table’s write pattern doesn’t align with the query pattern,
     * metadata can be rewritten to re-group data files into manifests using rewriteManifests.
     */
    private void rewriteManifests(SparkTable sparkTable) {
        RewriteManifests.Result result = SparkActions
                .get()
                .rewriteManifests(sparkTable.table())
                .option("min-input-files", "2")
                .rewriteIf(file -> file.length() < 10 * 1024 * 1024) // 10 MB
                .execute();
        System.out.println(result);
    }
}
