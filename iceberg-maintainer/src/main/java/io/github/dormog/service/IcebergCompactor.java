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
            SparkSessionCatalog<V2SessionCatalog> sparkSessionCatalog = (SparkSessionCatalog<V2SessionCatalog>) spark.sessionState().catalogManager().v2SessionCatalog();
            Namespace namespace = Namespace.of(icebergProperties.getDatabaseName());
            Identifier tableIdentifier = Identifier.of(namespace.levels(), icebergProperties.getTableName());
            SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);
            compactDataFiles(sparkTable);
        } catch (NoSuchTableException e) {
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
        log.info("The compaction succeeded. The number of added data files is {}, rewrittenDataFilesCount is {}", result.addedDataFilesCount(),
                result.rewrittenDataFilesCount());
    }
}
