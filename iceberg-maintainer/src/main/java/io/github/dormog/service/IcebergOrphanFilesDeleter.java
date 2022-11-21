package io.github.dormog.service;

import io.github.dormog.configuration.IcebergConfiguration;
import io.github.dormog.configuration.properties.IcebergProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IcebergOrphanFilesDeleter implements ActionExecutor {
    private final IcebergProperties icebergProperties;
    private final IcebergConfiguration icebergConfiguration;

    public void execute(SparkSession spark) {
        try {
            log.info("Starting orphan files deletion execution");
            SparkSessionCatalog<V2SessionCatalog> sparkSessionCatalog = (SparkSessionCatalog<V2SessionCatalog>) spark.sessionState().catalogManager().v2SessionCatalog();
            Identifier tableIdentifier = Identifier.of(Namespace.of(icebergProperties.getDatabaseName()).levels(), icebergProperties.getTableName());
            SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);
            deleteOrphanFiles(sparkTable);
        } catch (NoSuchTableException e) {
            e.printStackTrace();
        }
    }

    /**
     * In Spark and other distributed processing engines, task or job failures can leave files that are not referenced by table metadata,
     * and in some cases normal snapshot expiration may not be able to determine a file is no longer needed and delete it.
     */
    private void deleteOrphanFiles(SparkTable sparkTable) {
        long tsToExpire = System.currentTimeMillis() - (1000 * 60); // 10 minute
        DeleteOrphanFiles.Result result = SparkActions
                .get()
                .deleteOrphanFiles(sparkTable.table())
                .olderThan(tsToExpire)
                .execute();
        log.info(result.toString());
        log.info("The orphans deletion succeeded. The orphanFileLocations is {}",
                result.orphanFileLocations());
    }
}
