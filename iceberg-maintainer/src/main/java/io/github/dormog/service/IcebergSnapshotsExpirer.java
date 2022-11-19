package io.github.dormog.service;

import io.github.dormog.configuration.IcebergConfiguration;
import io.github.dormog.configuration.properties.IcebergProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.MigrateTable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class IcebergSnapshotsExpirer implements ActionExecutor {
    private final IcebergProperties icebergProperties;
    private final IcebergConfiguration icebergConfiguration;

    public void execute(SparkSession spark) {
        try {
            log.info("GOT HERE DataInitializer");
            // get catalog from spark
            SparkSessionCatalog<V2SessionCatalog> sparkSessionCatalog = (SparkSessionCatalog<V2SessionCatalog>) spark.sessionState().catalogManager().v2SessionCatalog();
            Identifier tableIdentifier = Identifier.of(Namespace.of(icebergProperties.getDatabaseName()).levels(), icebergProperties.getTableName());
            if (!sparkSessionCatalog.tableExists(tableIdentifier)) {
                throw new RuntimeException();
            }
            SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);
            expireSnapshots(sparkTable);
            deleteOrphanFiles(sparkTable);
            log.warn("------------AFTER API APPEND----------------");
            spark.sql("select * from " + icebergConfiguration.getTableFullName()).show();
        } catch (NoSuchTableException e) {
            e.printStackTrace();
        }
    }

    /**
     * This command, unlike .expireSnapshots().expireOlderThan(tsToExpire).commit(), run table expiration in parallel for large tables.
     * Data files are not deleted until they are no longer referenced by a snapshot that may be used for time travel or rollback.
     * Regularly expiring snapshots deletes unused data files.
     */
    private void expireSnapshots(SparkTable sparkTable) {
//        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24); // 1 day
        long tsToExpire = System.currentTimeMillis() - (1000 * 60); // 10 minute
        ExpireSnapshots.Result result = SparkActions
                .get()
                .expireSnapshots(sparkTable.table())
                .expireOlderThan(tsToExpire)
                .execute();
        log.info(result.toString());
        log.info("The snapshots expiring succeeded. The deletedManifestListsCount is {}, " +
                        "deletedManifestsCount is {}, deletedDataFilesCount is {}, deletedEqualityDeleteFilesCount is {}, deletedPositionDeleteFilesCount is {}", result.deletedManifestListsCount(),
                result.deletedManifestsCount(), result.deletedDataFilesCount(), result.deletedEqualityDeleteFilesCount(), result.deletedPositionDeleteFilesCount());
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
