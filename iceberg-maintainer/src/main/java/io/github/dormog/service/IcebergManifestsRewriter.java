package io.github.dormog.service;

import io.github.dormog.configuration.IcebergConfiguration;
import io.github.dormog.configuration.properties.IcebergProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.catalog.Namespace;
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
public class IcebergManifestsRewriter implements ActionExecutor {
    private final IcebergProperties icebergProperties;
    private final IcebergConfiguration icebergConfiguration;

    public void execute(SparkSession spark) {
        try {
            log.info("Starting manifests rewriting execution");
            SparkSessionCatalog<V2SessionCatalog> sparkSessionCatalog = (SparkSessionCatalog<V2SessionCatalog>) spark.sessionState().catalogManager().v2SessionCatalog();
            Identifier tableIdentifier = Identifier.of(Namespace.of(icebergProperties.getDatabaseName()).levels(), icebergProperties.getTableName());
            SparkTable sparkTable = (SparkTable) sparkSessionCatalog.loadTable(tableIdentifier);
            rewriteManifests(sparkTable);
        } catch (NoSuchTableException e) {
            e.printStackTrace();
        }
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
