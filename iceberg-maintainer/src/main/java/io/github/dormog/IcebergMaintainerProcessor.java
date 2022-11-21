package io.github.dormog;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.dormog.configuration.properties.ActiveTask;
import io.github.dormog.configuration.properties.IcebergMaintainerProperties;
import io.github.dormog.configuration.properties.KafkaConsumerProperties;
import io.github.dormog.service.IcebergCompactor;
import io.github.dormog.service.IcebergManifestsRewriter;
import io.github.dormog.service.IcebergOrphanFilesDeleter;
import io.github.dormog.service.IcebergSnapshotsExpirer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j
public class IcebergMaintainerProcessor implements ApplicationRunner {
    private final SparkSession spark;
    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final ObjectMapper objectMapper;
    private final IcebergMaintainerProperties icebergMaintainerProperties;
    private final IcebergOrphanFilesDeleter icebergOrphanFilesDeleter;
    private final IcebergManifestsRewriter icebergManifestsRewriter;
    private final IcebergCompactor icebergCompactor;
    private final IcebergSnapshotsExpirer icebergSnapshotsExpirer;

    @Override
    public void run(ApplicationArguments args) {
        for (ActiveTask task: icebergMaintainerProperties.getActiveTasks()) {
            switch (task) {
                case COMPACTION -> icebergCompactor.execute(spark);
                case REWRITE_MANIFESTS -> icebergManifestsRewriter.execute(spark);
                case EXPIRE_SNAPSHOTS -> icebergSnapshotsExpirer.execute(spark);
                case DELETE_ORPHANS -> icebergOrphanFilesDeleter.execute(spark);
            }
        }
    }
}
