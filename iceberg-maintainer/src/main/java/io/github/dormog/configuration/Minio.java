package io.github.dormog.configuration;

import io.github.dormog.configuration.properties.S3Properties;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.errors.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class Minio {
    public static final int MINIO_DEFAULT_PORT = 9000;
    public static final String TEST_BUCKET = "testbucket";
    public static final String MINIO_ACCESS_KEY = "admin";
    public static final String MINIO_SECRET_KEY = "password";
    static final String DEFAULT_IMAGE = "minio/minio";
    static final String DEFAULT_TAG = "dev";
    static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    static final String HEALTH_ENDPOINT = "/minio/health/ready";
    private GenericContainer<?> container;
    private MinioClient client;
    private final S3Properties s3Properties;
    private final S3Configuration s3Configuration;

    public void start() {
        try {
            log.info("Starting up the S3 container");
            this.container = new GenericContainer<>(DEFAULT_IMAGE)
                    .withExposedPorts(MINIO_DEFAULT_PORT)
                    .waitingFor(new HttpWaitStrategy()
                            .forPath(HEALTH_ENDPOINT)
                            .forPort(MINIO_DEFAULT_PORT)
                            .withStartupTimeout(Duration.ofSeconds(30)))
                    .withEnv("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
                    .withEnv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
                    .withCommand("server " + DEFAULT_STORAGE_DIRECTORY);

            this.container.start();
            this.container.waitingFor(Wait.forHttp("/minio/health/ready"));

            client = MinioClient.builder()
                    .endpoint("http://" + container.getHost() + ":" + container.getMappedPort(MINIO_DEFAULT_PORT))
                    .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                    .build();
            client.ignoreCertCheck();

            client.makeBucket(MakeBucketArgs.builder()
                    .bucket(TEST_BUCKET)
                    .build());

            log.info("Minio Started!");
        } catch (NoSuchAlgorithmException | KeyManagementException | ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException | InvalidResponseException | IOException | ServerException | XmlParserException e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void stop() {
        log.warn("------------FINAL S3 FILE LIST ----------------");
        listFiles();
        try {
            if (container != null) {
                container.stop();
            }
        } catch (Exception e) {
            // ignored
        }
    }

    public Integer getMappedPort() {
        return this.container.getMappedPort(MINIO_DEFAULT_PORT);
    }

    public void listBuckets()
            throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException,
            XmlParserException, ErrorResponseException {
        List<Bucket> bucketList = client.listBuckets();
        for (Bucket bucket : bucketList) {
            log.info("Bucket: {} {}", bucket.name(), bucket.creationDate());
        }
    }

    public void listFiles() {
        listFiles(null);
    }

    public void listFiles(String message) {
        if (client == null) {
            client = MinioClient.builder()
                    .endpoint(s3Configuration.getEndpoint())
                    .credentials(s3Properties.getAccessKey(), s3Properties.getSecretKey())
                    .build();
        }
        log.info("-----------------------------------------------------------------");
        if (message != null) {
            log.info("{}", message);
        }
        try {
            List<Bucket> bucketList = client.listBuckets();
            for (Bucket bucket : bucketList) {
                log.info("Bucket:{} ROOT", bucket.name());
                Iterable<Result<Item>> results =
                        client.listObjects(ListObjectsArgs.builder().bucket(bucket.name()).recursive(true).build());
                for (Result<Item> result : results) {
                    Item item = result.get();
                    log.info("Bucket:{} Item:{} Size:{}", bucket.name(), item.objectName(), item.size());
                }
            }
        } catch (Exception e) {
            log.info("Failed listing bucket");
        }
        log.info("-----------------------------------------------------------------");
    }
}
