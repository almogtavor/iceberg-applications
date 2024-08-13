package io.github.almogtavor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.almogtavor.model.SamplePojo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@Slf4j
public class DevSamplePojoKafkaProducer {
    public static final short REPLICATION_FACTOR = 1;
    public static final int NUM_PARTITIONS = 1;
    public static final String TOPIC = "input";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    private static final Random RANDOM = new Random();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static String generateRandomName() {
        String[] names = {"Paul", "John", "Ringo", "George", "Bob", "Charlie", "Parker"};
        return names[RANDOM.nextInt(names.length)];
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {
            Collection<NewTopic> topics = Collections.singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR));
            if (!adminClient.listTopics().names().get().contains(TOPIC)) {
                adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            }
        }
        try (
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        Map.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                BOOTSTRAP_SERVERS,
                                ProducerConfig.CLIENT_ID_CONFIG,
                                UUID.randomUUID().toString()
                        ),
                        new StringSerializer(),
                        new StringSerializer()
                );
        ) {
            int numberOfRecords = 10; // Set the number of records you want to generate

            for (int i = 0; i < numberOfRecords; i++) {
                String id = "ID" + i;
                SamplePojo samplePojo = SamplePojo.builder()
                        .itemId(id)
                        .name(generateRandomName())
                        .coolId(UUID.randomUUID().toString())
                        .age(RANDOM.nextInt(100)) // Random age between 0 and 99
                        .createdDate(new Date())
                        .build();

                producer.send(new ProducerRecord<>(TOPIC, id, OBJECT_MAPPER.writeValueAsString(samplePojo))).get();
            }
            log.info("Successfully produced to kafka {} records", numberOfRecords);
        } catch (JsonProcessingException e) {
            log.error("Could not produce to kafka", e);
        }
    }
}
