package io.github.dormog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.dormog.model.SamplePojo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DevSamplePojoKafkaProducer {
    public static final short REPLICATION_FACTOR = 1;
    public static final int NUM_PARTITIONS = 1;
    public static final String TOPIC = "input";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";

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
            String id1 = "ID1";
            producer.send(new ProducerRecord<>(TOPIC, id1, new ObjectMapper().writeValueAsString(SamplePojo.builder()
                    .itemId(id1)
                    .name("bla")
                    .coolId("some")
//                    .createdDate(new Date())
                    .build()))).get();
            String id2 = "ID2";
            producer.send(new ProducerRecord<>(TOPIC, id2, new ObjectMapper().writeValueAsString(SamplePojo.builder()
                    .itemId(id2)
                    .name("bla")
                    .coolId("some")
//                    .createdDate(new Date())
                    .build()))).get();

        } catch (ExecutionException | InterruptedException | JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
