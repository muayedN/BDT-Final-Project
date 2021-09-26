package io.muayed.finalproject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

@Component
public class MyKafkaProducer {
    private static String KafkaBrokerEndpoint = "localhost:9092";
    private static String KafkaTopic = "messages";
    private Producer<String, Performance> producer;

    @PostConstruct
    private void producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class);

        producer = new KafkaProducer<>(properties);
    }

    @Scheduled(fixedRate = 10000)
    public void init() {
        publishMessages();
        System.out.println("Producing job completed");
    }


    private Performance memoryUsage() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        double maxMem = memoryMXBean.getHeapMemoryUsage().getMax() / 1048580;
        double currentUsage = memoryMXBean.getHeapMemoryUsage().getInit() / 1048580;

        return new Performance(LocalDateTime.now().toString(), maxMem, currentUsage);
    }


    private void publishMessages() {

        ProducerRecord<String, Performance> record = new ProducerRecord<>(
                KafkaTopic, UUID.randomUUID().toString(), memoryUsage());
        producer.send(record, (metadata, exception) -> {
            if (metadata != null) {
                System.out.println("Data: -> " + record.key() + " | " + record.value());
            } else {
                System.out.println("Error Sending record -> " + record.value());
            }
        });
    }


}
