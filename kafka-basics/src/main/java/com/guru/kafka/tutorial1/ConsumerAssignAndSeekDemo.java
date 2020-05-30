package com.guru.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ConsumerAssignAndSeekDemo {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String FIRST_TOPIC = "first_topic";

    public static void main(String[] args) {
        //Create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign and Seek are used mostly to replay data or fetch specific message(s)

        //Assign
        TopicPartition topicPartition = new TopicPartition(FIRST_TOPIC, 0);
        consumer.assign(Collections.singletonList(topicPartition));

        // Seek
        consumer.seek(topicPartition, 5L);

        AtomicBoolean keepReading = new AtomicBoolean(true);

        int noOfMessagesToRead = 5;

        AtomicInteger noOfMessagesRead = new AtomicInteger(0);

        // poll for new data
        while (keepReading.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
            for (ConsumerRecord<String, String> record : records) {
                noOfMessagesRead.incrementAndGet();
                log.info(record.toString());
                if (noOfMessagesRead.get() >= noOfMessagesToRead) {
                    keepReading.set(false);
                    break;
                }

            }
        }
    }
}
