package com.guru.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerWithThreadDemo {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String MY_JAVA_APP = "my-java-app";
    public static final String FIRST_TOPIC = "first_topic";

    public static void main(String[] args) {
        new ConsumerWithThreadDemo().runDemo();
    }

    private void runDemo() {
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVERS, FIRST_TOPIC, MY_JAVA_APP, latch);

        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerRunnable.shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("Exception", e);
            }
        }));

        try {
            latch.await();
            log.info("Exiting application !!");
        } catch (InterruptedException e) {
            log.info("Exiting application !!");
        }
    }

    static class ConsumerRunnable implements Runnable {
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private final String bootStrapServer;
        private final String topic;
        private final String groupId;
        private final CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootStrapServer, String topic, String groupId, CountDownLatch latch) {
            this.bootStrapServer = bootStrapServer;
            this.topic = topic;
            this.groupId = groupId;
            this.latch = latch;
        }

        @Override
        public void run() {
            //Create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create consumer
            consumer = new KafkaConsumer<>(properties);

            // subscribe consumer to topic(s)
            consumer.subscribe(Collections.singletonList(topic));

            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
                    records.forEach(record -> log.info(record.toString()));
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal !!");
            } finally {
                consumer.close();
                logger.info("Consumer stopped !!");
                latch.countDown();
            }
        }

        public void shutDown() {
            //This method interrupts Kafka consumer and throws
            consumer.wakeup();
        }
    }
}
