package com.guru.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class ProducerDemoWithKeys {

    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // Create a producer record.
            String topic = "first_topic";
            String key = "id_" + i;
            String value = "Hello World from Java : " + i;
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            // Send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // Executes every time a record is sent successfully or exception is thrown.
                    if (null == exception) {
                        //success
                        log.info(record.key() + ":" + metadata.partition());
                    } else {
                        log.error("Error while producing", exception);
                    }
                }
            });
        }

        //flush data
        producer.flush();
    }
}
