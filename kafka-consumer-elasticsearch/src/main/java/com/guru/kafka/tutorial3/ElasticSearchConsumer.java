package com.guru.kafka.tutorial3;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ElasticSearchConsumer {
    private static final String TWITTER_TOPIC = "twitter_tweets";
    private static final JsonParser jsonParser = new JsonParser();
    public static void main(String[] args) throws IOException {
        new ElasticSearchConsumer().run();
    }

    private void run() throws IOException {
        KafkaConsumer<String, String> consumer = createKafkaConsumer(TWITTER_TOPIC);

        RestHighLevelClient client = createClient();

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
            for (ConsumerRecord<String, String> record : records) {
                // Set the IndexRequest id to be unique to achieve idepotence.
                // 1) Achieve Id from Kafka
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                
                // 2) Achieve id from data itself, in this case tweet id
                String id = extractIdFromTweet(record.value());
                ///{index}/_doc/{id}, /{index}/_doc, or /{index}/_create/{id}).
                IndexRequest indexRequest = new IndexRequest("twitter").source(record.value(), XContentType.JSON);
                indexRequest.id(id);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                log.info("{}", indexResponse.getId());

                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String extractIdFromTweet(String value) {
        return jsonParser.parse(value)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private RestHighLevelClient createClient() {
        String hostName = "kafka-course-6017116598.eu-west-1.bonsaisearch.net";
        String userName = "";
        String passWord = "";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, passWord));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(restClientBuilder);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
        final String MY_JAVA_APP = "kafka-demo-elasticsearch";

        //Create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, MY_JAVA_APP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to topic(s)
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
}
