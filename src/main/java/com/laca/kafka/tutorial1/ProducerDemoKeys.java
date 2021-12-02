package com.laca.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        //Constants
        String bootstrapServers = "localhost:9093";

        //Parameter declaration
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creates new producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String topicName = "first_topic";
            String value = "hello world: " + i;
            String key = "id_" + i;

            //Creates new record
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            //Sends record async (local only)
            producer.send(record, (recordMetadata, e) -> {
                //Executes every time when a record is successfully sent or exception is thrown
                if (e == null) {
                    logger.info("Received new metadata.\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            });
        }

        //Sends record to server (sync)
        producer.flush();
    }
}
