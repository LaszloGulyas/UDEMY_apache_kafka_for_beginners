package com.laca.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        //Constants
        String bootstrapServers = "localhost:9093";
        String topicName = "first_topic";

        //Parameter declaration
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creates new producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Creates new record
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello world!");

        //Sends record async (local only)
        producer.send(record);

        //Sends record to server (sync)
        producer.flush();
    }
}
