package com.tech.kafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello World!!");

        // Step 1 : Create producer properties
        // old way
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // new way
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        // Create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "hello world");

        // Send data - async
        kafkaProducer.send(record);

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
