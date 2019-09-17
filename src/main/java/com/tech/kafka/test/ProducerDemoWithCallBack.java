package com.tech.kafka.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

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

        for(int i =0 ; i<10; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "hello world"+i);

            // Send data - async
            kafkaProducer.send(record, (recordMetadata, e) -> {
                // executes every time record is successfully sent or an exception is thrown
                if (e == null) {
                    logger.info("received topic from metadata : " + recordMetadata.topic());
                    logger.info("received partition from metadata : " + recordMetadata.partition());
                    logger.info("received offset from metadata : " + recordMetadata.offset());
                    logger.info("received timestamp from metadata : " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing : " + e);
                }
            });
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
