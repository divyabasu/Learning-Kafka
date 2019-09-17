package com.tech.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"
        );
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );
        properties.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        // Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //assign
        TopicPartition partitionToreadFrom  =
                new TopicPartition("first_topic", 0);
        kafkaConsumer.assign(Collections.singletonList(partitionToreadFrom));

        // seek
        kafkaConsumer.seek(partitionToreadFrom, 15L);

        int numberOfRead = 5;
        int numberOfReadAlready = 0;
        boolean keepOnReading = true;
        //poll for new data
        while (keepOnReading){
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
            for(ConsumerRecord<String, String> record : records){
                numberOfReadAlready += 1;
                logger.info("Key : " + record.key());
                logger.info("Value : " + record.value());
                logger.info("Partition : " + record.partition());
                logger.info("Offsets : " + record.offset());
                if(numberOfRead == numberOfReadAlready){
                    keepOnReading = false;
                    break;
                }

            }
        }

        logger.info("Exiting the application");
    }
}
