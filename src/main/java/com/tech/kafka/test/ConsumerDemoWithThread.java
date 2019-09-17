package com.tech.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer");
        Runnable myConsumerThread = new ConsumerThread(latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutDown();
            try {
                latch.await();
            } catch(InterruptedException e){
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try{
            latch.await();
        } catch (InterruptedException e){
            logger.error("Application is interrupted " + e);
        } finally {
            logger.info("Application is closing");
        }
    }
}

class ConsumerThread implements Runnable {

    Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerThread(CountDownLatch latch) {
        this.latch = latch;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "new_app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<String, String>(properties);
    }

    @Override
    public void run() {
        try {
            //poll for new data
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key : " + record.key());
                    logger.info("Value : " + record.value());
                    logger.info("Partition : " + record.partition());
                    logger.info("Offsets : " + record.offset());

                }
            }
        } catch (WakeupException e){
            logger.info("Received shutdown signal");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutDown(){
        // wakeup method interrupts consumer.poll() with WakeupException
        consumer.wakeup();
    }
}
