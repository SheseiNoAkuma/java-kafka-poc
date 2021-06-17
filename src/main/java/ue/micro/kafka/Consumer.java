package ue.micro.kafka;

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

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {

        logger.info("ready to go");

        // create configuration
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-other-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest - latest - none

        CountDownLatch latch = new CountDownLatch(1);

        ConsumerAsync consumerAsync = new ConsumerAsync(latch, properties);

        Thread thread = new Thread(consumerAsync);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught shutdown hook");
            consumerAsync.shoutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            logger.info("application closing");
        }
    }

    public static class ConsumerAsync implements Runnable {

        private final CountDownLatch latch;
        private final Properties properties;
        private KafkaConsumer<String, String> consumer;

        public ConsumerAsync(CountDownLatch latch, Properties properties) {
            this.latch = latch;
            this.properties = properties;
        }

        @Override
        public void run() {

            // create a consumer
            consumer = new KafkaConsumer<>(properties);

            // subscribe the consumer
            // note an alternative to subscribe is assign and seek where you select your topic / partition / topic
            consumer.subscribe(Collections.singleton("java-topic"));

            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                    logger.info("read zero messages? {}", records.isEmpty());
                    for (ConsumerRecord<String, String> message: records)
                        logger.info("message read: {}", message);
                }
            } catch (WakeupException e) {
                logger.info("received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shoutdown() {
            // method to interrupt poll
            consumer.wakeup();
        }
    }
}
