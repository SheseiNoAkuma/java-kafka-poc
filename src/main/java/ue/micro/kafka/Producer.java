package ue.micro.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static final Random random = new Random();

    public static void main(String[] args) {

        logger.info("ready to go");

        // create configuration
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // crete producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            // send data -async
            ProducerRecord<String, String> data = new ProducerRecord<>("java-topic", String.valueOf(random.nextInt()), "anyValue");
            logger.info("about to produce {}", data);
            producer.send(data, (recordMetadata, e) -> {
                // execute every time record was successfully sent or an exception is thrown
                if (e != null)
                    logger.error("some error occurred", e);
                else
                    logger.info("Message sent with metadata: topic<{}>, partition<{}>, offset<{}>, timestamp<{}>",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
            });

            producer.flush();
        }
    }
}
