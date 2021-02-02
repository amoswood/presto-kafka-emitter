package com.asurion.eds.presto.listener.kafkamitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerFactory {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerFactory.class);

    static Map<String, String> immutableProperties = new HashMap<>() {{
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }};


    public Properties addProperties(final Properties properties) {
        return new Properties() {{
            properties.forEach((k, v) -> setProperty(
                    (String) k, (String) v
            ));
            putAll(immutableProperties);
        }};
    }

    public KafkaProducer<String, String> create(final Properties properties) {
        Properties kafkaProperties = addProperties(properties);
        log.info("Creating Kafka producer with properties " + kafkaProperties);
        return new KafkaProducer<>(kafkaProperties);
    }
}
