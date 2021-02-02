package com.asurion.eds.presto.listener.kafkamitter;

import com.asurion.eds.presto.listener.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class PrestoEventsKafkaEmitter implements EventListener {
    private static final Logger log = LoggerFactory.getLogger(PrestoEventsKafkaEmitter.class);
    private final String queryCompletedTopic;
    private final String queryCreatedTopic;
    private final String splitCompletedTopic;
    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper = new ObjectMapper() {{
        registerModule(new Jdk8Module()); // Optional support
    }};

    public PrestoEventsKafkaEmitter(Map<String, String> config) {
        Configuration configuration = new Configuration(config);

        log.info("Creating new Kafka Emitter for Presto. Configuring ...");
        Properties kafkaProperties = configuration.getConfig("kafka");
        kafkaProducer = new KafkaProducerFactory().create(kafkaProperties);

        Properties kafkaTopicProperties = configuration.getConfig("kafka-topics");
        queryCompletedTopic = kafkaTopicProperties.stringPropertyNames().contains("query-completed") ? kafkaTopicProperties.getProperty("query-completed") : null;
        queryCreatedTopic = kafkaTopicProperties.stringPropertyNames().contains("query-created") ? kafkaTopicProperties.getProperty("query-created") : null;
        splitCompletedTopic = kafkaTopicProperties.stringPropertyNames().contains("split-completed") ? kafkaTopicProperties.getProperty("split-completed") : null;
        if(isNotBlank(queryCompletedTopic)) {
            log.info("\tSending QueryCompletedEvent to topic '"+queryCompletedTopic+"'");
        }
        if(isNotBlank(queryCreatedTopic)) {
            log.info("\tSending QueryCreatedEvent to topic '"+queryCreatedTopic+"'");
        }
        if(isNotBlank(splitCompletedTopic)) {
            log.info("\tSending SplitCompletedEvent to topic '"+splitCompletedTopic+"'");
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        if(isNotBlank(queryCompletedTopic)) {
            try {
                String json = objectMapper.writeValueAsString(queryCompletedEvent);
                ProducerRecord<String, String> record = new ProducerRecord<>(queryCompletedTopic, queryCompletedEvent.getMetadata().getQueryId(), json);
                kafkaProducer.send(record);
            } catch (Exception e) {
                log.warn("Unable to emit QueryCompletedEvent to Kafka", e);
            }
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        if(isNotBlank(queryCreatedTopic)) {
            try {
                String json = objectMapper.writeValueAsString(queryCreatedEvent);
                ProducerRecord<String, String> record = new ProducerRecord<>(queryCreatedTopic, queryCreatedEvent.getMetadata().getQueryId(), json);
                kafkaProducer.send(record);
            } catch (Exception e) {
                log.warn("Unable to emit QueryCreatedEvent to Kafka", e);
            }
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        if(isNotBlank(splitCompletedTopic)) {
            try {
                String json = objectMapper.writeValueAsString(splitCompletedEvent);
                ProducerRecord<String, String> record = new ProducerRecord<>(splitCompletedTopic, splitCompletedEvent.getQueryId(), json);
                kafkaProducer.send(record);
            } catch (Exception e) {
                log.warn("Unable to emit SplitCompletedEvent to Kafka", e);
            }
        }
    }

    private boolean isNotBlank(final String str) {
        return str != null && !str.isEmpty();
    }
}
