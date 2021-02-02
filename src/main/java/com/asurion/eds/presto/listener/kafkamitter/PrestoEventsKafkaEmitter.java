package com.asurion.eds.presto.listener.kafkamitter;

import com.asurion.eds.presto.listener.Configuration;
import com.asurion.eds.presto.listener.kafkamitter.serialization.JsonMapper;

import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

public class PrestoEventsKafkaEmitter implements EventListener {
    private final String queryCompletedTopic;
    private final String queryCreatedTopic;
    private final String splitCompletedTopic;
    private final KafkaProducer<String, String> kafkaProducer;
    private final JsonMapper jsonMapper = new JsonMapper();


    public PrestoEventsKafkaEmitter(Map<String, String> config) {
        Configuration configuration = new Configuration(config);

        Properties kafkaProperties = configuration.getConfig("kafka");
        kafkaProducer = new KafkaProducerFactory().create(kafkaProperties);

        Properties kafkaTopicProperties = configuration.getConfig("kafka-topics");
        queryCompletedTopic = kafkaTopicProperties.stringPropertyNames().contains("query-completed") ? kafkaTopicProperties.getProperty("query-completed") : null;
        queryCreatedTopic = kafkaTopicProperties.stringPropertyNames().contains("query-created") ? kafkaTopicProperties.getProperty("query-created") : null;
        splitCompletedTopic = kafkaTopicProperties.stringPropertyNames().contains("split-completed") ? kafkaTopicProperties.getProperty("split-completed") : null;
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        if(queryCompletedTopic instanceof String) {
            String message = jsonMapper.convert(queryCompletedEvent);
            ProducerRecord<String, String> record = new ProducerRecord<>(queryCompletedTopic, queryCompletedEvent.getMetadata().getQueryId(), message);
            kafkaProducer.send(record);
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        if(queryCreatedTopic instanceof String) {
            String message = jsonMapper.convert(queryCreatedEvent);
            ProducerRecord<String, String> record = new ProducerRecord<>(queryCreatedTopic, queryCreatedEvent.getMetadata().getQueryId(), message);
            kafkaProducer.send(record);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        if(splitCompletedTopic instanceof String) {
            String message = jsonMapper.convert(splitCompletedEvent);
            ProducerRecord<String, String> record = new ProducerRecord<>(splitCompletedTopic, splitCompletedEvent.getQueryId(), message);
            kafkaProducer.send(record);
        }
    }
}
