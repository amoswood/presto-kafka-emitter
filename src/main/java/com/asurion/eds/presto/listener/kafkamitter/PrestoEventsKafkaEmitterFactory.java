package com.asurion.eds.presto.listener.kafkamitter;

import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.Map;

public class PrestoEventsKafkaEmitterFactory implements EventListenerFactory {
    private final static String NAME = "kafka-emitter";

    public String getName() {
        return NAME;
    }

    public EventListener create(Map<String, String> config) {
        return new PrestoEventsKafkaEmitter(config);
    }
}
