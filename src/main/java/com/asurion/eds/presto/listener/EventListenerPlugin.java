package com.asurion.eds.presto.listener;

import com.asurion.eds.presto.listener.kafkamitter.PrestoEventsKafkaEmitterFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.Collections;

public class EventListenerPlugin implements Plugin {

    public Iterable<EventListenerFactory> getEventListenerFactories() {
        return Collections.singletonList(
                new PrestoEventsKafkaEmitterFactory()
        );
    }
}
