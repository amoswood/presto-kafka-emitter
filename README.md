# Presto events kafka logging plugin

## Build

```
Requires Java 11+
$ mvn build package
```
This will produce uber jar `/target/presto-kafka-emitter-1.0.jar`

## Deployment

On presto coordinator node:

1. Create directory `kafka-emitter` in `$PRESTO_HOME/plugin` and put assembled jar in
2. Create file `/etc/presto/event-listener.properties` and fill it as in the example
```
event-listener.name=kafka-emitter
kafka-emitter.kafka-topics.query-created=platform.dal.sys.presto.query-created
kafka-emitter.kafka-topics.query-completed=platform.dal.sys.presto.query-completed
kafka-emitter.kafka-topics.split-completed=platform.dal.sys.presto.split-completed
kafka-emitter.kafka.bootstrap.servers=pkc-4njzv.us-east-1.aws.confluent.cloud:9092
kafka-emitter.kafka.ssl.endpoint.identification.algorithm=https
kafka-emitter.kafka.security.protocol=SASL_SSL
kafka-emitter.kafka.sasl.mechanism=PLAIN
kafka-emitter.kafka.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<API_KEY>" password="<API_PASSWORD>";
```

*Restart Presto*
