# Spring Kafka Consumer

A Spring Boot application that listens for string messages on a configurable Kafka topic and
logs each message's topic, partition, offset, key, and value.

## Requirements

- Java 17 or newer
- Maven 3.8+
- A running Kafka broker

## Run

From this directory:

```bash
mvn spring-boot:run
```

By default, the app connects to `localhost:9092`, listens to the `orders` topic, and uses the
`orders-consumer` consumer group. Override these values with `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_TOPIC`, and `KAFKA_CONSUMER_GROUP`.

The consumer starts at the earliest available offset when joining a group with no committed
offset. Existing group offsets take precedence.

To publish a test message, run the producer application in [`../kafkaproducer`](../kafkaproducer)
and send a `POST /api/messages` request as documented in the [project README](../README.md).

## Test and package

```bash
mvn test
mvn package
```
