# Spring Kafka Applications

This folder contains two Spring Boot applications that communicate through Kafka:

- [Kafka producer](./kafkaproducer) exposes a REST API for publishing messages.
- [Kafka consumer](./kafkaconsumer) listens for messages and logs their metadata and values.

## Requirements

- Java 17 or newer
- Maven 3.8+
- A running Kafka broker

Both applications connect to `localhost:9092` and use the `orders` topic by default. Set
`KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC` to override these defaults. Start each application
from its own directory with `mvn spring-boot:run`.

## Run the consumer

In a terminal:

```bash
cd kafkaconsumer
mvn spring-boot:run
```

The consumer uses the `orders-consumer` group by default. Override it with
`KAFKA_CONSUMER_GROUP`. When the group has no committed offset, it reads from the earliest
available offset.

## Run the producer

In another terminal:

```bash
cd kafkaproducer
mvn spring-boot:run
```

Publish a message:

```bash
curl -i -X POST http://localhost:8080/api/messages \
  -H 'Content-Type: application/json' \
  -d '{"key":"ORD-204","value":"{\"orderId\":\"ORD-204\",\"status\":\"CREATED\"}"}'
```

The producer waits for Kafka to acknowledge the message and returns its topic, key, partition,
and offset. The `key` is optional; `value` must be a non-blank string. Invalid requests return
`400 Bad Request`, and Kafka send failures return `502 Bad Gateway`.

## Test and package

Run these commands from either application directory:

```bash
mvn test
mvn package
```

For a local multi-broker Kafka setup and topic instructions, see
[`../lab04/docker-compose.yml`](../lab04/docker-compose.yml) and the
[Kafka producer lab](../lab09.md).
