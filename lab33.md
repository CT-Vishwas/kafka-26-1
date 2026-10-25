# Lab: Customer Order Totals with Kafka Streams

This lab aggregates each customer's order amounts. Kafka record keys are customer IDs and
record values are numeric amounts. The input and output values are UTF-8 strings, so they can
be read with Kafka's console tools.

## Requirements

- Java 17+
- Maven 3.8+
- Docker Engine and Docker Compose
- The Kafka cluster from [lab04/docker-compose.yml](lab04/docker-compose.yml)

## Step 1: Start Kafka and create topics

From the repository root, start the Kafka cluster:

```bash
docker compose -f lab04/docker-compose.yml up -d
docker compose -f lab04/docker-compose.yml ps
```

Create the input and output topics:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --if-not-exists --topic order-amounts \
	--partitions 3 --replication-factor 3

docker compose -f lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --if-not-exists --topic customer-total \
	--partitions 3 --replication-factor 3
```

## Step 2: Create the Streams application

Add Kafka Streams to the consumer application's `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
</dependency>
```

Create `src/main/java/com/example/kafka/retry/CustomerOrderTotals.java`:

```java
package com.example.kafka.retry;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class CustomerOrderTotals {

    public static void main(String[] args) {
        KafkaStreams streams = createStreams("localhost:9092");
        streams.start();

        Runtime.getRuntime().addShutdownHook(
            new Thread(streams::close)
        );
    }

    static KafkaStreams createStreams(String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-order-totals");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(
            StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
            StreamsConfig.EXACTLY_ONCE_V2
        );
        props.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass()
        );
        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass()
        );

        StreamsBuilder builder = new StreamsBuilder();

        // The producer writes amounts as strings; parse them before aggregation.
        KStream<String, String> orders = builder.stream(
            "order-amounts",
            Consumed.with(Serdes.String(), Serdes.String())
        );

        KTable<String, Long> totals = orders
            .mapValues(value -> {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException e) {
                    return 0L;
                }
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .reduce(Long::sum);

        // Write totals as strings so console consumers display readable values.
        totals.toStream()
            .mapValues(String::valueOf)
            .to(
                "customer-total",
                Produced.with(Serdes.String(), Serdes.String())
            );

        return new KafkaStreams(builder.build(), props);
    }
}
```

When running the topology from the Spring Boot consumer, add the following bean to
`SpringKafkaConsumerApplication`. It starts and closes Kafka Streams with the Spring
application lifecycle, and reads the bootstrap address from Spring configuration:

```java
@Bean(initMethod = "start", destroyMethod = "close")
KafkaStreams customerOrderTotalsStreams(
    @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers
) {
    return CustomerOrderTotals.createStreams(bootstrapServers);
}
```

Include these imports in that application class:

```java
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
```

Use the Spring application as the runner below. The `main` method in
`CustomerOrderTotals` is available when running the Streams class directly; do not start
both runners for this lab.

## Step 3: Start the applications

In one terminal, start the Spring Boot consumer and Streams processor:

```bash
cd spring-kafka-streams-1/kafkaconsumer
mvn spring-boot:run
```

In another terminal, start the producer:

```bash
cd spring-kafka-streams-1/kafkaproducer
mvn spring-boot:run
```

Both applications default to `localhost:9092`. If Kafka uses another address, set
`KAFKA_BOOTSTRAP_SERVERS` to the same broker address for both applications.

## Step 4: Publish customer orders

The producer endpoint accepts the customer ID in the URL and a JSON numeric `amount`.
Use whole-number amounts for this example.

```bash
curl -X POST http://localhost:8082/orders/cust-A \
	-H 'Content-Type: application/json' \
	-d '{"amount":500}'

curl -X POST http://localhost:8082/orders/cust-B \
	-H 'Content-Type: application/json' \
	-d '{"amount":800}'

curl -X POST http://localhost:8082/orders/cust-A \
	-H 'Content-Type: application/json' \
	-d '{"amount":300}'
```

The resulting latest totals should be `cust-A=800` and `cust-B=800`.

## Step 5: Read the output

Read the output topic as text:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic customer-total \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

The topic is an update stream: each new order emits the customer's new total. For the
example above, you will see updates for `cust-A` of `500` and then `800`, and an update for
`cust-B` of `800`. Use the latest value for each key; do not add together all values in the
output topic.

Exactly-once processing protects Kafka Streams' input offsets, state updates, and Kafka output
from being inconsistently committed during retries. It does not deduplicate two separately
published input records with the same customer and amount. If an order is submitted twice,
both records count. The Streams state is also retained across normal restarts, so re-running
the same requests increases the totals again.
