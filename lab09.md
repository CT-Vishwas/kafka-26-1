# Lab: Java Kafka Producer with Replication

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker Engine and Docker Compose

This lab uses the three-broker cluster in [`lab04/docker-compose.yml`](./lab04/docker-compose.yml). Run Docker Compose commands from the repository root.

## Lab steps

### 1. Start the Kafka cluster

```bash
docker compose -f lab04/docker-compose.yml up -d
docker compose -f lab04/docker-compose.yml ps
```

Wait until all three brokers are running.

### 2. Create and verify the topic

Create `orders` with three partitions, replication factor 3, and minimum ISR 2:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

If `orders` already exists, skip creation and describe it:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --describe \
  --topic orders
```

Check that the topic has three partitions, replication factor 3, and ISR members.

### 3. Create a Maven producer project

```bash
mvn archetype:generate \
  -DgroupId=com.example.kafka \
  -DartifactId=kafka-java-producer \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DarchetypeVersion=1.5 \
  -DinteractiveMode=false

cd kafka-java-producer
```

In `pom.xml`, set Java 17 and add the Kafka client dependency and exec plugin:

```xml
<properties>
  <maven.compiler.release>17</maven.compiler.release>
</properties>

<dependencies>
  <dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>4.3.1</version>
  </dependency>
</dependencies>

<build>
  <plugins>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-compiler-plugin</artifactId>
      <version>3.13.0</version>
      <configuration>
        <release>${maven.compiler.release}</release>
      </configuration>
    </plugin>
    <plugin>
      <groupId>org.codehaus.mojo</groupId>
      <artifactId>exec-maven-plugin</artifactId>
      <version>3.5.0</version>
    </plugin>
  </plugins>
</build>
```

### 4. Write the producer

Create `src/main/java/com/example/kafka/OrderProducer.java`:

```java
package com.example.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("enable.idempotence", true);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                String orderId = "ORD-" + i;
                String orderJson = """
                    {"orderId":"%s","product":"Laptop","quantity":1,"amount":75000}
                    """.formatted(orderId);

                ProducerRecord<String, String> record =
                    new ProducerRecord<>("orders", orderId, orderJson);

                record.headers().add(
                    "event-type", "ORDER_CREATED".getBytes(StandardCharsets.UTF_8)
                );
                producer.send(record, (metadata, error) -> {
                    if (error != null) {
                        System.err.println("Failed to send " + orderId + ": " + error);
                    } else {
                        System.out.printf(
                            "Sent key=%s partition=%d offset=%d%n",
                            orderId, metadata.partition(), metadata.offset()
                        );
                    }
                });
            }
            producer.flush();
        }
    }
}
```

### 5. Build and run

```bash
mvn clean package
mvn exec:java -Dexec.mainClass=com.example.kafka.OrderProducer
```

The producer prints the key, partition, and offset for each successful send.

### 6. Consume and inspect messages

From the repository root, open a new terminal and run:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

### 7. Test broker failure

From the repository root, stop broker 1:

```bash
docker compose -f lab04/docker-compose.yml stop kafka1
```

Describe the topic through the remaining brokers:

```bash
docker compose -f lab04/docker-compose.yml exec kafka2 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka2:29092,kafka3:29092 \
  --describe \
  --topic orders
```

Check leader and ISR changes. Then restore broker 1 and wait for replicas to catch up:

```bash
docker compose -f lab04/docker-compose.yml start kafka1
```

### 8. Stop the cluster

From the repository root:

```bash
docker compose -f lab04/docker-compose.yml down
```

## Key points

- Each partition has one leader; replicas follow the leader.
- `acks=all` waits for acknowledgment from the current in-sync replicas.
- `min.insync.replicas=2` prevents `acks=all` writes when fewer than two replicas are in sync.
- A record key such as `orderId` determines partitioning; records with the same key are routed to the same partition.
- Producer idempotence helps avoid duplicate records caused by retries in a producer session; it does not guarantee exactly-once behavior across application restarts.
- Use host listener addresses (`localhost:9092,9093,9094`) for Java on the host and Docker listener addresses (`kafka1:29092`, etc.) for commands inside containers.
