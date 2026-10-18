# Lab: Java Kafka Consumer with Consumer Groups

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker Engine and Docker Compose

This lab uses the three-broker cluster in [`lab04/docker-compose.yml`](./lab04/docker-compose.yml). Run Docker Compose commands from the repository root.

## Lab steps

### 1. Start Kafka and prepare the topic

```bash
docker compose -f lab04/docker-compose.yml up -d
```

Create `orders` if it does not already exist:

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

If it already exists, continue to the next step. To check the topic:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --describe \
  --topic orders
```

### 2. Create a Maven consumer project

```bash
mvn archetype:generate \
  -DgroupId=com.example.kafka \
  -DartifactId=kafka-java-consumer \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DarchetypeVersion=1.5 \
  -DinteractiveMode=false

cd kafka-java-consumer
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

### 3. Write the consumer

Create `src/main/java/com/example/kafka/OrderConsumer.java`:

```java
package com.example.kafka;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class OrderConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "orders-java-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.List.of("orders"));

            while (true) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

                for (var record : records) {
                    System.out.printf(
                        "key=%s value=%s partition=%d offset=%d%n",
                        record.key(), record.value(),
                        record.partition(), record.offset()
                    );
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }
}
```

### 4. Build and start the consumer

```bash
mvn clean package
mvn exec:java -Dexec.mainClass=com.example.kafka.OrderConsumer
```

The consumer keeps running and polls Kafka for new records.

### 5. Produce test records

In a second terminal, from the repository root, start a console producer:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

Enter a few records:

```text
ORD-101:{"orderId":"ORD-101","product":"Laptop"}
ORD-102:{"orderId":"ORD-102","product":"Keyboard"}
```

Verify that the Java consumer prints each record's key, value, partition, and offset. Stop it with `Ctrl+C`.

### 6. Observe consumer group behavior

Run two copies of `OrderConsumer` with the same `group.id`. Kafka assigns partitions among members of the group; each record is processed by only one member in that group. With three partitions, up to three consumers can receive assignments.

To see group membership and assignments, from the repository root run:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --describe \
  --group orders-java-consumer
```

### 7. Stop the cluster

From the repository root:

```bash
docker compose -f lab04/docker-compose.yml down
```

## Key points

- `group.id` identifies a consumer group; Kafka stores offsets separately for each group.
- `subscribe()` lets Kafka assign partitions and rebalance them as group membership changes.
- `auto.offset.reset=earliest` is used only when the group has no committed offset (or its offset is invalid); otherwise consumption resumes from the committed position.
- With auto-commit disabled, call `commitSync()` after processing records to commit progress.
- Committing after processing gives at-least-once behavior: a crash before commit can cause records to be read again.
- Consumers in the same group share partitions; separate groups each receive their own copy of the topic's records.
- Use host listener addresses (`localhost:9092,9093,9094`) for Java on the host and Docker listener addresses (`kafka1:29092`, etc.) for commands inside containers.
