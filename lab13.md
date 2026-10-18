# Lab: Implement a Custom Kafka Serializer

## Prerequisites

- Java 17+
- Maven 3.8+
- A Kafka cluster from [`lab04/docker-compose.yml`](./lab04/docker-compose.yml)
- The `orders` topic

Run Docker Compose commands from the repository root.

## Lab steps

### 1. Start Kafka and verify the topic

```bash
docker compose -f lab04/docker-compose.yml up -d
```

Check that `orders` exists. If needed, create it as shown in [lab12](./lab12.md).

### 2. Create a Maven project

```bash
mvn archetype:generate \
  -DgroupId=com.example.kafka \
  -DartifactId=kafka-custom-serializer \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DarchetypeVersion=1.5 \
  -DinteractiveMode=false

cd kafka-custom-serializer
```

In `pom.xml`, set Java 17 and add the Kafka client, Jackson, and exec plugin:

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
  <dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.17.2</version>
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

### 3. Define the order value type

Create `src/main/java/com/example/kafka/Order.java`:

```java
package com.example.kafka;

public record Order(String orderId, String customer, String status, double total) {}
```

### 4. Implement the custom serializer

Create `src/main/java/com/example/kafka/OrderJsonSerializer.java`:

```java
package com.example.kafka;

import java.util.Map;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class OrderJsonSerializer implements Serializer<Order> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Order order) {
        if (order == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(order);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize order for topic " + topic, e);
        }
    }
}
```

### 5. Write a producer that uses the serializer

Create `src/main/java/com/example/kafka/OrderProducer.java`:

```java
package com.example.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class OrderProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            OrderJsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(props)) {
            Order order = new Order("ORD-301", "C-301", "CREATED", 79.99);
            ProducerRecord<String, Order> record =
                new ProducerRecord<>("orders", order.orderId(), order);

            var metadata = producer.send(record).get();
            System.out.printf(
                "Sent key=%s partition=%d offset=%d%n",
                order.orderId(), metadata.partition(), metadata.offset()
            );
        }
    }
}
```

### 6. Build and publish a record

From the Maven project directory:

```bash
mvn clean package
mvn exec:java -Dexec.mainClass=com.example.kafka.OrderProducer
```

The producer should print the key, partition, and offset. Serialization errors fail the send and are reported by the producer.

### 7. Verify the serialized value

From the repository root, consume the record:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property key.separator=:
```

Find `ORD-301` and confirm its value is JSON, for example:

```text
ORD-301:{"orderId":"ORD-301","customer":"C-301","status":"CREATED","total":79.99}
```

Stop the consumer with `Ctrl+C`.

### 8. Stop the cluster

From the repository root:

```bash
docker compose -f lab04/docker-compose.yml down
```

## Key points

- Kafka serializers convert application objects into byte arrays before publishing.
- `OrderJsonSerializer` uses Jackson to encode an `Order` as JSON bytes.
- The producer's generic value type and configured value serializer must agree.
- A null value is passed through as `null` bytes; serialization errors are raised as `SerializationException`.
- A custom serializer is producer-side logic. Consumers still need a compatible deserializer or must interpret the JSON bytes themselves.
- Use the same JSON field names and types in producers and consumers to keep the message contract consistent.
