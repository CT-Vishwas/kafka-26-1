# Lab: Implement synchronous producer

## Lab objectives

By the end of this lab, you will be able to:

- Build a Java Kafka producer that sends records synchronously.
- Use `send(...).get()` to wait for broker acknowledgment.
- Capture partition/offset metadata for each confirmed write.
- Understand trade-offs between synchronous and asynchronous publish patterns.

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker Engine and Docker Compose
- Kafka cluster from [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml)

Run Docker commands from the repository root unless noted.

## Lab steps

### 1. Start Kafka cluster

```bash
docker compose -f code/lab04/docker-compose.yml up -d
docker compose -f code/lab04/docker-compose.yml ps
```

Wait until `kafka1`, `kafka2`, and `kafka3` are running.

### 2. Create topic for synchronous producer

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic sync-orders \
	--partitions 3 \
	--replication-factor 3 \
	--config min.insync.replicas=2
```

Describe topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--topic sync-orders
```

### 3. Create Maven project

```bash
mvn archetype:generate \
	-DgroupId=com.example.kafka \
	-DartifactId=kafka-sync-producer \
	-DarchetypeArtifactId=maven-archetype-quickstart \
	-DarchetypeVersion=1.5 \
	-DinteractiveMode=false

cd kafka-sync-producer
```

### 4. Configure `pom.xml`

Replace generated `pom.xml` with:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
				 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
				 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>

	<groupId>com.example.kafka</groupId>
	<artifactId>kafka-sync-producer</artifactId>
	<version>1.0-SNAPSHOT</version>

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
</project>
```

### 5. Implement synchronous producer

Create `src/main/java/com/example/kafka/SyncOrderProducer.java`:

```java
package com.example.kafka;

import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SyncOrderProducer {
		public static void main(String[] args) {
				final String topic = "sync-orders";
				final int messageCount = 20;

				Properties props = new Properties();
				props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
								"localhost:9092,localhost:9093,localhost:9094");
				props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

				// Reliability-focused producer settings
				props.put(ProducerConfig.ACKS_CONFIG, "all");
				props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
				props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
				props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
				props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");

				try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
						for (int i = 1; i <= messageCount; i++) {
								String key = "ORD-" + i;
								String value = "{\"orderId\":\"" + key + "\",\"status\":\"CREATED\",\"ts\":\""
												+ Instant.now() + "\"}";

								ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

								// Synchronous send: wait for ack before sending next record
								var metadata = producer.send(record).get();

								System.out.printf(
												"SENT key=%s partition=%d offset=%d timestamp=%d%n",
												key,
												metadata.partition(),
												metadata.offset(),
												metadata.timestamp());
						}
				} catch (Exception e) {
						System.err.println("Producer failed: " + e.getMessage());
						e.printStackTrace();
				}

				System.out.println("Synchronous producer run complete.");
		}
}
```

### 6. Build and run

From `kafka-sync-producer` project directory:

```bash
mvn clean package
mvn exec:java -Dexec.mainClass=com.example.kafka.SyncOrderProducer
```

Expected behavior:

- each message waits for ack before next send,
- output appears in strict loop order,
- failures surface immediately at `get()`.

### 7. Verify records in Kafka

From repository root:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic sync-orders \
	--from-beginning \
	--property print.key=true \
	--property print.partition=true \
	--property print.offset=true
```

Confirm all records are present. Stop consumer with `Ctrl+C`.

### 8. Optional: compare with asynchronous producer

Run the asynchronous lab producer from [src/LABs/lab20.md](lab20.md) and compare:

- synchronous: lower throughput, immediate error propagation,
- asynchronous: higher throughput, callback-based completion.

### 9. Clean up

Delete topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic sync-orders
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Synchronous producer waits for broker acknowledgment on every send.
- `send(...).get()` gives deterministic success/failure handling.
- This pattern is simpler for strict control flow but usually slower than asynchronous send.
- `acks=all` and idempotence improve durability guarantees.
