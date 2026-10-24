# Lab: Implement asynchronous producer

## Lab objectives

By the end of this lab, you will be able to:

- Build a Java Kafka producer that sends records asynchronously.
- Use callback handlers to capture success/failure metadata.
- Tune producer settings for batching and reliability.
- Verify partition and offset results for async sends.

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

### 2. Create topic for async producer

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic async-orders \
	--partitions 3 \
	--replication-factor 3 \
	--config min.insync.replicas=2
```

Describe the topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--topic async-orders
```

### 3. Create Maven project

```bash
mvn archetype:generate \
	-DgroupId=com.example.kafka \
	-DartifactId=kafka-async-producer \
	-DarchetypeArtifactId=maven-archetype-quickstart \
	-DarchetypeVersion=1.5 \
	-DinteractiveMode=false

cd kafka-async-producer
```

### 4. Configure `pom.xml`

Replace the generated `pom.xml` content with:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
				 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
				 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>

	<groupId>com.example.kafka</groupId>
	<artifactId>kafka-async-producer</artifactId>
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

### 5. Implement asynchronous producer

Create file `src/main/java/com/example/kafka/AsyncOrderProducer.java`:

```java
package com.example.kafka;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AsyncOrderProducer {
		public static void main(String[] args) throws InterruptedException {
				final int messageCount = 20;
				final String topic = "async-orders";

				Properties props = new Properties();
				props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
								"localhost:9092,localhost:9093,localhost:9094");
				props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

				// Reliability + batching
				props.put(ProducerConfig.ACKS_CONFIG, "all");
				props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
				props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
				props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
				props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");

				CountDownLatch latch = new CountDownLatch(messageCount);

				try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
						for (int i = 1; i <= messageCount; i++) {
								String key = "ORD-" + i;
								String value = "{\"orderId\":\"" + key + "\",\"status\":\"CREATED\",\"ts\":\""
												+ Instant.now() + "\"}";

								ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

								// Asynchronous send with callback
								producer.send(record, (metadata, exception) -> {
										if (exception != null) {
												System.err.printf("FAILED key=%s error=%s%n", key, exception.getMessage());
										} else {
												System.out.printf(
																"SENT key=%s partition=%d offset=%d timestamp=%d%n",
																key, metadata.partition(), metadata.offset(), metadata.timestamp());
										}
										latch.countDown();
								});
						}

						// flush ensures pending records are sent before close
						producer.flush();
				}

				if (!latch.await(10, TimeUnit.SECONDS)) {
						System.err.println("Timeout waiting for callbacks.");
				}

				System.out.println("Async producer run complete.");
		}
}
```

### 6. Build and run

From the project directory (`kafka-async-producer`):

```bash
mvn clean package
mvn exec:java -Dexec.mainClass=com.example.kafka.AsyncOrderProducer
```

Expected output includes callback lines with partition and offset values.

### 7. Verify messages in Kafka

From repository root, consume records:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic async-orders \
	--from-beginning \
	--property print.key=true \
	--property print.partition=true \
	--property print.offset=true
```

Confirm all `ORD-*` events are present. Stop consumer with `Ctrl+C`.

### 8. Optional: observe async behavior under load

Increase `messageCount` to `1000`, run again, and observe:

- callback completion order may differ from send loop order,
- ordering per partition is preserved,
- throughput improves with batching (`linger.ms` + `batch.size`).

### 9. Clean up

Delete topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic async-orders
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- `send()` is non-blocking and returns immediately.
- Producer callbacks provide async success/failure and metadata.
- Async producers can achieve higher throughput than synchronous send patterns.
- `acks=all` with idempotence improves reliability while keeping asynchronous flow.
