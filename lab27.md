# Lab: Multiple producers and consumers in Spring consumer groups

## Lab objectives

By the end of this lab, you will be able to:

- Run multiple Spring Boot producer instances publishing to the same Kafka topic.
- Run multiple Spring Boot consumer instances in the same and different consumer groups.
- Observe load sharing within one group and independent consumption across groups.
- Verify partition assignment and lag using Kafka CLI tools.

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker Engine and Docker Compose
- Kafka cluster from [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml)

Run Docker commands from repository root unless noted.

## Lab steps

### 1. Start Kafka cluster

```bash
docker compose -f code/lab04/docker-compose.yml up -d
docker compose -f code/lab04/docker-compose.yml ps
```

### 2. Create topic for this lab

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic spring-scale-demo \
	--partitions 6 \
	--replication-factor 3
```

Verify:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--topic spring-scale-demo
```

---

## Part A: Spring producer application

### 3. Create producer project

```bash
mvn archetype:generate \
	-DgroupId=com.example.kafka \
	-DartifactId=spring-scale-producer \
	-DarchetypeArtifactId=maven-archetype-quickstart \
	-DarchetypeVersion=1.5 \
	-DinteractiveMode=false

cd spring-scale-producer
```

### 4. Configure producer `pom.xml`

Replace `pom.xml` with:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
				 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
				 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>

	<parent>
		<groupId>org.springframework.boot</groupId>
		<artifactId>spring-boot-starter-parent</artifactId>
		<version>3.3.5</version>
		<relativePath/>
	</parent>

	<groupId>com.example.kafka</groupId>
	<artifactId>spring-scale-producer</artifactId>
	<version>1.0.0</version>

	<properties>
		<java.version>17</java.version>
	</properties>

	<dependencies>
		<dependency>
			<groupId>org.springframework.boot</groupId>
			<artifactId>spring-boot-starter-web</artifactId>
		</dependency>
		<dependency>
			<groupId>org.springframework.kafka</groupId>
			<artifactId>spring-kafka</artifactId>
		</dependency>
	</dependencies>

	<build>
		<plugins>
			<plugin>
				<groupId>org.springframework.boot</groupId>
				<artifactId>spring-boot-maven-plugin</artifactId>
			</plugin>
		</plugins>
	</build>
</project>
```

### 5. Add producer application code

Create `src/main/java/com/example/kafka/producer/ScaleProducerApplication.java`:

```java
package com.example.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ScaleProducerApplication {
		public static void main(String[] args) {
				SpringApplication.run(ScaleProducerApplication.class, args);
		}
}
```

Create `src/main/java/com/example/kafka/producer/ProducerController.java`:

```java
package com.example.kafka.producer;

import java.time.Instant;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

		private static final String TOPIC = "spring-scale-demo";
		private final KafkaTemplate<String, String> kafkaTemplate;

		public ProducerController(KafkaTemplate<String, String> kafkaTemplate) {
				this.kafkaTemplate = kafkaTemplate;
		}

		@PostMapping("/produce/{producerId}")
		public String produce(
						@PathVariable String producerId,
						@RequestParam(defaultValue = "20") int count) {

				for (int i = 1; i <= count; i++) {
						String key = "cust-" + (i % 12);
						String value = "{\"producer\":\"" + producerId + "\",\"seq\":" + i
										+ ",\"ts\":\"" + Instant.now() + "\"}";
						kafkaTemplate.send(TOPIC, key, value);
				}

				return "Producer " + producerId + " published " + count + " events";
		}
}
```

Create `src/main/resources/application.yml`:

```yaml
server:
	port: 8083

spring:
	kafka:
		bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
		producer:
			acks: all
			key-serializer: org.apache.kafka.common.serialization.StringSerializer
			value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

### 6. Run producer instance 1

```bash
mvn spring-boot:run
```

Keep terminal running.

### 7. Run producer instance 2 (second terminal)

From same project folder, run with different HTTP port:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8084"
```

Now two producer instances are active: `8083` and `8084`.

---

## Part B: Spring consumer application

### 8. Create consumer project

Open new terminal:

```bash
mvn archetype:generate \
	-DgroupId=com.example.kafka \
	-DartifactId=spring-scale-consumer \
	-DarchetypeArtifactId=maven-archetype-quickstart \
	-DarchetypeVersion=1.5 \
	-DinteractiveMode=false

cd spring-scale-consumer
```

### 9. Configure consumer `pom.xml`

Use same dependency structure as producer, but keep `spring-kafka` + Spring Boot starter:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
				 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
				 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>

	<parent>
		<groupId>org.springframework.boot</groupId>
		<artifactId>spring-boot-starter-parent</artifactId>
		<version>3.3.5</version>
		<relativePath/>
	</parent>

	<groupId>com.example.kafka</groupId>
	<artifactId>spring-scale-consumer</artifactId>
	<version>1.0.0</version>

	<properties>
		<java.version>17</java.version>
	</properties>

	<dependencies>
		<dependency>
			<groupId>org.springframework.boot</groupId>
			<artifactId>spring-boot-starter</artifactId>
		</dependency>
		<dependency>
			<groupId>org.springframework.kafka</groupId>
			<artifactId>spring-kafka</artifactId>
		</dependency>
	</dependencies>

	<build>
		<plugins>
			<plugin>
				<groupId>org.springframework.boot</groupId>
				<artifactId>spring-boot-maven-plugin</artifactId>
			</plugin>
		</plugins>
	</build>
</project>
```

### 10. Add consumer application code

Create `src/main/java/com/example/kafka/consumer/ScaleConsumerApplication.java`:

```java
package com.example.kafka.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ScaleConsumerApplication {
		public static void main(String[] args) {
				SpringApplication.run(ScaleConsumerApplication.class, args);
		}
}
```

Create `src/main/java/com/example/kafka/consumer/GroupListeners.java`:

```java
package com.example.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class GroupListeners {

		// Same group listener (scaled by running multiple app instances)
		@KafkaListener(topics = "spring-scale-demo", groupId = "orders-processing-group", id = "proc-listener")
		public void processingGroup(String value,
																@Header(KafkaHeaders.RECEIVED_KEY) String key,
																@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
																@Header(KafkaHeaders.OFFSET) long offset) {
				System.out.printf("[PROCESSING] key=%s partition=%d offset=%d value=%s%n",
								key, partition, offset, value);
		}

		// Different group receives independent copy
		@KafkaListener(topics = "spring-scale-demo", groupId = "orders-audit-group", id = "audit-listener")
		public void auditGroup(String value,
													 @Header(KafkaHeaders.RECEIVED_KEY) String key,
													 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
													 @Header(KafkaHeaders.OFFSET) long offset) {
				System.out.printf("[AUDIT] key=%s partition=%d offset=%d value=%s%n",
								key, partition, offset, value);
		}
}
```

Create `src/main/resources/application.yml`:

```yaml
spring:
	kafka:
		bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
		consumer:
			auto-offset-reset: earliest
			key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
			value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

### 11. Run consumer instance 1

```bash
mvn spring-boot:run
```

### 12. Run consumer instance 2 (same code, second terminal)

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.application.name=consumer-2"
```

Behavior expected:

- For `orders-processing-group`, records are shared between instance 1 and 2.
- For `orders-audit-group`, each app instance is also in the same group, so records are shared across instances.

---

## Part C: Generate load and observe group behavior

### 13. Produce events from both producer instances

From a new terminal:

```bash
curl -X POST "http://localhost:8083/produce/P1?count=100"
curl -X POST "http://localhost:8084/produce/P2?count=100"
```

You now have two producer applications publishing concurrently.

### 14. Observe consumer logs

Check both consumer terminals:

- `[PROCESSING]` events should be distributed across consumer instances.
- `[AUDIT]` events should also be distributed inside that group.
- Offsets increase per partition.

### 15. Verify consumer group details in Kafka

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group orders-processing-group

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group orders-audit-group
```

Observe assignments and lag.

### 16. Simulate scale event

Stop one consumer instance with `Ctrl+C`, then publish more messages:

```bash
curl -X POST "http://localhost:8083/produce/P1?count=50"
```

Kafka rebalances and remaining consumer instance takes over all partitions for each group member present.

### 17. Optional: independent copy in a third group

Add another listener method with `groupId = "orders-notify-group"`, restart one consumer app, and publish again.

Expected: the new group also receives its own full logical copy of events.

### 18. Clean up

Stop all Spring app terminals (`Ctrl+C`).

Delete topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic spring-scale-demo
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Multiple producers can publish to the same topic concurrently.
- Multiple consumer instances in one group share partitions for scalability.
- Different consumer groups consume independently from the same topic.
- Group membership changes trigger rebalance and reassignment.
