# Lab: Spring Boot producer and consumer

## Lab objectives

By the end of this lab, you will be able to:

- Build a Spring Boot Kafka producer service.
- Build a Spring Boot Kafka consumer service.
- Publish messages via REST and consume them with `@KafkaListener`.
- Verify end-to-end message flow with Kafka CLI.

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

### 2. Create topic for Spring apps

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic spring-orders \
	--partitions 3 \
	--replication-factor 3
```

---

## Part A: Spring Boot Producer App

### 3. Add producer application code

Create `src/main/java/com/example/kafka/producer/ProducerApplication.java`:

```java
package com.example.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerApplication {
		public static void main(String[] args) {
				SpringApplication.run(ProducerApplication.class, args);
		}
}
```

Create `src/main/java/com/example/kafka/producer/OrderProducerController.java`:

```java
package com.example.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderProducerController {

		private final KafkaTemplate<String, String> kafkaTemplate;
		private static final String TOPIC = "spring-orders";

		public OrderProducerController(KafkaTemplate<String, String> kafkaTemplate) {
				this.kafkaTemplate = kafkaTemplate;
		}

		@PostMapping("/orders/{key}")
		public String send(@PathVariable String key, @RequestBody String payload) {
				kafkaTemplate.send(TOPIC, key, payload);
				return "Published key=" + key;
		}
}
```

Create `src/main/resources/application.yml`:

```yaml
server:
	port: 8081

spring:
	kafka:
		bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
		producer:
			acks: all
			key-serializer: org.apache.kafka.common.serialization.StringSerializer
			value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

### 4. Run producer app

```bash
mvn spring-boot:run
```

Keep this terminal running.

---

## Part B: Spring Boot Consumer App

### 5. Add consumer application code

Create `src/main/java/com/example/kafka/consumer/ConsumerApplication.java`:

```java
package com.example.kafka.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerApplication {
		public static void main(String[] args) {
				SpringApplication.run(ConsumerApplication.class, args);
		}
}
```

Create `src/main/java/com/example/kafka/consumer/OrderConsumerListener.java`:

```java
package com.example.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumerListener {

		@KafkaListener(topics = "spring-orders", groupId = "spring-orders-group")
		public void consume(String value) {
				System.out.println("Consumed => " + value);
		}
}
```

Create `src/main/resources/application.yml`:

```yaml
spring:
	kafka:
		bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
		consumer:
			group-id: spring-orders-group
			auto-offset-reset: earliest
			enable-auto-commit: true
			key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
			value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

### 6. Run consumer app

```bash
mvn spring-boot:run
```

Keep this terminal running.

---

## End-to-end test

### 7. Publish orders through producer REST endpoint

In a new terminal:

```bash
curl -X POST "http://localhost:8081/orders/ORD-1001" \
	-H "Content-Type: application/json" \
	-d '{"orderId":"ORD-1001","status":"CREATED","amount":1200}'

curl -X POST "http://localhost:8081/orders/ORD-1002" \
	-H "Content-Type: application/json" \
	-d '{"orderId":"ORD-1002","status":"PAID","amount":2200}'
```

Verify consumer terminal prints both messages.

### 8. Verify using Kafka CLI (optional)

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic spring-orders \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

### 13. Clean up

Stop Spring apps with `Ctrl+C`.

Delete topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic spring-orders
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Spring Boot simplifies Kafka integration via `KafkaTemplate` and `@KafkaListener`.
- Producer and consumer apps can be developed and deployed independently.
- Consumer groups (`group.id`) control message consumption semantics.
- REST + Kafka is a common entry pattern for event-driven microservices.
