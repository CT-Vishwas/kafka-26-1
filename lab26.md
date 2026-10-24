# Lab: Multiple listeners in Spring

## Lab objectives

By the end of this lab, you will be able to:

- Configure multiple `@KafkaListener` methods in a Spring Boot app.
- Process the same topic with different consumer groups.
- Verify independent consumption for each listener group.
- Observe parallel processing using listener concurrency.

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker Engine and Docker Compose
- Kafka cluster from [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml)

Run Docker commands from the repository root unless noted.

## Lab steps

### 1. Start the Kafka cluster

```bash
docker compose -f code/lab04/docker-compose.yml up -d
docker compose -f code/lab04/docker-compose.yml ps
```

## Part A: Create Spring Boot app


### Add application class

Create `src/main/java/com/example/kafka/multi/MultiListenerApplication.java`:

```java
package com.example.kafka.multi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MultiListenerApplication {
		public static void main(String[] args) {
				SpringApplication.run(MultiListenerApplication.class, args);
		}
}
```

### Add producer REST controller

Create `src/main/java/com/example/kafka/multi/OrderPublishController.java`:

```java
package com.example.kafka.multi;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderPublishController {

		private static final String TOPIC = "spring-multi-listener";
		private final KafkaTemplate<String, String> kafkaTemplate;

		public OrderPublishController(KafkaTemplate<String, String> kafkaTemplate) {
				this.kafkaTemplate = kafkaTemplate;
		}

		@PostMapping("/publish/{key}")
		public String publish(@PathVariable String key, @RequestBody String value) {
				kafkaTemplate.send(TOPIC, key, value);
				return "Published key=" + key;
		}
}
```

### Add multiple listeners

Create `src/main/java/com/example/kafka/multi/MultiGroupListeners.java`:

```java
package com.example.kafka.multi;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Component
public class MultiGroupListeners {

		// Listener A: Analytics group
		@KafkaListener(
						topics = "spring-multi-listener",
						groupId = "analytics-group",
						id = "analytics-listener")
		public void analyticsListener(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
						@Header(KafkaHeaders.OFFSET) long offset) {
				System.out.printf("[ANALYTICS] key=%s value=%s partition=%d offset=%d%n",
								key, value, partition, offset);
		}

		// Listener B: Billing group
		@KafkaListener(
						topics = "spring-multi-listener",
						groupId = "billing-group",
						id = "billing-listener")
		public void billingListener(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
						@Header(KafkaHeaders.OFFSET) long offset) {
				System.out.printf("[BILLING] key=%s value=%s partition=%d offset=%d%n",
								key, value, partition, offset);
		}
}
```

### Add listener concurrency configuration

Create `src/main/java/com/example/kafka/multi/KafkaListenerConfig.java`:

```java
package com.example.kafka.multi;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

@Configuration
public class KafkaListenerConfig {

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
						ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
						ConsumerFactory<Object, Object> consumerFactory) {
				ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
								new ConcurrentKafkaListenerContainerFactory<>();
				configurer.configure(factory, consumerFactory);

				// Up to 3 concurrent consumers per listener (bounded by partition count)
				factory.setConcurrency(3);
				return factory;
		}
}
```

### Add application configuration

Create `src/main/resources/application.yml`:

```yaml
server:
	port: 8082

spring:
	kafka:
		bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
		producer:
			acks: all
			key-serializer: org.apache.kafka.common.serialization.StringSerializer
			value-serializer: org.apache.kafka.common.serialization.StringSerializer
		consumer:
			auto-offset-reset: earliest
			key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
			value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

---

## Part B: Run and test

### 10. Run Spring Boot app

```bash
mvn spring-boot:run
```

### Publish test events

Open a new terminal and send events:

```bash
curl -X POST "http://localhost:8082/publish/ORD-2001" \
	-H "Content-Type: application/json" \
	-d '{"orderId":"ORD-2001","status":"CREATED"}'

curl -X POST "http://localhost:8082/publish/ORD-2002" \
	-H "Content-Type: application/json" \
	-d '{"orderId":"ORD-2002","status":"PAID"}'

curl -X POST "http://localhost:8082/publish/ORD-2003" \
	-H "Content-Type: application/json" \
	-d '{"orderId":"ORD-2003","status":"SHIPPED"}'
```

Observe application logs:

- `[ANALYTICS]` logs should appear.
- `[BILLING]` logs should also appear.

Both groups receive all records independently.

### Verify consumer groups via CLI

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group analytics-group

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group billing-group
```

### Optional: add second listener in same group

Add another method with `groupId = "analytics-group"` and different `id`, then restart app.

Expected behavior:

- records are shared between listeners in the same group,
- each record is still processed once per group.

### Clean up

Stop Spring app with `Ctrl+C`.

Delete topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic spring-multi-listener
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Multiple Spring listeners can model separate business pipelines.
- Different `group.id` values receive independent copies of the same topic events.
- Listener concurrency improves parallel consumption up to partition count.
- Consumer-group behavior in Spring maps directly to Kafka group semantics.
