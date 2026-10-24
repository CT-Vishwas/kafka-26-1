# Lab: Retry implementation in Spring

## Lab objectives

By the end of this lab, you will be able to:

- Implement retry logic for Kafka consumers in Spring Boot.
- Configure retry attempts and backoff interval.
- Route permanently failing messages to a DLT.
- Verify retry and DLT behavior end-to-end.

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

### 2. Create input topic
Go To Kafka Folder

```bash
bin/kafka-topics.sh \
	--bootstrap-server localhost:9092 \
	--create \
	--topic retry-orders \
	--partitions 3 \
	--replication-factor 3
```

Spring will auto-create retry topics and DLT topics when the listener starts.

---

## Part A: Create Spring Boot app

### 3. Create Maven project

### 5. Add application class

Create `src/main/java/com/example/kafka/retry/RetryLabApplication.java`:

```java
package com.example.kafka.retry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RetryLabApplication {
		public static void main(String[] args) {
				SpringApplication.run(RetryLabApplication.class, args);
		}
}
```

### 6. Add producer endpoint for test events

Create `src/main/java/com/example/kafka/retry/OrderPublishController.java`:

```java
package com.example.kafka.retry;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderPublishController {

		private final KafkaTemplate<String, String> kafkaTemplate;

		public OrderPublishController(KafkaTemplate<String, String> kafkaTemplate) {
				this.kafkaTemplate = kafkaTemplate;
		}

		@PostMapping("/orders/{key}")
		public String publish(@PathVariable String key, @RequestBody String payload) {
				kafkaTemplate.send("retry-orders", key, payload);
				return "Published key=" + key;
		}
}
```

### 7. Implement retryable Kafka listener

Create `src/main/java/com/example/kafka/retry/RetryOrderListener.java`:

```java
package com.example.kafka.retry;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Component
public class RetryOrderListener {

		@RetryableTopic(
						attempts = "4",                // 1 initial + 3 retries
						backoff = @org.springframework.retry.annotation.Backoff(delay = 2000, multiplier = 2.0),
						topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
						autoCreateTopics = "true",
						dltTopicSuffix = "-dlt")
		@KafkaListener(topics = "retry-orders", groupId = "retry-orders-group")
		public void consume(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						@Header(KafkaHeaders.OFFSET) long offset) {

				System.out.printf("[CONSUME] topic=%s key=%s offset=%d value=%s%n",
								topic, key, offset, value);

				// Simulate transient/permanent failures using payload markers
				if (value.contains("FAIL")) {
						throw new IllegalStateException("Simulated processing failure for value=" + value);
				}

				System.out.printf("[SUCCESS] key=%s processed%n", key);
		}

		@DltHandler
		public void handleDlt(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						@Header(KafkaHeaders.OFFSET) long offset) {
				System.out.printf("[DLT] topic=%s key=%s offset=%d value=%s%n",
								topic, key, offset, value);
		}
}
```

### 8. Add Spring configuration

Create `src/main/resources/application.yml`:

```yaml
server:
	port: 8085

spring:
	kafka:
		bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
		consumer:
			auto-offset-reset: earliest
			key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
			value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
		producer:
			key-serializer: org.apache.kafka.common.serialization.StringSerializer
			value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

### 9. Run the application

```bash
mvn spring-boot:run
```

---

## Part B: Test retry flow

### 10. Send one successful record

```bash
curl -X POST "http://localhost:8085/orders/ORD-3001" \
	-H "Content-Type: application/json" \
	-d '{"orderId":"ORD-3001","status":"OK"}'
```

Observe immediate `[SUCCESS]` log.

### 11. Send failing record to trigger retries

```bash
curl -X POST "http://localhost:8085/orders/ORD-3002" \
	-H "Content-Type: application/json" \
	-d '{"orderId":"ORD-3002","status":"FAIL"}'
```

Observe logs:

- initial consume on `retry-orders`
- retries on `retry-orders-retry-0`, `retry-orders-retry-1`, ...
- final handoff to `retry-orders-dlt`

### 12. Verify retry and DLT topics exist

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--list | grep retry-orders
```

### 13. Inspect DLT records via CLI

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic retry-orders-dlt \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

You should see the failed `ORD-3002` payload.

### 14. Optional: test transient recovery

Change listener logic to fail only when payload contains `FAIL-PERM`, then send payload that fails once and succeeds on retry.

Expected: message succeeds before reaching DLT.

### 15. Clean up

Stop app with `Ctrl+C`.

Delete all retry-related topics:

```bash
bin/kafka-topics.sh \
	--bootstrap-server localhost:9092 \
	--delete --topic retry-orders

bin/kafka-topics.sh \
	--bootstrap-server localhost:9092 \
	--list | grep retry-orders | xargs -r -I{} /opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:9092 --delete --topic {}
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Spring Kafka supports declarative retry using `@RetryableTopic`.
- Backoff controls retry delay growth (for example exponential retry delay).
- After retry attempts are exhausted, events can be routed to DLT for later handling.
- Retry/DLT makes consumer processing resilient without losing failed records.
