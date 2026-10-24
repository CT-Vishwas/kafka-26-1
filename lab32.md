# Lab: Schema in Kafka (Schema Registry + Avro)

## Lab objectives

By the end of this lab, you will be able to:

- Understand why schemas are important in Kafka event contracts.
- Run Schema Registry with your Kafka cluster.
- Produce Avro messages and auto-register schema.
- Consume Avro messages using schema-aware deserialization.
- Validate schema versions and compatibility settings.

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

### 2. Create topic for Avro events

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic orders-avro \
	--partitions 3 \
	--replication-factor 3
```

### 3. Start Schema Registry container

```bash
docker run -d --name schema-registry-lab \
	--add-host=host.docker.internal:host-gateway \
	-p 8081:8081 \
	-e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
	-e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
	-e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:9092,PLAINTEXT://host.docker.internal:9093,PLAINTEXT://host.docker.internal:9094 \
	confluentinc/cp-schema-registry:7.7.1
```

Verify:

```bash
curl -s http://localhost:8081/subjects
```

Expected: `[]` (no schemas yet).

---

## Part A: Build Avro producer and consumer

### 4. Create Maven project

```bash
mvn archetype:generate \
	-DgroupId=com.example.kafka \
	-DartifactId=kafka-schema-lab \
	-DarchetypeArtifactId=maven-archetype-quickstart \
	-DarchetypeVersion=1.5 \
	-DinteractiveMode=false

cd kafka-schema-lab
```

### 5. Configure `pom.xml`

Replace generated `pom.xml` with:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
				 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
				 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>

	<groupId>com.example.kafka</groupId>
	<artifactId>kafka-schema-lab</artifactId>
	<version>1.0-SNAPSHOT</version>

	<properties>
		<maven.compiler.release>17</maven.compiler.release>
		<confluent.version>7.7.1</confluent.version>
	</properties>

	<repositories>
		<repository>
			<id>confluent</id>
			<url>https://packages.confluent.io/maven/</url>
		</repository>
	</repositories>

	<dependencies>
		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-clients</artifactId>
			<version>4.3.1</version>
		</dependency>
		<dependency>
			<groupId>io.confluent</groupId>
			<artifactId>kafka-avro-serializer</artifactId>
			<version>${confluent.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.avro</groupId>
			<artifactId>avro</artifactId>
			<version>1.11.4</version>
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

### 6. Add Avro producer

Create `src/main/java/com/example/kafka/AvroOrderProducer.java`:

```java
package com.example.kafka;

import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroOrderProducer {

		private static final String ORDER_SCHEMA = """
				{
					"type": "record",
					"name": "OrderEvent",
					"namespace": "com.example.kafka.avro",
					"fields": [
						{"name": "orderId", "type": "string"},
						{"name": "customer", "type": "string"},
						{"name": "amount", "type": "double"},
						{"name": "status", "type": "string"}
					]
				}
				""";

		public static void main(String[] args) throws Exception {
				Properties props = new Properties();
				props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
				props.put("schema.registry.url", "http://localhost:8081");
				props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
				props.put(ProducerConfig.ACKS_CONFIG, "all");

				Schema schema = new Schema.Parser().parse(ORDER_SCHEMA);

				try (KafkaProducer<String, Object> producer = new KafkaProducer<>(props)) {
						for (int i = 1; i <= 5; i++) {
								GenericRecord record = new GenericData.Record(schema);
								record.put("orderId", "ORD-" + i);
								record.put("customer", "CUST-" + i);
								record.put("amount", 1000.0 + i * 250);
								record.put("status", i % 2 == 0 ? "PAID" : "CREATED");

								ProducerRecord<String, Object> msg = new ProducerRecord<>("orders-avro", "ORD-" + i, record);
								var metadata = producer.send(msg).get();
								System.out.printf("Sent key=%s partition=%d offset=%d%n",
												"ORD-" + i, metadata.partition(), metadata.offset());
						}
				}
		}
}
```

### 7. Add Avro consumer

Create `src/main/java/com/example/kafka/AvroOrderConsumer.java`:

```java
package com.example.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class AvroOrderConsumer {
		public static void main(String[] args) {
				Properties props = new Properties();
				props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
				props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-avro-group");
				props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
				props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
				props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
				props.put("schema.registry.url", "http://localhost:8081");
				props.put("specific.avro.reader", "false");

				try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
						consumer.subscribe(Collections.singletonList("orders-avro"));

						long end = System.currentTimeMillis() + 15000;
						while (System.currentTimeMillis() < end) {
								ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
								for (ConsumerRecord<String, GenericRecord> r : records) {
										System.out.printf(
														"key=%s orderId=%s customer=%s amount=%s status=%s partition=%d offset=%d%n",
														r.key(),
														r.value().get("orderId"),
														r.value().get("customer"),
														r.value().get("amount"),
														r.value().get("status"),
														r.partition(),
														r.offset());
								}
						}
				}
		}
}
```

### 8. Build project

```bash
mvn clean package
```

### 9. Run producer (schema registration happens here)

```bash
mvn exec:java -Dexec.mainClass=com.example.kafka.AvroOrderProducer
```

### 10. Verify schema registration in Schema Registry

List subjects:

```bash
curl -s http://localhost:8081/subjects
```

Inspect latest schema:

```bash
curl -s http://localhost:8081/subjects/orders-avro-value/versions/latest
```

### 11. Run consumer

```bash
mvn exec:java -Dexec.mainClass=com.example.kafka.AvroOrderConsumer
```

You should see structured fields, not raw bytes.

---

## Part B: Compatibility and schema evolution

### 12. Check compatibility mode

```bash
curl -s http://localhost:8081/config/orders-avro-value
```

If no subject-level config exists, global default usually applies (`BACKWARD`).

### 13. Set explicit backward compatibility

```bash
curl -X PUT http://localhost:8081/config/orders-avro-value \
	-H "Content-Type: application/json" \
	-d '{"compatibility":"BACKWARD"}'
```

### 14. (Optional) Register evolved schema version

Register a new schema that adds optional field `currency` with default:

```bash
curl -X POST http://localhost:8081/subjects/orders-avro-value/versions \
	-H "Content-Type: application/vnd.schemaregistry.v1+json" \
	-d '{"schema":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"namespace\":\"com.example.kafka.avro\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"customer\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"INR\"}]}"}'
```

Then query versions:

```bash
curl -s http://localhost:8081/subjects/orders-avro-value/versions
```

---

## Cleanup

### 15. Stop containers and topic

```bash
docker rm -f schema-registry-lab

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete --topic orders-avro

docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Schemas define stable message contracts between producers and consumers.
- Schema Registry centralizes schema versions and compatibility checks.
- Avro with Schema Registry enables compact payloads and safer schema evolution.
- Compatibility settings prevent breaking changes in event-driven systems.
