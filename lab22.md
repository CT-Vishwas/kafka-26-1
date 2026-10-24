# Lab: Manual offset commit

## Lab objectives

By the end of this lab, you will be able to:

- Configure a Kafka consumer with auto-commit disabled.
- Commit offsets manually after successful processing.
- Observe replay behavior when a record is read but not committed.
- Verify committed offsets using Kafka CLI tools.

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

### 2. Create topic for commit testing

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic commit-demo \
	--partitions 3 \
	--replication-factor 3
```

### 3. Create Maven project

```bash
mvn archetype:generate \
	-DgroupId=com.example.kafka \
	-DartifactId=kafka-manual-commit-consumer \
	-DarchetypeArtifactId=maven-archetype-quickstart \
	-DarchetypeVersion=1.5 \
	-DinteractiveMode=false

cd kafka-manual-commit-consumer
```

### 4. Configure `pom.xml`

Replace generated `pom.xml` with:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
				 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
				 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>

	<groupId>com.example.kafka</groupId>
	<artifactId>kafka-manual-commit-consumer</artifactId>
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

### 5. Produce normal records

Open a second terminal from repository root:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic commit-demo \
	--property parse.key=true \
	--property key.separator=:
```

Send:

```text
o1:OK-1
o2:OK-2
o3:OK-3
```

Observe consumer output and `Committed offsets for current batch.`

### 6. Implement consumer with manual commit

Create `src/main/java/com/example/kafka/ManualCommitConsumer.java`:

```java
package com.example.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ManualCommitConsumer {
		public static void main(String[] args) {
				Properties props = new Properties();
				props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
								"localhost:9092,localhost:9093,localhost:9094");
				props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-commit-group");
				props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
				props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
				props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

				// IMPORTANT: disable auto commit
				props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

				try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
						consumer.subscribe(Collections.singletonList("commit-demo"));

						while (true) {
								ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

								for (ConsumerRecord<String, String> record : records) {
										System.out.printf("PROCESS key=%s value=%s partition=%d offset=%d%n",
														record.key(), record.value(), record.partition(), record.offset());

										// Simulate processing failure when value contains FAIL
										if (record.value() != null && record.value().contains("FAIL")) {
												System.err.printf("Simulated failure at offset=%d. Not committing.%n",
																record.offset());
												throw new RuntimeException("Simulated processing error");
										}
								}

								if (!records.isEmpty()) {
										consumer.commitSync();
										System.out.println("Committed offsets for current batch.");
								}
						}
				}
		}
}
```

### 7. Build and run consumer

From project directory:

```bash
mvn clean package
mvn exec:java -Dexec.mainClass=com.example.kafka.ManualCommitConsumer
```

Keep this terminal running.



### 8. Verify committed offsets in the group

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group manual-commit-group
```

Record current `CURRENT-OFFSET` values.

### 9. Simulate processing failure (no commit)

Produce a failing record:

```bash
docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic commit-demo \
	--property parse.key=true \
	--property key.separator=: <<'EOF'
o4:FAIL-RETRY
EOF
```

Consumer should crash on simulated failure before commit.

### 10. Restart consumer and observe replay

Restart consumer using the same command from step 6:

```bash
mvn exec:java -Dexec.mainClass=com.example.kafka.ManualCommitConsumer
```

Expected behavior:

- the failed record is read again,
- this demonstrates at-least-once behavior with manual commits.

### 11. Recover and commit successfully

Stop consumer, edit code to remove failure simulation block, rebuild, and run again.

Then verify offsets advanced:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group manual-commit-group
```

### 12. Clean up

Delete topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic commit-demo
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- With manual commit, you control when offsets are marked as processed.
- Commit only after successful business processing.
- If a failure happens before commit, records are replayed after restart.
- This gives at-least-once delivery and improves processing safety.
