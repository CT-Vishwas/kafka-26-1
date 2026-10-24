# Lab: Kafka Streams — word count, aggregation, join, windowing, and filtering

## Lab objectives

By the end of this lab, you will be able to:

- Build and run a Kafka Streams Java application.
- Implement classic word count.
- Implement grouping and aggregation.
- Join two streams using keys and event-time windows.
- Apply filtering rules to stream data.
- Observe output topics for each stream operation.

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

### 2. Create input and output topics

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --topic text-lines --partitions 3 --replication-factor 3

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --topic orders-stream --partitions 3 --replication-factor 3

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --topic payments-stream --partitions 3 --replication-factor 3

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --topic wordcount-out --partitions 3 --replication-factor 3

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --topic order-total-out --partitions 3 --replication-factor 3

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --topic order-payment-join-out --partitions 3 --replication-factor 3

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --topic order-window-count-out --partitions 3 --replication-factor 3

docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create --topic high-value-orders-out --partitions 3 --replication-factor 3
```

---

## Part A: Create Kafka Streams project

### 3. Create Maven project

```bash
mvn archetype:generate \
	-DgroupId=com.example.kafka \
	-DartifactId=kafka-streams-lab \
	-DarchetypeArtifactId=maven-archetype-quickstart \
	-DarchetypeVersion=1.5 \
	-DinteractiveMode=false

cd kafka-streams-lab
```

### 4. Configure `pom.xml`

Replace generated `pom.xml` with:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
				 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
				 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>

	<groupId>com.example.kafka</groupId>
	<artifactId>kafka-streams-lab</artifactId>
	<version>1.0-SNAPSHOT</version>

	<properties>
		<maven.compiler.release>17</maven.compiler.release>
	</properties>

	<dependencies>
		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-streams</artifactId>
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

### 5. Add Streams application code

Create `src/main/java/com/example/kafka/StreamsLabApp.java`:

```java
package com.example.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Produced;

public class StreamsLabApp {
		public static void main(String[] args) {
				Properties props = new Properties();
				props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-lab-app");
				props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
				props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
				props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

				StreamsBuilder builder = new StreamsBuilder();

				// 1) WORD COUNT: text-lines -> wordcount-out
				KStream<String, String> textLines = builder.stream("text-lines");
				textLines
								.flatMapValues(v -> Arrays.asList(v.toLowerCase(Locale.ROOT).split("\\W+")))
								.filter((k, word) -> word != null && !word.isBlank())
								.groupBy((k, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
								.count(Materialized.as("wordcount-store"))
								.toStream()
								.map((word, count) -> new org.apache.kafka.streams.KeyValue<>(word, count.toString()))
								.to("wordcount-out", Produced.with(Serdes.String(), Serdes.String()));

				// 2) FILTER + AGGREGATION: orders-stream -> high-value-orders-out + order-total-out
				KStream<String, String> orders = builder.stream("orders-stream");

				// Keep only high-value orders (amount >= 1000) assuming key=orderId and value=amount as string
				orders.filter((orderId, amountStr) -> {
						try {
								return Double.parseDouble(amountStr) >= 1000;
						} catch (Exception e) {
								return false;
						}
				}).to("high-value-orders-out", Produced.with(Serdes.String(), Serdes.String()));

				// Aggregate total order amount by a synthetic group key (first char of orderId)
				KGroupedStream<String, String> groupedOrders = orders.groupBy(
								(orderId, amount) -> orderId.substring(0, 1),
								Grouped.with(Serdes.String(), Serdes.String()));

				groupedOrders.aggregate(
												() -> 0.0,
												(grp, amount, total) -> {
														try {
																return total + Double.parseDouble(amount);
														} catch (Exception e) {
																return total;
														}
												},
												Materialized.with(Serdes.String(), Serdes.Double()))
								.toStream()
								.map((k, total) -> new org.apache.kafka.streams.KeyValue<>(k, String.format("%.2f", total)))
								.to("order-total-out", Produced.with(Serdes.String(), Serdes.String()));

				// 3) JOIN: orders-stream join payments-stream by orderId within 2 minutes
				KStream<String, String> payments = builder.stream("payments-stream");

				orders.join(
												payments,
												(orderAmount, paymentStatus) -> "amount=" + orderAmount + ",payment=" + paymentStatus,
												JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(2)))
								.to("order-payment-join-out", Produced.with(Serdes.String(), Serdes.String()));

				// 4) WINDOWING: count events per key per 30-second window
				orders.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
								.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
								.count(Materialized.with(Serdes.String(), Serdes.Long()))
								.toStream()
								.map((Windowed<String> wk, Long count) ->
												new org.apache.kafka.streams.KeyValue<>(
																wk.key(),
																"windowStart=" + wk.window().startTime() + ",count=" + count))
								.to("order-window-count-out", Produced.with(Serdes.String(), Serdes.String()));

				KafkaStreams streams = new KafkaStreams(builder.build(), props);
				streams.start();

				Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		}
}
```

### 6. Build and run Streams app

```bash
mvn clean package
mvn exec:java -Dexec.mainClass=com.example.kafka.StreamsLabApp
```

Keep this app running.

---

## Part B: Test each operation

### 7. Word count test

Produce text lines:

```bash
docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic text-lines <<'EOF'
Kafka streams is powerful
Kafka streams supports stateful processing
Kafka kafka kafka
EOF
```

Consume results:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic wordcount-out \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

### 8. Filtering + aggregation test

Produce order amounts (key = orderId, value = amount):

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic orders-stream \
	--property parse.key=true \
	--property key.separator=:
```

Enter:

```text
A100:500
A101:1200
B200:700
B201:1800
A102:2000
```

Check filtered high-value output:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic high-value-orders-out \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

Check aggregated totals:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic order-total-out \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

### 9. Join test

Publish payments using same `orderId` keys:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic payments-stream \
	--property parse.key=true \
	--property key.separator=:
```

Enter:

```text
A101:PAID
A102:FAILED
B201:PAID
```

Consume join output:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic order-payment-join-out \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

### 10. Windowing test

Publish repeated order keys quickly:

```bash
docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic orders-stream \
	--property parse.key=true \
	--property key.separator=: <<'EOF'
A101:100
A101:200
A101:300
B201:400
B201:500
EOF
```

Consume window-count output:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic order-window-count-out \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

You should see per-key counts with window start time.

### 11. Optional topology observation

List internal topics created by Kafka Streams:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--list | grep streams-lab-app
```

Observe changelog/repartition topics used for stateful operations.

---

## Cleanup

### 12. Stop app and remove topics

Stop Streams app with `Ctrl+C`, then run:

```bash
for t in text-lines orders-stream payments-stream wordcount-out order-total-out order-payment-join-out order-window-count-out high-value-orders-out; do
	docker compose -f code/lab04/docker-compose.yml exec kafka1 \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 --delete --topic "$t"
done

docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Word count demonstrates stateful stream processing fundamentals.
- Aggregation computes continuously updated metrics from events.
- Joins correlate events from separate streams using key + time.
- Windowing adds time boundaries to stream analytics.
- Filtering creates targeted derived streams for downstream consumers.
