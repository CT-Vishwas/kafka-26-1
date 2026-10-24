# Lab: Broker metrics (JMX), throughput testing, and message-rate monitoring

## Lab objectives

By the end of this lab, you will be able to:

- Enable JMX access for Kafka brokers in Docker Compose.
- Capture broker metrics such as `MessagesInPerSec`, `BytesInPerSec`, and `BytesOutPerSec`.
- Run throughput tests with Kafka performance tools.
- Correlate throughput test load with broker message-rate metrics.

## Prerequisites

- Docker Engine and Docker Compose
- Kafka cluster from [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml)

Run commands from the repository root unless noted.

## Lab steps

### 1. Enable JMX ports in Docker Compose

Open [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml) and add these environment values and port mappings:

- `kafka1`: `JMX_PORT=9991`, `KAFKA_JMX_HOSTNAME=kafka1`, port mapping `9991:9991`
- `kafka2`: `JMX_PORT=9992`, `KAFKA_JMX_HOSTNAME=kafka2`, port mapping `9992:9992`
- `kafka3`: `JMX_PORT=9993`, `KAFKA_JMX_HOSTNAME=kafka3`, port mapping `9993:9993`

Example for one broker section:

```yaml
ports:
	- "9092:9092"
	- "9991:9991"
environment:
	...
	JMX_PORT: 9991
	KAFKA_JMX_HOSTNAME: kafka1
```

### 2. Restart cluster with JMX enabled

```bash
docker compose -f code/lab04/docker-compose.yml down
docker compose -f code/lab04/docker-compose.yml up -d
docker compose -f code/lab04/docker-compose.yml ps
```

### 3. Create benchmark topic

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic metrics-demo \
	--partitions 6 \
	--replication-factor 3
```

### 4. Start JMX metrics watcher (MessagesInPerSec)

Open a new terminal and run:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool \
	--jmx-url service:jmx:rmi:///jndi/rmi://kafka1:9991/jmxrmi \
	--object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec \
	--attributes OneMinuteRate,FiveMinuteRate,Count \
	--reporting-interval 2000
```

Keep this running during test load.

### 5. Start JMX metrics watcher (Bytes In/Out)

Open another terminal and run:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool \
	--jmx-url service:jmx:rmi:///jndi/rmi://kafka1:9991/jmxrmi \
	--object-name kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec \
	--attributes OneMinuteRate,Count \
	--reporting-interval 2000
```

In a third terminal:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool \
	--jmx-url service:jmx:rmi:///jndi/rmi://kafka1:9991/jmxrmi \
	--object-name kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec \
	--attributes OneMinuteRate,Count \
	--reporting-interval 2000
```

### 6. Throughput test (producer load)

Run producer benchmark:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-producer-perf-test.sh \
	--topic metrics-demo \
	--num-records 400000 \
	--record-size 256 \
	--throughput -1 \
	--producer-props bootstrap.servers=kafka1:29092,kafka2:29092,kafka3:29092 acks=all linger.ms=10 batch.size=32768 compression.type=snappy
```

Capture producer output:

- records/sec
- MB/sec
- average latency
- max latency

### 7. Message-rate monitoring during load

While step 6 runs, watch `MessagesInPerSec` output from step 4:

- `OneMinuteRate` should rise during publish load.
- `Count` should increase continuously.

Cross-check expected relationship:

$$
	ext{Producer records/sec} \approx \sum \text{broker MessagesInPerSec rates (topic scope dependent)}
$$

Because load is partitioned and replicated, exact equality is not expected at a single broker view.

### 8. Consumer-side throughput test

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-perf-test.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic metrics-demo \
	--messages 200000 \
	--group metrics-consumer-group \
	--timeout 120000 \
	--show-detailed-stats
```

Now watch `BytesOutPerSec` from step 5 to see read traffic impact.

### 9. Check lag and consumption progress

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group metrics-consumer-group
```

Observe `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG`.

### 10. Optional: compare two producer profiles

Run again with low-batch profile:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-producer-perf-test.sh \
	--topic metrics-demo \
	--num-records 200000 \
	--record-size 256 \
	--throughput -1 \
	--producer-props bootstrap.servers=kafka1:29092,kafka2:29092,kafka3:29092 acks=1 linger.ms=0 batch.size=16384 compression.type=none
```

Compare changes in:

- producer throughput,
- `MessagesInPerSec` trend,
- latency metrics.

### 11. Record summary

Create a table in your notes:

| Scenario | Producer settings | Producer records/sec | Avg latency (ms) | MessagesInPerSec (1m rate) | BytesInPerSec (1m rate) | BytesOutPerSec (1m rate) |
|---|---|---:|---:|---:|---:|---:|
| Profile A | `acks=all`, snappy, linger=10 |  |  |  |  |  |
| Profile B | `acks=1`, no compression, linger=0 |  |  |  |  |  |

### 12. Clean up

Stop JMX watcher terminals (`Ctrl+C`), then:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic metrics-demo

docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- JMX exposes broker internals needed for operational monitoring.
- `MessagesInPerSec` is a core metric for write rate monitoring.
- `BytesInPerSec` and `BytesOutPerSec` help estimate network and broker IO pressure.
- Throughput tests are meaningful only when correlated with broker-side metrics.
