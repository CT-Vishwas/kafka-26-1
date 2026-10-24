# Lab: Performance benchmarking exercise

## Lab objectives

By the end of this lab, you will be able to:

- Run Kafka producer and consumer benchmark tools.
- Measure throughput, latency, and consumer fetch rate.
- Compare baseline vs tuned producer configurations.
- Understand how partitions and producer settings affect performance.

## Prerequisites

- Docker Engine and Docker Compose
- Kafka cluster from [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml)

Run commands from the repository root unless noted.

## Lab steps

### 1. Start the Kafka cluster

```bash
docker compose -f code/lab04/docker-compose.yml up -d
docker compose -f code/lab04/docker-compose.yml ps
```

Wait until all brokers are healthy.

### 2. Create benchmark topic

Use multiple partitions to allow parallel write/read.

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic perf-demo \
	--partitions 6 \
	--replication-factor 3
```

Verify:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--topic perf-demo
```

### 3. Producer benchmark (baseline)

Run a baseline test with fixed-size records.

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-producer-perf-test.sh \
	--topic perf-demo \
	--num-records 500000 \
	--record-size 200 \
	--throughput -1 \
	--producer-props bootstrap.servers=kafka1:29092,kafka2:29092,kafka3:29092 acks=1 linger.ms=0 batch.size=16384 compression.type=none
```

Capture from output:

- records/sec
- MB/sec
- avg latency
- max latency

### 4. Producer benchmark (reliability profile)

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-producer-perf-test.sh \
	--topic perf-demo \
	--num-records 500000 \
	--record-size 200 \
	--throughput -1 \
	--producer-props bootstrap.servers=kafka1:29092,kafka2:29092,kafka3:29092 acks=all enable.idempotence=true linger.ms=20 batch.size=32768 compression.type=snappy
```

Compare with baseline:

- throughput changes,
- latency changes,
- reliability guarantees (`acks=all`, idempotence).

### 5. Optional: rate-limited producer benchmark

Simulate controlled ingress with throughput cap.

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-producer-perf-test.sh \
	--topic perf-demo \
	--num-records 200000 \
	--record-size 200 \
	--throughput 20000 \
	--producer-props bootstrap.servers=kafka1:29092,kafka2:29092,kafka3:29092 acks=all linger.ms=10 batch.size=32768 compression.type=lz4
```

### 6. Consumer benchmark

Run consumer performance test against data in `perf-demo`.

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-perf-test.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic perf-demo \
	--messages 300000 \
	--group perf-consumer-group \
	--timeout 120000 \
	--show-detailed-stats
```

Capture:

- MB/sec consumed
- records/sec consumed
- total fetch time metrics

### 7. Increase consumer parallelism and compare

Start two console consumers in same group (two terminals) and then produce new data:

Terminal A:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic perf-demo \
	--group perf-live-group
```

Terminal B: run same command.

Check group assignment:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group perf-live-group
```

Observe partition split and lag behavior.

### 8. Capture benchmark summary table

Create a small comparison table in your notes:

| Scenario | acks | compression | linger.ms | batch.size | records/sec | avg latency | max latency |
|---|---|---|---:|---:|---:|---:|---:|
| Baseline | 1 | none | 0 | 16384 |  |  |  |
| Reliable+batched | all | snappy | 20 | 32768 |  |  |  |
| Rate-limited | all | lz4 | 10 | 32768 |  |  |  |

### 9. Clean up

Delete topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic perf-demo
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Benchmarking must include both throughput and latency.
- Producer settings (`acks`, `linger.ms`, `batch.size`, compression) change performance characteristics.
- More partitions enable better parallelism for consumers and producers.
- Reliability-focused settings can reduce raw throughput but improve delivery guarantees.
