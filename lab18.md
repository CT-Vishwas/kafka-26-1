# Lab: Explore consumer group parallel consumption

## Lab objectives

By the end of this lab, you will be able to:

- Configure a topic for parallel consumption using multiple partitions.
- Run multiple consumers in the same consumer group.
- Observe how partitions are distributed across consumers.
- Verify parallel processing behavior and lag reduction.
- Compare behavior when consumer count is less than, equal to, and greater than partition count.

## Prerequisites

- Docker Engine and Docker Compose
- Kafka cluster from [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml)

Run all commands from the repository root unless noted.

## Lab steps

### 1. Start the Kafka cluster

```bash
docker compose -f code/lab04/docker-compose.yml up -d
docker compose -f code/lab04/docker-compose.yml ps
```

Wait until all brokers are running.

### 2. Create a topic with higher partition count

Use 6 partitions so multiple consumers can process in parallel.

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic parallel-demo \
	--partitions 6 \
	--replication-factor 3
```

Verify:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--topic parallel-demo
```

### 3. Produce a larger message batch

Generate 300 keyed events:

```bash
for i in $(seq 1 300); do
	key=$((i % 12))
	echo "user-$key:event-$i"
done | docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic parallel-demo \
	--property parse.key=true \
	--property key.separator=:
```

### 4. Start one consumer (baseline)

Open Terminal A:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic parallel-demo \
	--group parallel-group \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

With one consumer, all 6 partitions are assigned to that single member.

### 5. Inspect group assignment and lag

From another terminal:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group parallel-group
```

Observe `PARTITION`, `CONSUMER-ID`, and `LAG`.

### 6. Add second consumer for parallelism

Open Terminal B and run the same consumer command from step 4.

Kafka rebalances and splits partitions across two consumers (roughly 3 + 3).

Re-check assignments:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group parallel-group
```

### 7. Add third consumer for more parallelism

Open Terminal C and run the same consumer command again.

Now assignments are typically 2 partitions per consumer.

Produce more data while three consumers are active:

```bash
for i in $(seq 301 600); do
	key=$((i % 12))
	echo "user-$key:event-$i"
done | docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic parallel-demo \
	--property parse.key=true \
	--property key.separator=:
```

### 8. Compare lag with active parallel consumers

Run:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group parallel-group
```

Observe how lag decreases as multiple members consume partitions concurrently.

### 9. Start extra consumers beyond partition count

Open Terminal D and Terminal E, each with the same command from step 4.

With 6 partitions:

- Up to 6 consumers can be active with partition assignments.
- Any additional consumers stay idle (no partition assigned).

Validate in group describe output by checking which consumers have assigned partitions.

### 10. Observe rebalance on consumer failure

Stop one active consumer (`Ctrl+C`) from Terminal B or C.

Kafka rebalances and reassigns that consumer's partitions to remaining members.

Check again:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group parallel-group
```

### 11. Stop all consumers and clean up

Stop all consumer terminals with `Ctrl+C`.

Delete the topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic parallel-demo
```

Stop the cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Consumer-group parallelism is bounded by partition count.
- One partition is consumed by one group member at a time.
- Increasing consumers up to partition count improves parallel consumption.
- Extra consumers beyond partition count remain idle.
- Rebalance events occur whenever group membership changes.
