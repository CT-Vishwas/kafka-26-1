# Lab: Explore consumers in a single consumer group

## Lab objectives

By the end of this lab, you will be able to:

- Run multiple consumers in the same consumer group.
- Observe partition assignment across group members.
- Verify that each record is consumed by only one member in the group.
- Observe rebalance behavior when consumers join or leave.
- Inspect group offsets and lag with Kafka CLI tools.

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

### 2. Create a topic with 3 partitions

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic cg-demo \
	--partitions 3 \
	--replication-factor 3
```

Describe the topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--topic cg-demo
```

### 3. Produce initial records

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic cg-demo \
	--property parse.key=true \
	--property key.separator=:
```

Type sample records:

```text
u1:event-1
u2:event-2
u3:event-3
u1:event-4
u2:event-5
u3:event-6
```

Exit producer with `Ctrl+C`.

### 4. Start first consumer (Terminal A)

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic cg-demo \
	--group cg-lab-group \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

This consumer joins group `cg-lab-group` and starts consuming.

### 5. Check group details from another terminal

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group cg-lab-group
```

Observe current partition assignment and committed offsets.

### 6. Start second consumer in same group (Terminal B)

Run the same command as step 4 in a second terminal.

Kafka triggers rebalance and splits partitions between the two consumers.

Check group details again:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group cg-lab-group
```

You should see two consumer members and partition distribution across them.

### 7. Produce more records and observe load sharing

In another terminal, run producer again:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic cg-demo \
	--property parse.key=true \
	--property key.separator=:
```

Send records:

```text
u1:event-7
u2:event-8
u3:event-9
u1:event-10
u2:event-11
u3:event-12
```

Check Terminal A and Terminal B outputs:

- Every record is processed by only one consumer in this group.
- Records from the same partition stay ordered.

### 8. Add third consumer (Terminal C)

Start a third consumer with the same command/group. With 3 partitions, each consumer should get about one partition.

Check assignment again:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group cg-lab-group
```

### 9. Observe rebalance when one consumer leaves

Stop Terminal B consumer using `Ctrl+C`.

Kafka rebalances remaining consumers and reassigns partitions.

Verify:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group cg-lab-group
```

### 10. Check lag and offsets

Produce a few records, then run:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group cg-lab-group
```

Observe:

- `CURRENT-OFFSET`
- `LOG-END-OFFSET`
- `LAG`

When consumers catch up, lag should approach `0`.

### 11. Clean up

Stop all consumer terminals with `Ctrl+C`.

Delete demo topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic cg-demo
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Consumers in the same `group.id` share partitions.
- A partition is consumed by only one group member at a time.
- Adding/removing consumers triggers rebalance and partition reassignment.
- Throughput can scale by increasing partitions and consumer instances in a group.
- Consumer group offsets and lag can be monitored with `kafka-consumer-groups.sh`.