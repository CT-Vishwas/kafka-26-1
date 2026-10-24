# Lab: Inspect Kafka log files

## Lab objectives

By the end of this lab, you will be able to:

- Locate Kafka log directories on a broker.
- Identify topic-partition folders and segment files.
- Produce sample records and verify data is written to disk.
- Inspect message records, offsets, and timestamps from segment files.
- Relate topic metadata to actual log files stored by Kafka.

## Prerequisites

- Docker Engine and Docker Compose
- Kafka cluster from [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml)

Run all commands from the repository root unless noted.

## Lab steps

### 1. Start the 3-broker Kafka cluster

```bash
docker compose -f code/lab04/docker-compose.yml up -d
docker compose -f code/lab04/docker-compose.yml ps
```

Wait until `kafka1`, `kafka2`, and `kafka3` are running.

### 2. Create a test topic for log inspection

Create a topic with one partition (easy to inspect) and replication factor 3:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic logs-demo \
	--partitions 1 \
	--replication-factor 3
```

Describe the topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--topic logs-demo
```

Note the leader broker for partition `logs-demo-0`.

### 3. Publish sample messages

```bash
docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic logs-demo <<'EOF'
order-1001:CREATED
order-1002:PAID
order-1003:SHIPPED
order-1004:DELIVERED
EOF
```

### 4. Locate Kafka log directories

Check broker log root directory (`KAFKA_LOG_DIRS`):

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	ls -lah /var/lib/kafka/data
```

List topic-partition folders:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	find /var/lib/kafka/data -maxdepth 1 -type d | sort
```

Find the folder for your topic partition:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	ls -lah /var/lib/kafka/data/logs-demo-0
```

You should see files such as:

- `00000000000000000000.log`
- `00000000000000000000.index`
- `00000000000000000000.timeindex`

### 5. Inspect records inside segment files

Dump the `.log` segment content:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-dump-log.sh \
	--print-data-log \
	--files /var/lib/kafka/data/logs-demo-0/00000000000000000000.log
```

Observe:

- `offset` values (for example `0,1,2,3`)
- message payload (`CREATED`, `PAID`, etc.)
- timestamp information

### 6. Compare offsets via Kafka CLI

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-get-offsets.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic logs-demo
```

Confirm the latest offset reported by Kafka matches what you observed in the segment dump.

### 7. Verify replica log files on follower brokers

Run on `kafka2` and `kafka3`:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka2 \
	ls -lah /var/lib/kafka/data/logs-demo-0

docker compose -f code/lab04/docker-compose.yml exec kafka3 \
	ls -lah /var/lib/kafka/data/logs-demo-0
```

You should see corresponding segment and index files on followers as replication completes.

### 8. Clean up

Delete the test topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic logs-demo
```

Stop the cluster when done:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Kafka stores each topic partition in a separate directory under `KAFKA_LOG_DIRS`.
- A partition is made of segment `.log` files plus index files (`.index`, `.timeindex`).
- Offsets are sequential and can be validated from both CLI tools and raw segment dump output.
- Replication creates corresponding log files on follower brokers.
