# Lab: Configure log compaction

## Lab objectives

By the end of this lab, you will be able to:

- Configure a Kafka topic with `cleanup.policy=compact`.
- Produce keyed records with multiple updates for the same key.
- Produce tombstone records (`key` with null value).
- Verify compaction-related topic settings.
- Observe compacted-state consumption behavior.

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

Wait until `kafka1`, `kafka2`, and `kafka3` are running.

### 2. Create a compaction topic

Create a single-partition topic to make output easier to understand.

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic customer-state \
	--partitions 1 \
	--replication-factor 3
```

### 3. Configure log compaction and faster cleaner behavior

Set `cleanup.policy=compact` and add low lags so compaction can happen quickly during a lab.

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--entity-type topics \
	--entity-name customer-state \
	--alter \
	--add-config cleanup.policy=compact,min.cleanable.dirty.ratio=0.01,segment.ms=10000,delete.retention.ms=30000
```

What these settings do:

- `cleanup.policy=compact`: keep latest record per key.
- `min.cleanable.dirty.ratio=0.01`: allow cleaner to run with little dirty data.
- `segment.ms=10000`: roll segments every 10s to make records eligible sooner.
- `delete.retention.ms=30000`: tombstones are retained for at least 30s before removal during compaction.

### 4. Verify topic-level configuration

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--entity-type topics \
	--entity-name customer-state \
	--describe
```

Confirm `cleanup.policy=compact` appears.

### 5. Produce keyed records with updates

Send multiple versions for the same keys.

```bash
docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic customer-state \
	--property parse.key=true \
	--property key.separator=: <<'EOF'
cust-101:status=NEW
cust-102:status=NEW
cust-101:status=ACTIVE
cust-103:status=NEW
cust-102:status=SUSPENDED
cust-101:status=VIP
EOF
```

### 6. Produce tombstone records

Tombstone means key exists and value is `null`.

Use console producer null marker support:

```bash
docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic customer-state \
	--property parse.key=true \
	--property key.separator=: \
	--property null.marker=NULL <<'EOF'
cust-103:NULL
EOF
```

This creates a tombstone record for key `cust-103`.

### 7. Read current topic stream from beginning

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic customer-state \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

You will see all produced events (including historical updates) until cleaner compacts segments.

Stop consumer with `Ctrl+C`.

### 8. Wait and trigger additional segment roll activity

Compaction is asynchronous. Wait around 30–90 seconds and produce a few more updates.

```bash
sleep 30

docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic customer-state \
	--property parse.key=true \
	--property key.separator=: <<'EOF'
cust-101:status=PREMIUM
cust-102:status=ACTIVE
EOF
```

### 9. Inspect topic partition files

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	ls -lah /var/lib/kafka/data/customer-state-0
```

Optional deep inspection of one segment:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-dump-log.sh \
	--print-data-log \
	--files /var/lib/kafka/data/customer-state-0/00000000000000000000.log
```

### 10. Validate compacted-state expectation

Consume again from beginning:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic customer-state \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

Expected concept:

- For each key, compaction keeps the latest value over time.
- Older superseded values may still appear until cleaner completes.
- Tombstoned keys are eventually removed after `delete.retention.ms` and cleaner cycles.

### 11. Clean up topic overrides and resources

Delete topic-level overrides (optional but recommended):

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--entity-type topics \
	--entity-name customer-state \
	--alter \
	--delete-config cleanup.policy,min.cleanable.dirty.ratio,segment.ms,delete.retention.ms
```

Delete the demo topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic customer-state
```

Stop cluster when done:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Log compaction is key-based retention, not time-only deletion.
- Compaction keeps the latest value per key and runs asynchronously.
- Topic-level settings allow you to tune compaction behavior for specific workloads.
- Tombstones are used to represent deletions in compacted topics.
