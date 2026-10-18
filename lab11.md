# Lab: Reset Kafka Consumer Offsets

## Prerequisites

- A running cluster from [`lab04/docker-compose.yml`](./lab04/docker-compose.yml)
- The `orders` topic from [lab10](./lab10.md)
- The Java consumer from lab10

Run Docker Compose commands from the repository root. This lab uses a dedicated consumer group, `orders-offset-reset-lab`, so it will not change offsets used by other consumer groups.

## Lab steps

### 1. Start Kafka and produce test records

```bash
docker compose -f lab04/docker-compose.yml up -d
```

If the `orders` topic has no records yet, produce a few from a second terminal:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --topic orders
```

Enter a few test messages, then exit the producer with `Ctrl+C`.

### 2. Start the consumer with a dedicated group

In the Java consumer from lab10, set:

```java
props.put("group.id", "orders-offset-reset-lab");
```

Run the consumer and wait for it to read records. Stop it with `Ctrl+C`, then confirm it has exited before resetting offsets.

### 3. Inspect the committed offsets

From the repository root:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --describe \
  --group orders-offset-reset-lab
```

Check the current offsets and lag for each partition. Make sure no consumer from this group is still running before continuing.

### 4. Preview a reset to the beginning

Use `--dry-run` first; it displays the proposed offsets without changing them:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --group orders-offset-reset-lab \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --dry-run
```

### 5. Apply the reset and replay records

After reviewing the preview, run the same command with `--execute`:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --group orders-offset-reset-lab \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute
```

Restart the Java consumer using the same group ID. It should read the records again from the beginning. Stop it with `Ctrl+C` after confirming the replay.

### 6. Preview and apply a reset to the end

With the consumer stopped, preview a reset to the end of each partition:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --group orders-offset-reset-lab \
  --topic orders \
  --reset-offsets \
  --to-latest \
  --dry-run
```

Apply it:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --group orders-offset-reset-lab \
  --topic orders \
  --reset-offsets \
  --to-latest \
  --execute
```

Restart the consumer. It should not replay the older records; produce a new record to `orders` and verify that the consumer receives it.

### 7. Stop the cluster

From the repository root:

```bash
docker compose -f lab04/docker-compose.yml down
```

## Key points

- Stop all consumers in a group before resetting its offsets; an active group cannot be reset.
- Use `--dry-run` to preview the change, then `--execute` to apply it.
- `--to-earliest` moves the group position to the earliest retained record; restarting the consumer replays available records.
- `--to-latest` moves the group position to the current end; existing records are skipped, but subsequently produced records can be consumed.
- Offsets belong to a consumer group. Resetting one group does not change offsets for other groups.
- A reset changes the group's committed positions; it does not delete or modify topic records.
