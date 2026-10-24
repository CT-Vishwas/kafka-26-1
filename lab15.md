# Lab: Observe segment creation

## Lab objectives

By the end of this lab, you will be able to:

- Configure a topic to roll segments quickly.
- Generate enough records to trigger segment creation.
- Verify multiple segment files in a topic-partition directory.
- Inspect records across multiple segment files.

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

### 2. Create a topic for segment-roll testing

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic segment-roll-demo \
	--partitions 1 \
	--replication-factor 3
```

### 3. Set a very small segment size

Set topic-level `segment.bytes` so Kafka creates new segments quickly.

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--entity-type topics \
	--entity-name segment-roll-demo \
	--alter \
	--add-config segment.bytes=1024
```

Verify configuration:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--entity-type topics \
	--entity-name segment-roll-demo \
	--describe
```

### 4. Confirm initial segment files

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	ls -lah /var/lib/kafka/data/segment-roll-demo-0
```

Initially, you should typically see one active segment base file such as `00000000000000000000.log`.

### 5. Produce enough data to force segment rolling

```bash
for i in $(seq 1 300); do
	echo "order-$i:$(printf 'X%.0s' {1..120})"
done | docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic segment-roll-demo
```

### 6. Observe newly created segment files

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	ls -lah /var/lib/kafka/data/segment-roll-demo-0/*.log
```

You should now see multiple `.log` files with increasing base offsets, for example:

- `00000000000000000000.log`
- `00000000000000000012.log`
- `00000000000000000024.log`

Also verify index files:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	ls -lah /var/lib/kafka/data/segment-roll-demo-0
```

### 7. Inspect records from two segment files

Inspect earliest segment:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-dump-log.sh \
	--print-data-log \
	--files /var/lib/kafka/data/segment-roll-demo-0/00000000000000000000.log
```

Inspect a later segment (replace with an actual file name seen in your output):

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-dump-log.sh \
	--print-data-log \
	--files /var/lib/kafka/data/segment-roll-demo-0/00000000000000000012.log
```

Compare offsets to confirm each segment starts at its own base offset.

### 8. Optional: observe replica segment files on other brokers

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka2 \
	ls -lah /var/lib/kafka/data/segment-roll-demo-0/*.log

docker compose -f code/lab04/docker-compose.yml exec kafka3 \
	ls -lah /var/lib/kafka/data/segment-roll-demo-0/*.log
```

### 9. Clean up

Delete the demo topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic segment-roll-demo
```

Stop the cluster when done:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Kafka splits partition data into multiple segment files over time.
- `segment.bytes` directly impacts how frequently new segments are created.
- New segment file names are based on the starting offset of that segment.
- Each segment has matching index files for fast offset and time-based lookup.