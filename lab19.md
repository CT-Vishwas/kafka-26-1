# Lab: Multiple group consumption

## Lab objectives

By the end of this lab, you will be able to:

- Run multiple consumer groups on the same topic.
- Verify that each group receives its own copy of topic records.
- Compare independent offsets and lag per group.
- Observe how restarting one group does not affect other groups.

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

### 2. Create a shared topic

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic multi-group-demo \
	--partitions 3 \
	--replication-factor 3
```

Verify:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--topic multi-group-demo
```

### 3. Start Consumer Group A

Open Terminal A:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic multi-group-demo \
	--group orders-analytics-group \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

### 4. Start Consumer Group B

Open Terminal B:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic multi-group-demo \
	--group orders-billing-group \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

Both groups subscribe to the same topic but maintain separate offsets.

### 5. Produce records to the shared topic

Open Terminal C:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic multi-group-demo \
	--property parse.key=true \
	--property key.separator=:
```

Send sample events:

```text
ORD-1001:CREATED
ORD-1002:PAID
ORD-1003:SHIPPED
ORD-1004:DELIVERED
```

Stop producer with `Ctrl+C`.

### 6. Verify both groups consume all events

Observe Terminal A and Terminal B outputs.

Expected behavior:

- Group A receives all records.
- Group B also receives all records.
- This is not duplicate processing; it is independent consumption per group.

### 7. Inspect offsets for each group

Check Group A:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group orders-analytics-group
```

Check Group B:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-consumer-groups.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--describe \
	--group orders-billing-group
```

Compare `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG` for each group.

### 8. Stop Group B and continue Group A

Stop Terminal B consumer (`Ctrl+C`). Keep Group A running.

Produce more records:

```bash
docker compose -f code/lab04/docker-compose.yml exec -T kafka1 \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic multi-group-demo \
	--property parse.key=true \
	--property key.separator=: <<'EOF'
ORD-1005:RETURN_REQUESTED
ORD-1006:REFUNDED
EOF
```

Group A consumes new records immediately. Group B is offline and lags behind.

### 9. Restart Group B and observe catch-up

Restart the Group B consumer using the same command from step 4.

Expected behavior:

- Group B resumes from its own committed offsets.
- It catches up with records produced while it was offline.
- Group A is unaffected.

### 10. Demonstrate replay with a new group C

Open Terminal D and start a brand-new group:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic multi-group-demo \
	--group orders-audit-group \
	--from-beginning \
	--property print.key=true \
	--property key.separator=:
```

Because this is a new group, it reads all available retained records from beginning.

### 11. Clean up

Stop all consumer terminals with `Ctrl+C`.

Delete topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete \
	--topic multi-group-demo
```

Stop cluster:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- Consumer groups are independent subscribers of the same topic.
- Offsets are tracked per `group.id`, not per topic globally.
- Stopping one group does not impact consumption progress of other groups.
- New groups can replay retained data from the beginning.
