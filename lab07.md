# Lab: demonstrate Kafka fault tolerance

Assume your Kafka CLI scripts are available in `bin/` and the cluster uses `localhost:9092` as a bootstrap address. Adapt the addresses if your brokers use different listeners.

### Step 1 — Create a replicated topic

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic security-events \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

A replication factor of 3 requires at least three brokers eligible to host the topic's replicas.

### Step 2 — Inspect partition leadership and replicas

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic security-events
```

Look for output fields such as:

```
Topic: security-events
Partition: 0
Leader: 1
Replicas: 1,2,3
Isr: 1,2,3
```

The exact broker IDs and leaders will depend on your cluster.

Interpretation:

- `Leader`: broker currently leading that partition.
- `Replicas`: brokers assigned to store the partition's replicas.
- `Isr`: replicas currently considered in sync.

### Step 3 — Produce test events with strong acknowledgments

```bash
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic security-events \
  --producer-property acks=all
```

Enter a few test events:

```bash
{"event_id":"EVT-1001","severity":"HIGH"}
{"event_id":"EVT-1002","severity":"MEDIUM"}
{"event_id":"EVT-1003","severity":"HIGH"}
```

### Step 4 — Stop one broker

In a local Docker Compose lab, stop one broker using the appropriate service name:

```
docker compose stop kafka1
```

Replace `broker1` with the actual service name in your Compose file. Do not perform this experiment on a production cluster.

### Step 5 — Inspect the topic again

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic security-events
```

Check the following:

1. Did partitions whose leaders were on the stopped broker elect new leaders?
2. Did their ISR lists shrink?
3. Can the producer continue writing?
4. Are some partitions temporarily unavailable?

Not every partition needs to change leadership: partitions already led by healthy brokers may continue uninterrupted.

### Step 6 — Observe the minimum ISR behavior

If the surviving ISR for a partition contains two brokers, writes with `acks=all` can continue because `min.insync.replicas=2` is satisfied.

If only one ISR member remains for a partition, writes with `acks=all` should fail under this configuration.

This is a valuable demonstration of how Kafka protects data by refusing writes when its configured replication requirement is not met.

### Step 7 — Restart the broker and verify recovery

```
docker compose start kafka1
```

After the broker reconnects, inspect the topic again:

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic security-events
```

The returning broker can catch up and rejoin the ISR. Recovery may take time, depending on the amount of data and the cluster's health.

## Recommended production configuration

For an important security-event pipeline, a reasonable starting point is:

| Component                | Recommendation                                                                                 | Why                                                 |
| ------------------------ | ---------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| Topic replication factor | `3`                                                                                            | Multiple copies across brokers.                     |
| Producer acknowledgments | `acks=all`                                                                                     | Stronger acknowledgment before success.             |
| Minimum ISR              | `2`                                                                                            | Require at least two in-sync replicas for writes.   |
| Producer idempotence     | Enabled                                                                                        | Protect against duplicates from supported retries.  |
| Consumer offset handling | Commit after successful processing, or use transactions where appropriate                      | Avoid skipping unfinished work.                     |
| Broker placement         | Independent machines or failure zones                                                          | Reduce correlated failures.                         |
| Controller quorum        | Three or more appropriately distributed controllers                                            | Maintain metadata-management availability.          |
| Monitoring               | Alert on under-replicated partitions, offline partitions, ISR shrinkage, and controller health | Detect degraded redundancy before a second failure. |
| Disaster recovery        | Backups or replication to another cluster where required                                       | Protect against cluster-wide or regional failures.  |

## Key takeaways

1. Replication provides multiple copies of partition data.
2. Leader election allows an eligible replica to take over after a leader failure.
3. ISR helps determine which replicas are sufficiently caught up to participate in normal safe leadership and write behavior.
4. `acks=all` plus `min.insync.replicas=2` is a common configuration for balancing durability and availability with three replicas.
5. Consumer offsets and processing semantics determine whether failures cause repeated or skipped processing.
6. KRaft controller quorum protects the cluster's metadata-management capability.
7. Fault tolerance is not a backup: correlated failures, operator mistakes, accidental deletion, and cluster-wide disasters require additional protection.