# Lab: Configure Dynamic Topic Configurations in Kafka

In your Docker Compose Kafka cluster, you can change topic settings while Kafka is running, without restarting brokers. This is called **dynamic topic configuration**.

In this lab, you will use the existing `orders` topic to:

- change retention settings,
- inspect effective configuration,
- and restore defaults.

## Pre-req
Set a variable in each new terminal session after navigating to the Kafka installation directory:

```
cd ~/kafka_2.13-4.3.1
export BS=localhost:9092
```

## Listing the Topics in the cluster
```bash
bin/kafka-topics.sh \
  --list \
  --bootstrap-server "$BS"
```

## Describing the topic

```bash
bin/kafka-topics.sh \
  --describe \
  --topic cyber-events \
  --bootstrap-server "$BS"
```

## Understand topic-level configuration
Use the below command to see all the existing configuration:

```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --describe \
  --all
```

Kafka has several configurable topic settings:

| Configuration | Purpose | Example |
|---|---|---|
| `retention.ms` | How long Kafka retains records, based on time | `3600000` = 1 hour |
| `retention.bytes` | Approximate maximum retained log size per partition | `104857600` = 100 MiB |
| `cleanup.policy` | Whether Kafka deletes old data or compacts by key | `delete` or `compact` |
| `segment.bytes` | Target size of a log segment | `1073741824` = 1 GiB |
| `min.insync.replicas` | Minimum in-sync replicas required for writes using `acks=all` | `2` |

> Retention is not necessarily an exact deletion deadline. Kafka removes eligible data as log segments become eligible for cleanup, so records may remain beyond the configured retention period.

## Change the retention period dynamically

Configure the `orders` topic to retain data for 1 hour instead of Kafka's common default of 7 days.

Run this command from your project directory:

```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.ms=3600000
```

Expected output:

```text
Completed updating config for topic orders.
```

For 5 minutes:
```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.ms=300000
```


The change applies dynamically; you do not need to restart Kafka.

### Try different retention periods

Use this command pattern and replace `retention.ms` as needed:

```bash
docker compose exec kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka1:29092 --entity-type topics --entity-name orders --alter --add-config retention.ms=<milliseconds>
```

Examples:

- 10 minutes = `600000`
- 30 minutes = `1800000`
- 1 hour = `3600000`
- 6 hours = `21600000`
- 1 day = `86400000`
- 7 days = `604800000`

## Verify the configuration

Run:

```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --describe
```

Look for output similar to:

```text
Dynamic configs for topic orders are:
  retention.ms=3600000
```

This indicates that `orders` has a topic-level override. Other topics are unaffected.

## Configure retention by size

You can also limit retained data by size per partition.

Set a limit of 100 MiB per partition:

```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.bytes=104857600
```

With three partitions, this gives a nominal aggregate size limit of approximately 300 MiB for the topic, although actual disk usage and cleanup behavior depend on log segments and replication.

When using the `delete` cleanup policy, Kafka removes eligible old segments when the time or size retention limit is reached.

## Change the cleanup policy

Kafka supports both deletion and compaction.

To use time/size-based deletion:

```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config cleanup.policy=delete
```

To use log compaction instead:

```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config cleanup.policy=compact
```

> **Caution:** Compaction is useful when you want to retain the latest value for each message key. It does not mean older records are deleted immediately, and changing to compaction changes the topic's data-retention behavior.

## Remove a topic override and restore the default

If you want `orders` to inherit Kafka's default retention again, delete the override:

```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --delete-config retention.ms
```

Verify the result:

```bash
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name orders \
  --describe
```

The `retention.ms` override should no longer appear. Kafka will use the applicable server-level default, commonly seven days (`604800000` ms), unless your cluster config specifies otherwise.

## Test it in Kafka UI

1. Open Kafka UI.
2. Select `local-kafka-cluster`.
3. Open **Topics → orders**.
4. Inspect topic configuration (if your UI version exposes it).

Use the CLI commands above as the authoritative verification of effective topic overrides.

## Key takeaway

- Dynamic topic configuration is useful when different topics have different operational requirements—for example, retaining application events for seven days but retaining temporary debugging messages for one hour.

- You can change these settings while the cluster is running, without restarting brokers.

- For this lab, try changing `orders` to one minute of retention, produce messages, and observe cleanup. Remember that retention is segment-based, so a one-minute setting does not guarantee each message disappears exactly one minute after production.
