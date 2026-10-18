# Exploring Kafka CLI Tools

This lab introduces the core Kafka command-line tools used to manage topics and validate data flow.
You will practice creating a topic, producing events, consuming retained messages, and verifying broker connectivity.
By the end, you should be comfortable using the CLI for basic Kafka troubleshooting and operations.

## Objectives

- Understand how to connect to a Kafka broker using a bootstrap server.
- Use CLI commands to list, create, and describe topics.
- Produce and consume messages to verify end-to-end topic behavior.
- Build confidence with essential Kafka operational commands.


## Explore Kafka CLI tools

Set a variable in each new terminal session after navigating to the Kafka installation directory:

```
cd ~/kafka_2.13-4.3.1
export BS=localhost:9092
```

Here, BS means bootstrap server—the broker address clients use to connect to Kafka.

### Lab 5.1 — Check connectivity and list topics

```
bin/kafka-broker-api-versions.sh \
  --bootstrap-server "$BS"

bin/kafka-topics.sh \
  --list \
  --bootstrap-server "$BS"
```

The first command checks broker connectivity and supported APIs. The second lists existing topics.

### Lab 5.2 — Create and inspect a topic

Create a topic for the lab:

```
bin/kafka-topics.sh \
  --create \
  --topic cyber-events \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server "$BS"
```

Why these settings?

- `--topic`: topic name.
- `--partitions 3`: allows messages to be distributed across three partitions.
- `--replication-factor 1`: stores each partition on the single broker.

Now inspect it:

```
bin/kafka-topics.sh \
  --describe \
  --topic cyber-events \
  --bootstrap-server "$BS"
```

Expected result: a topic with three partitions, each having replication factor one and a leader on the only broker.&#x20;



### Lab 5.3 — Produce messages

Run the producer in Terminal 2:

```
bin/kafka-console-producer.sh \
  --topic cyber-events \
  --bootstrap-server "$BS"
```

Type messages, pressing Enter after each:

```
Failed login from 10.0.0.15
Malware detected on endpoint-23
Firewall blocked connection to port 445
Multiple failed SSH attempts from 10.0.0.27
```

Each line is a separate Kafka message. Press `Ctrl+C` when finished.

### Lab 5.4 — Consume messages

In Terminal 3, run:

```
bin/kafka-console-consumer.sh \
  --topic cyber-events \
  --from-beginning \
  --bootstrap-server "$BS"
```

You should see the four messages you produced.

To observe messages arriving live, start the consumer without `--from-beginning`, then publish additional messages from Terminal 2.

The `--from-beginning` option lets a new consumer read the available retained messages from the beginning of the topic.&#x20;