# Lab: Kafka Partitions

Assume Kafka is running on your EC2 instance and listening on `localhost:9092`.

### Step 1: Create a topic with three partitions

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic security-events \
  --partitions 3 \
  --replication-factor 1
```

The replication factor is 1 because this example assumes a single-broker cluster.

### Step 2: Describe the partitions

```
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic security-events
```

Inspect the output for:

- `PartitionCount`: Number of partitions.
- `Partition`: Partition ID, such as 0, 1, or 2.
- `Leader`: Broker ID leading that partition.
- `Replicas`: Brokers hosting copies of the partition.
- `Isr`: In-sync replica broker IDs.

### Step 3: Produce some records

```
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic security-events
```

Enter these records, one per line:

```
Failed login from server-01
Malware detected on server-02
Port scan from external IP
Privilege escalation on server-01
```

### Step 4: Read records with partition metadata

In another terminal:

```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic security-events \
  --from-beginning \
  --property print.partition=true \
  --property print.offset=true
```

You can now observe which partition received each record and its offset.

The exact distribution depends on the producer's partitioning behavior and configuration. The displayed output may not be in one global chronological order.

### Step 5: Inspect partition distribution across brokers

If you later expand your cluster to three brokers, run the topic description command again. You can inspect the leader and replica assignments to see how partitions are distributed.

For production clusters, you can also use Kafka's administrative tools to inspect or reassign partition replicas when balancing broker storage and workload.


# Lab: Multi Broker Partitions

This hands-on lab is designed for your three-broker Kafka cluster. You will create topics with different partition counts, observe how partitions are distributed across brokers, produce and consume messages, explore offsets, and understand leader election and replication.
By the end, you should be able to explain the relationship between topics, partitions, leaders, replicas, brokers, and consumer groups using actual command-line output.

## Prepare your terminal
The examples below use Kafka's standard command-line tools. Run them from the machine where Kafka is installed, or from a container that can access your brokers.
Check your Kafka version:

```
cd kafka
bin/kafka-topics.sh --version
```

Check connectivity and broker metadata:

```
bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

Set a reusable bootstrap server variable:

```
export BS="localhost:9092"
```

Check the available CLI commands if necessary:

```
bin/kafka-topics.sh --help
bin/kafka-console-producer.sh --help
bin/kafka-console-consumer.sh --help
```

Create topics with different partition counts

We will create three topics to compare partition counts and replication factors.

| Topic       | Partitions | Replication factor | Purpose                                              |
| ----------- | ---------- | ------------------ | ---------------------------------------------------- |
| `lab-one`   | 1          | 3                  | One partition replicated across brokers              |
| `lab-three` | 3          | 3                  | One partition per broker as a possible leader layout |
| `lab-six`   | 6          | 3                  | More partitions than brokers                         |

Run these commands:

```
bin/kafka-topics.sh --bootstrap-server "$BS" \
  --create --topic lab-one \
  --partitions 1 --replication-factor 3

bin/kafka-topics.sh --bootstrap-server "$BS" \
  --create --topic lab-three \
  --partitions 3 --replication-factor 3

bin/kafka-topics.sh --bootstrap-server "$BS" \
  --create --topic lab-six \
  --partitions 6 --replication-factor 3
```

If a topic already exists, Kafka will report that it exists. You can inspect it rather than deleting it.

## Inspect the topic metadata

```
bin/kafka-topics.sh --bootstrap-server "$BS" --list
```

Describe each topic:

```
bin/kafka-topics.sh --bootstrap-server "$BS" \
  --describe --topic lab-one

bin/kafka-topics.sh --bootstrap-server "$BS" \
  --describe --topic lab-three

bin/kafka-topics.sh --bootstrap-server "$BS" \
  --describe --topic lab-six
```
- **The leader is a broker assigned to a partition**. It is not the partition itself.

## Understand the distribution
Understand partition distribution

For `lab-six`, each partition has three replicas, so there are:

$[ 6 \text{ partitions} \times 3 \text{ replicas} = 18 ]$

partition-replica placements across the cluster.

If assignments are balanced, each broker stores six partition replicas. This does not mean each broker has six leaders; the leader distribution is a separate consideration.

Explore the output yourself:

```
kafka-topics.sh --bootstrap-server "$BS" \
  --describe --topic lab-six
```

Answer these questions from your output:

1. Which broker leads partition 0?
2. Which brokers hold replicas of partition 0?
3. Is every partition assigned three replicas?
4. Are all replicas currently in the ISR?
5. How many partitions does each broker lead?

You can count the leaders manually from the `Leader` column. For a larger cluster, this can be automated with a script.

A broker can be the leader for multiple partitions at the same time. Kafka allows one leader per partition, not one leader per broker.

Analyze your actual `lab-six` output.

## Example: Count leaders in your cluster

Your cluster has 3 brokers, 6 partitions, and replication factor 3.

| Partition   | Leader broker | Replicas |
| ----------- | ------------- | -------- |
| `lab-six-0` | Broker 1      | 1, 2, 3  |
| `lab-six-1` | Broker 2      | 2, 3, 1  |
| `lab-six-2` | Broker 3      | 3, 1, 2  |
| `lab-six-3` | Broker 1      | 1, 3, 2  |
| `lab-six-4` | Broker 3      | 3, 2, 1  |
| `lab-six-5` | Broker 2      | 2, 1, 3  |

Total: 6 partition leaders, evenly distributed across 3 brokers.

Your cluster is balanced: each broker leads two partitions.

## Remember these rules

- One leader per partition: Yes.
- One leader per broker: No.
- One broker can lead many partitions: Yes.
- One broker can store many partition replicas: Yes.
- With replication factor 3 and 3 brokers: Each partition has one replica on each broker.

## Produce messages and inspect partition placement

Now we will send sample order events to `lab-three`.

Start the console producer:

```
bin/kafka-console-producer.sh \
  --bootstrap-server "$BS" \
  --topic lab-three
```

Type these messages, one per line:

```
order-101,customer-A,250
order-102,customer-B,400
order-103,customer-A,150
order-104,customer-C,700
order-105,customer-B,300
order-106,customer-D,900
order-107,customer-A,200
order-108,customer-C,500
order-109,customer-D,100
```

Press `Ctrl+C` when finished.

By default, these are unkeyed messages. Kafka's default partitioning behavior for the console producer generally distributes unkeyed records across available partitions, but the exact behavior can depend on the producer implementation and configuration. Do not assume each message will land in a particular partition.

With the command, messages have no keys. The default partitioning behavior for unkeyed messages depends on the Kafka client version and configuration. You should not assume that every message will be distributed round-robin across all partitions.

All messages in a single partition too.

### Consume messages and show their metadata

Run:

```
bin/kafka-console-consumer.sh \
  --bootstrap-server "$BS" \
  --topic lab-three \
  --from-beginning \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true
```

## Send records to a specific partition

Now you will explicitly choose the destination partition.

Start a producer:

```
bin/kafka-console-producer.sh \
  --bootstrap-server "$BS" \
  --topic lab-three \
  --property parse.key=true \
  --property key.separator=:
```

Enter these records:

```
customer-A:order-201
customer-B:order-202
customer-A:order-203
customer-C:order-204
customer-B:order-205
```

This producer parses the text before `:` as the message key and the text after it as the value.

Kafka's default keyed partitioning generally hashes the key to select a partition. Therefore, records with the same key normally go to the same partition, provided the partition count and partitioning configuration remain unchanged.

For an even more controlled experiment, explicitly select the partition using the console producer's `--property` options:

```
kafka-console-producer.sh \
  --bootstrap-server "$BS" \
  --topic lab-three \
  --property parse.key=true \
  --property key.separator=: \
  --property partitioner.class=org.apache.kafka.clients.producer.internals.DefaultPartitioner
```

The default partitioner does not provide a reliable way to select an exact partition just by entering a record. To guarantee a specific partition, use a small Kafka client program that sets the record's partition field explicitly.

### Consume messages and show their metadata

Run:

```
bin/kafka-console-consumer.sh \
  --bootstrap-server "$BS" \
  --topic lab-three \
  --from-beginning \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true
```

You can also inspect records from just one partition:

```
kafka-console-consumer.sh \
  --bootstrap-server "$BS" \
  --topic lab-three \
  --partition 1 \
  --from-beginning
```

## Knowledge-check exercises

Try answering these without looking at the explanations above.

1. If a topic has six partitions and replication factor three, how many partition replicas exist in total?
2. Can two different partitions both have offset `5`?
3. If Broker 1 leads partition 0 and Broker 2 is an in-sync follower, what can happen when Broker 1 fails?
4. If you increase a topic from three partitions to six, does Kafka redistribute the existing records?
5. Does replication factor three mean that every message is written to three leaders?
6. Can Kafka guarantee a total ordering of records across all partitions?


## Kafka Partitions Lab — Knowledge Check Answers

1. If a topic has six partitions and replication factor three, how many partition replicas exist in total?

Answer: 18 replicas.

Calculation: \\(6 \times 3 = 18\\).

Each partition has one leader and two follower replicas, assuming all three replicas are assigned and available.

2. Can two different partitions both have offset `5`?

Answer: Yes.

Offsets are maintained independently for each partition. For example, partition 0 can have offset 5, and partition 1 can also have offset 5. They identify different records.

3. If Broker 1 leads partition 0 and Broker 2 is an in-sync follower, what can happen when Broker 1 fails?

Answer: Broker 2 or another eligible in-sync replica can be elected leader for partition 0.

Producers and consumers can resume working with the new leader once the cluster completes the transition, subject to configuration and availability.

4. If you increase a topic from three partitions to six, does Kafka redistribute the existing records?

Answer: No.

Existing records remain in their original partitions. New records may be assigned to the additional partitions. Increasing the partition count can also change key-based partition mapping.

5. Does replication factor three mean that every message is written to three leaders?

Answer: No.

Each partition has one leader and two follower replicas. The leader handles the write, and followers replicate the data. Replication acknowledgements depend on producer settings such as `acks`.

6. Can Kafka guarantee a total ordering of records across all partitions?

Answer: No.

Kafka guarantees record ordering within an individual partition, not across every partition in a topic.

If related records need ordering, use a consistent message key so they are routed to the same partition.
