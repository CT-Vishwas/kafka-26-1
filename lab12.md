# Lab: Publish JSON Messages to Kafka

## Prerequisites

- A running cluster from [`lab04/docker-compose.yml`](./lab04/docker-compose.yml)
- The `orders` topic

Run Docker Compose commands from the repository root.

## Lab steps

### 1. Start Kafka and verify the topic

```bash
docker compose -f lab04/docker-compose.yml up -d
```

Check that `orders` exists:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --describe \
  --topic orders
```

If needed, create it:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3
```

### 2. Publish JSON records with the console producer

Start the producer with key parsing enabled:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

Enter each message on a single line as `key:JSON-value`:

```text
ORD-201:{"orderId":"ORD-201","customer":"C-101","items":[{"sku":"KB-01","quantity":1}],"total":79.99,"status":"CREATED"}
ORD-202:{"orderId":"ORD-202","customer":"C-102","items":[{"sku":"MS-02","quantity":2}],"total":39.98,"status":"CREATED"}
```

Exit the producer with `Ctrl+C`.

### 3. Read and verify the JSON records

Run a console consumer to display the keys and values:

```bash
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property key.separator=:
```

Confirm the output contains the order keys and JSON values. Stop the consumer with `Ctrl+C`.

### 4. Publish JSON from the Java producer

In the producer project from [lab09](./lab09.md), use `StringSerializer` for both key and value. Construct the JSON value and send it as a string:

```java
String orderId = "ORD-203";
String json = """
    {"orderId":"ORD-203","customer":"C-103","items":[{"sku":"HD-03","quantity":1}],"total":129.50,"status":"CREATED"}
    """;

ProducerRecord<String, String> record =
    new ProducerRecord<>("orders", orderId, json);
producer.send(record);
producer.flush();
```

Run the producer, then use the consumer from the previous step to confirm that `ORD-203` appears.

### 5. Stop the cluster

From the repository root:

```bash
docker compose -f lab04/docker-compose.yml down
```

## Key points

- Kafka stores records as bytes; JSON is a convention for the message value, not a built-in topic data type.
- A JSON value can be published as a string using the console producer or Java `StringSerializer`.
- With key parsing enabled, enter each record as `key:JSON`; the key is metadata separate from the JSON value.
- Keep each console-producer record on one line. Malformed JSON can still be published; validate it in the producer or consumer application when required.
- Use a consistent JSON field structure so consumers can interpret messages reliably.
