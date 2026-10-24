# Lab: Kafka Connect — File source, JDBC sink, CSV import, and DB export

## Lab objectives

By the end of this lab, you will be able to:

- Run Kafka Connect in distributed mode.
- Ingest file/CSV data into Kafka using a File Source Connector.
- Export Kafka topic data to PostgreSQL using a JDBC Sink Connector.
- Validate end-to-end flow from file → Kafka → database.

## Prerequisites

- Docker Engine and Docker Compose
- Running Kafka cluster from [code/lab04/docker-compose.yml](../../code/lab04/docker-compose.yml)
- `curl` installed

Run commands from repository root unless noted.

## Lab steps

### 1. Start Kafka cluster

```bash
docker compose -f code/lab04/docker-compose.yml up -d
docker compose -f code/lab04/docker-compose.yml ps
```

### 2. Create topic for CSV/file import

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--create \
	--topic csv-orders \
	--partitions 3 \
	--replication-factor 3
```

### 3. Start PostgreSQL container (sink target DB)

```bash
docker run -d --name pg-kafka-connect \
	-e POSTGRES_USER=appuser \
	-e POSTGRES_PASSWORD=apppass \
	-e POSTGRES_DB=ordersdb \
	-p 5432:5432 \
	postgres:16
```

### 4. Start Kafka Connect container with JDBC plugin

```bash
docker run -d --name kafka-connect-lab \
	--add-host=host.docker.internal:host-gateway \
	-p 8083:8083 \
	-e CONNECT_BOOTSTRAP_SERVERS=host.docker.internal:9092,host.docker.internal:9093,host.docker.internal:9094 \
	-e CONNECT_REST_ADVERTISED_HOST_NAME=localhost \
	-e CONNECT_GROUP_ID=connect-cluster-1 \
	-e CONNECT_CONFIG_STORAGE_TOPIC=_connect-configs \
	-e CONNECT_OFFSET_STORAGE_TOPIC=_connect-offsets \
	-e CONNECT_STATUS_STORAGE_TOPIC=_connect-status \
	-e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=3 \
	-e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=3 \
	-e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=3 \
	-e CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
	-e CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
	-e CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components \
	confluentinc/cp-kafka-connect:7.7.1 \
	bash -c "confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest && /etc/confluent/docker/run"
```

Wait until Connect REST is ready:

```bash
curl -s http://localhost:8083/
```

### 5. Prepare CSV file inside Connect container

```bash
docker exec -it kafka-connect-lab bash -lc 'cat > /tmp/orders.csv <<EOF
order_id,customer,amount,status
1001,Alice,1200,CREATED
1002,Bob,2200,PAID
1003,Carol,800,CREATED
1004,David,1400,SHIPPED
EOF'
```

### 6. Create File Source Connector (CSV import to Kafka)

```bash
curl -X POST http://localhost:8083/connectors \
	-H "Content-Type: application/json" \
	-d '{
		"name": "file-source-orders",
		"config": {
			"connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
			"tasks.max": "1",
			"file": "/tmp/orders.csv",
			"topic": "csv-orders"
		}
	}'
```

Check connector status:

```bash
curl -s http://localhost:8083/connectors/file-source-orders/status
```

### 7. Verify records are in Kafka topic

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--topic csv-orders \
	--from-beginning
```

You should see CSV lines appear in the topic.

### 8. Create JDBC Sink Connector (export Kafka data to DB)

```bash
curl -X POST http://localhost:8083/connectors \
	-H "Content-Type: application/json" \
	-d '{
		"name": "jdbc-sink-orders",
		"config": {
			"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
			"tasks.max": "1",
			"topics": "csv-orders",
			"connection.url": "jdbc:postgresql://host.docker.internal:5432/ordersdb",
			"connection.user": "appuser",
			"connection.password": "apppass",
			"auto.create": "true",
			"auto.evolve": "true",
			"insert.mode": "insert",
			"pk.mode": "none",
			"table.name.format": "orders_csv",
			"value.converter": "org.apache.kafka.connect.storage.StringConverter"
		}
	}'
```

Check connector status:

```bash
curl -s http://localhost:8083/connectors/jdbc-sink-orders/status
```

### 9. Validate exported data in PostgreSQL

```bash
docker exec -it pg-kafka-connect psql -U appuser -d ordersdb -c "SELECT * FROM orders_csv;"
```

You should see records copied from Kafka topic into table `orders_csv`.

### 10. Append new CSV data and observe incremental flow

Append new rows:

```bash
docker exec -it kafka-connect-lab bash -lc 'cat >> /tmp/orders.csv <<EOF
1005,Eve,3000,PAID
1006,Frank,450,CREATED
EOF'
```

Re-check topic and DB:

```bash
docker exec -it pg-kafka-connect psql -U appuser -d ordersdb -c "SELECT * FROM orders_csv;"
```

### 11. List connectors and inspect configuration

```bash
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/file-source-orders/config
curl -s http://localhost:8083/connectors/jdbc-sink-orders/config
```

### 12. Cleanup

Delete connectors:

```bash
curl -X DELETE http://localhost:8083/connectors/file-source-orders
curl -X DELETE http://localhost:8083/connectors/jdbc-sink-orders
```

Stop and remove containers:

```bash
docker rm -f kafka-connect-lab pg-kafka-connect
```

Delete lab topic:

```bash
docker compose -f code/lab04/docker-compose.yml exec kafka1 \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
	--delete --topic csv-orders
```

Stop Kafka cluster when done:

```bash
docker compose -f code/lab04/docker-compose.yml down
```

## What you learned

- File Source Connector can continuously ingest file content into Kafka topics.
- JDBC Sink Connector can continuously export Kafka records to relational databases.
- Kafka Connect decouples data integration from custom application code.
- End-to-end pipelines (file/CSV → Kafka → DB) are easy to build and operate with connectors.
