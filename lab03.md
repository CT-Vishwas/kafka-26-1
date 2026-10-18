# Kafka setup using docker compose

This setup demonstrates how to run a multi-broker Kafka cluster using Docker Compose. It includes multiple Kafka brokers.
- Install Docker Compose

```bash
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/download/v5.6.0/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
```

- See the docker compose version

```bash
docker compose --version
```

- Inspect docker compose yaml file for kafka in kraft mode 

```yaml
version: '3.8'

services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"   # Native internal/external broker communications
    # Host machine access (if needed outside Docker)
    environment:
      # Cluster Configuration (KRaft Mode)
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'
      
      # Listeners setup
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://:9092,EXTERNAL://localhost:9094'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT'
      
      # Required settings for single-node development
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      LOG_FLUSH_INTERVAL_MESSAGES: 9223372036854775807
      
  kafka-ui:
    image: kafbat/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    depends_on:
      - kafka
    environment:
      DYNAMIC_CONFIG_ENABLED: 'true'
      KAFKA_CLUSTERS_0_NAME: 'local-kraft-cluster'
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: 'kafka:9092'
```

**Note:** Ensure that Docker and Docker Compose are installed and running on your system before executing the commands.
- docker containers from previous runs, are removed
```bash
docker container ls -a
docker container rm kafka
```

- From the directory containing the `docker-compose.yml` file, run the following command to start the Kafka cluster:

```bash
docker compose up -d
```
- Verify that the Kafka containers are running:

```bash
docker compose ps
```

- Access Kafka UI at [http://localhost:8080](http://localhost:8080) to manage and monitor the Kafka cluster.

- To stop the Kafka cluster, run the following command:

```bash
docker compose down
```