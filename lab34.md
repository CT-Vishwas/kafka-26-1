# Lab: Monitor Kafka with Prometheus and Grafana

## Objectives

- Export Kafka broker JMX metrics for Prometheus.
- View broker metrics in Grafana.
- Monitor the existing three-broker Kafka cluster without removing its data.

## Prerequisites

- Docker Engine and Docker Compose
- The Kafka cluster from [lab04/docker-compose.yml](lab04/docker-compose.yml)
- The JMX Exporter Java agent JAR from the official Prometheus JMX Exporter releases

Run commands from the repository root unless noted.

> Enabling the exporter requires restarting the Kafka broker processes. The steps below
> recreate brokers one at a time and preserve their named data volumes. Do not use
> `docker compose down -v`.

## Step 1: Configure the JMX Exporter

Create `lab04/monitoring/jmx-kafka.yml`:

```yaml
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
  - pattern: ".*"
```

Download the JMX Exporter Java agent JAR from an official release and save it as:

```text
lab04/monitoring/jmx_prometheus_javaagent.jar
```

The JAR is a local dependency for this lab; avoid committing it to the repository.

## Step 2: Enable metrics on each Kafka broker

Edit [lab04/docker-compose.yml](lab04/docker-compose.yml). For **each** of `kafka1`,
`kafka2`, and `kafka3`, add the following volume mounts and environment variable:

```yaml
    volumes:
      - kafka1-data:/var/lib/kafka/data
      - ./monitoring/jmx-kafka.yml:/etc/kafka/jmx-kafka.yml:ro
      - ./monitoring/jmx_prometheus_javaagent.jar:/opt/kafka/jmx_prometheus_javaagent.jar:ro
    environment:
      # Keep the broker's existing environment variables.
      KAFKA_OPTS: "-javaagent:/opt/kafka/jmx_prometheus_javaagent.jar=9404:/etc/kafka/jmx-kafka.yml"
```

Keep each broker's existing data volume name (`kafka1-data`, `kafka2-data`, or
`kafka3-data`). Port `9404` is exposed inside each container; do not map it to the
same host port for all three brokers.

## Step 3: Add Prometheus and Grafana

Add these services to the same Compose file:

```yaml
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
```

Add the following scrape configuration as `lab04/monitoring/prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: kafka
    static_configs:
      - targets:
          - kafka1:9404
          - kafka2:9404
          - kafka3:9404
```

Add the Prometheus and Grafana volumes to the Compose file’s existing top-level
`volumes` section:

```yaml
  prometheus-data:
  grafana-data:
```

## Step 4: Restart brokers one at a time

From the repository root, recreate each broker so it picks up the Java agent. Wait for
each broker to come back before moving to the next:

```bash
docker compose -f lab04/docker-compose.yml up -d --no-deps kafka1
docker compose -f lab04/docker-compose.yml exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092 --list
```

Repeat for `kafka2` and `kafka3`:

```bash
docker compose -f lab04/docker-compose.yml up -d --no-deps kafka2
docker compose -f lab04/docker-compose.yml exec kafka2 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka2:29092 --list

docker compose -f lab04/docker-compose.yml up -d --no-deps kafka3
docker compose -f lab04/docker-compose.yml exec kafka3 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka3:29092 --list
```

If a broker is not ready yet, wait and retry its topic-list command before restarting
another broker.

## Step 5: Start the monitoring services

```bash
docker compose -f lab04/docker-compose.yml up -d prometheus grafana
docker compose -f lab04/docker-compose.yml ps
```

## Step 6: Check metrics in Prometheus

Open [http://localhost:9090](http://localhost:9090), select **Status → Targets**, and
confirm all three Kafka targets are **UP**.

You can also check an exporter endpoint from the host only if you temporarily publish a
unique host port for that broker. Otherwise, inspect it from inside the Compose network
or use Prometheus's query page to search for Kafka metrics such as
`messagesinpersec`.

## Step 7: Connect Grafana to Prometheus

1. Open [http://localhost:3000](http://localhost:3000).
2. Sign in with the initial credentials `admin` / `admin` and change the password when
   prompted.
3. Add a Prometheus data source.
4. Set its URL to `http://prometheus:9090` and save it.
5. Open **Explore**, select the Prometheus data source, and search for Kafka metrics.

Use the metric browser to find metrics for message rates, bytes in/out, request handling,
and broker health. Metric names depend on the JMX Exporter rules and Kafka version.

## Troubleshooting

- If Prometheus targets are down, check `docker compose -f lab04/docker-compose.yml logs
  kafka1 kafka2 kafka3 prometheus` and verify the JAR and YAML mounts exist.
- Confirm the `KAFKA_OPTS` value is set on all three brokers and the exporter listens on
  port `9404`.
- Check that the target names in `prometheus.yml` match the Compose service names.
- A broker restart should preserve Kafka data as long as the named volumes are not
  removed.

## Clean up

Stop the monitoring services without deleting their data:

```bash
docker compose -f lab04/docker-compose.yml stop prometheus grafana
```

To stop the cluster as well, without removing its data volumes:

```bash
docker compose -f lab04/docker-compose.yml down
```

Do not add `-v` unless you intentionally want to delete the named volumes.

Yes, a restart is needed for Kafka to load the Java agent. The draft restarts brokers sequentially and avoids deleting the cluster’s data volumes. Switch to an editing agent to have this saved as `lab34.md`.