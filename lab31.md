# Lab: Kafka security — SSL, SASL, ACLs, and secure producer/consumer flow

## Lab objectives

By the end of this lab, you will be able to:

- Configure Kafka with TLS/SSL encryption.
- Configure SASL/PLAIN authentication.
- Create and validate ACLs for topic access control.
- Run secure producer and consumer clients with authenticated access.

## Prerequisites

- Docker Engine and Docker Compose
- Java `keytool` and `openssl` installed on host
- Existing Kafka lab workspace

Run all commands from repository root unless noted.

## Lab overview

This lab uses a **dedicated security sandbox** (single broker) so you can focus on security controls without changing earlier labs.

Security model used:

- Transport security: `SASL_SSL`
- Authentication: `SASL/PLAIN`
- Authorization: ACLs with `kafka-acls.sh`

---

## Part A: Prepare certificates and security files

### 1. Create lab folder structure

```bash
mkdir -p code/lab31/security/{certs,config,data}
```

### 2. Generate Certificate Authority (CA)

```bash
openssl req -new -x509 -keyout code/lab31/security/certs/ca-key.pem \
	-out code/lab31/security/certs/ca-cert.pem \
	-days 365 -passout pass:capass \
	-subj "/CN=Kafka-Training-CA"
```

### 3. Generate broker keystore and certificate

```bash
keytool -genkey -keystore code/lab31/security/certs/kafka.server.keystore.jks \
	-alias broker -validity 365 -storepass changeit -keypass changeit \
	-dname "CN=kafka-secure, OU=Training, O=Example, L=City, S=State, C=IN"

keytool -keystore code/lab31/security/certs/kafka.server.keystore.jks \
	-alias broker -certreq -file code/lab31/security/certs/broker.csr \
	-storepass changeit -keypass changeit

openssl x509 -req -CA code/lab31/security/certs/ca-cert.pem \
	-CAkey code/lab31/security/certs/ca-key.pem -in code/lab31/security/certs/broker.csr \
	-out code/lab31/security/certs/broker-signed.crt -days 365 -CAcreateserial \
	-passin pass:capass

keytool -keystore code/lab31/security/certs/kafka.server.keystore.jks \
	-alias CARoot -import -file code/lab31/security/certs/ca-cert.pem \
	-storepass changeit -keypass changeit -noprompt

keytool -keystore code/lab31/security/certs/kafka.server.keystore.jks \
	-alias broker -import -file code/lab31/security/certs/broker-signed.crt \
	-storepass changeit -keypass changeit -noprompt
```

### 4. Create broker truststore

```bash
keytool -keystore code/lab31/security/certs/kafka.server.truststore.jks \
	-alias CARoot -import -file code/lab31/security/certs/ca-cert.pem \
	-storepass changeit -keypass changeit -noprompt
```

### 5. Create JAAS configuration (broker + client users)

Create file `code/lab31/security/config/kafka_server_jaas.conf`:

```properties
KafkaServer {
	org.apache.kafka.common.security.plain.PlainLoginModule required
	username="admin"
	password="admin-secret"
	user_admin="admin-secret"
	user_app1="app1-secret"
	user_app2="app2-secret";
};
```

### 6. Create client properties files

Create `code/lab31/security/config/admin.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
ssl.truststore.location=/opt/kafka/security/certs/kafka.server.truststore.jks
ssl.truststore.password=changeit
```

Create `code/lab31/security/config/app1.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="app1" password="app1-secret";
ssl.truststore.location=/opt/kafka/security/certs/kafka.server.truststore.jks
ssl.truststore.password=changeit
```

Create `code/lab31/security/config/app2.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="app2" password="app2-secret";
ssl.truststore.location=/opt/kafka/security/certs/kafka.server.truststore.jks
ssl.truststore.password=changeit
```

---

## Part B: Start secure Kafka broker

### 7. Create secure Docker Compose file

Create `code/lab31/docker-compose.yml`:

```yaml
services:
	kafka-secure:
		image: apache/kafka:4.3.1
		hostname: kafka-secure
		container_name: kafka-secure
		ports:
			- "9096:9096"
		environment:
			KAFKA_NODE_ID: 1
			KAFKA_PROCESS_ROLES: broker,controller
			KAFKA_LISTENERS: SASL_SSL://:9096,CONTROLLER://:29093
			KAFKA_ADVERTISED_LISTENERS: SASL_SSL://localhost:9096
			KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT
			KAFKA_INTER_BROKER_LISTENER_NAME: SASL_SSL
			KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
			KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-secure:29093
			CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

			KAFKA_LOG_DIRS: /var/lib/kafka/data

			KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
			KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN

			KAFKA_SSL_KEYSTORE_LOCATION: /opt/kafka/security/certs/kafka.server.keystore.jks
			KAFKA_SSL_KEYSTORE_PASSWORD: changeit
			KAFKA_SSL_KEY_PASSWORD: changeit
			KAFKA_SSL_TRUSTSTORE_LOCATION: /opt/kafka/security/certs/kafka.server.truststore.jks
			KAFKA_SSL_TRUSTSTORE_PASSWORD: changeit

			KAFKA_AUTHORIZER_CLASS_NAME: org.apache.kafka.metadata.authorizer.StandardAuthorizer
			KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false"
			KAFKA_SUPER_USERS: User:admin

			KAFKA_OPTS: -Djava.security.auth.login.config=/opt/kafka/security/config/kafka_server_jaas.conf

		volumes:
			- ./security/certs:/opt/kafka/security/certs
			- ./security/config:/opt/kafka/security/config
			- ./security/data:/var/lib/kafka/data
```

### 8. Start secure broker

```bash
docker compose -f code/lab31/docker-compose.yml up -d
docker compose -f code/lab31/docker-compose.yml ps
```

### 9. Verify secure connectivity with admin user

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:9096 \
	--command-config /opt/kafka/security/config/admin.properties \
	--create --topic secure-orders --partitions 3 --replication-factor 1
```

---

## Part C: ACL creation and validation

### 10. Grant producer permission to app1

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-acls.sh \
	--bootstrap-server localhost:9096 \
	--command-config /opt/kafka/security/config/admin.properties \
	--add --allow-principal User:app1 \
	--operation WRITE --operation DESCRIBE \
	--topic secure-orders
```

### 11. Grant consumer permission to app2

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-acls.sh \
	--bootstrap-server localhost:9096 \
	--command-config /opt/kafka/security/config/admin.properties \
	--add --allow-principal User:app2 \
	--operation READ --operation DESCRIBE \
	--topic secure-orders

docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-acls.sh \
	--bootstrap-server localhost:9096 \
	--command-config /opt/kafka/security/config/admin.properties \
	--add --allow-principal User:app2 \
	--operation READ \
	--group secure-consumer-group
```

### 12. List ACLs

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-acls.sh \
	--bootstrap-server localhost:9096 \
	--command-config /opt/kafka/security/config/admin.properties \
	--list
```

---

## Part D: Secure producer/consumer flow

### 13. Produce messages as app1 (authorized producer)

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server localhost:9096 \
	--producer.config /opt/kafka/security/config/app1.properties \
	--topic secure-orders
```

Type a few records and press `Ctrl+C`:

```text
order-1
order-2
order-3
```

### 14. Consume as app2 (authorized consumer)

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server localhost:9096 \
	--consumer.config /opt/kafka/security/config/app2.properties \
	--topic secure-orders \
	--group secure-consumer-group \
	--from-beginning
```

Expected: app2 reads all records.

### 15. Negative test: app2 tries to write (should fail)

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-console-producer.sh \
	--bootstrap-server localhost:9096 \
	--producer.config /opt/kafka/security/config/app2.properties \
	--topic secure-orders
```

Expected: authorization error (no WRITE ACL).

### 16. Negative test: app1 tries to read with group (should fail)

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server localhost:9096 \
	--consumer.config /opt/kafka/security/config/app1.properties \
	--topic secure-orders \
	--group app1-read-test \
	--from-beginning
```

Expected: authorization error (no READ ACL on topic/group).

---

## Part E: Cleanup

### 17. Remove ACLs (optional)

```bash
docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-acls.sh \
	--bootstrap-server localhost:9096 \
	--command-config /opt/kafka/security/config/admin.properties \
	--remove --allow-principal User:app1 --operation WRITE --operation DESCRIBE --topic secure-orders

docker compose -f code/lab31/docker-compose.yml exec kafka-secure \
	/opt/kafka/bin/kafka-acls.sh \
	--bootstrap-server localhost:9096 \
	--command-config /opt/kafka/security/config/admin.properties \
	--remove --allow-principal User:app2 --operation READ --operation DESCRIBE --topic secure-orders
```

### 18. Stop and remove secure lab

```bash
docker compose -f code/lab31/docker-compose.yml down
rm -rf code/lab31/security/data
```

## What you learned

- SSL encrypts broker-client traffic and protects data in transit.
- SASL/PLAIN authenticates client identities.
- ACLs enforce fine-grained authorization on topics and consumer groups.
- A secure producer/consumer flow requires all three layers: encryption, authentication, and authorization.
