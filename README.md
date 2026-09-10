# Kafka A -> Kafka B -> MongoDB (Python)

Assignment implementation based on the provided **3-broker Confluent Kafka 7.6.1 KRaft + SASL/PLAIN + AKHQ** Docker Compose design.

## Requirement

1. Consume data from the provided Kafka source (Kafka A).
2. Produce the same records into a topic in your own Kafka cluster (Kafka B).
3. Consume that target topic and persist records into MongoDB.
4. Deliver GitHub source code and evidence that the program runs.

## Architecture

```text
Provided Kafka A
46.202.167.130:9094
SASL_PLAINTEXT / PLAIN
Topic: product_view
        |
        | consume
        v
Python relay
- preserves key/value/headers
- does NOT require the source schema
- adds original source position as relay headers
- commits Kafka A only after Kafka B acknowledges delivery
        |
        | produce
        v
Your Kafka B (Docker)
3 brokers: kafka-0, kafka-1, kafka-2
Topic: product_view_local
        |
        | consume
        v
Python Mongo sink
- auto/json/raw payload policy
- idempotent Mongo upsert
- manual Kafka commit
- DLQ for known invalid records
        |
        +-------- valid ----------> MongoDB
        |
        +-------- invalid --------> product_view_local_dlq
```

## Why this project uses the provided Kafka Compose design

The original Compose has three KRaft brokers and four listener roles:

- `CONTROLLER :9093` - KRaft controller traffic.
- `INTERNAL :29092` - broker-to-broker traffic, `PLAINTEXT`.
- `DOCKER_NETWORK :9092` - clients running inside `streaming-network`, `SASL_PLAINTEXT`.
- `EXTERNAL :9094` - clients running on the host, `SASL_PLAINTEXT`.

The three external host addresses are therefore:

```text
kafka-0 -> localhost:9094
kafka-1 -> localhost:9194
kafka-2 -> localhost:9294
```

Python running in PyCharm uses all three as bootstrap servers:

```env
TARGET_KAFKA_BOOTSTRAP_SERVERS=localhost:9094,localhost:9194,localhost:9294
```

If the Python services are later run inside Docker Compose, Compose overrides that with:

```text
kafka-0:9092,kafka-1:9092,kafka-2:9092
```

This is why the same Python code can run both on your laptop and in containers.

## Project structure

```text
kafka_relay_mongo_project/
├── .env.example
├── .gitignore
├── .dockerignore
├── compose.yaml
├── Dockerfile
├── pyproject.toml
├── README.md
│
├── config/kafka/
│   └── kafka_server_jaas.conf.example
│
├── infra/mongo/
│   └── init-mongo.js
│
├── scripts/
│   ├── prepare_local.sh
│   ├── reset_local.sh
│   └── verify_local.sh
│
├── src/kafka_relay_mongo/
│   ├── config.py
│   ├── kafka_clients.py
│   ├── create_topics.py
│   ├── inspect_source.py
│   ├── event_identity.py
│   ├── payload_codec.py
│   ├── mongo_document.py
│   ├── relay.py
│   └── sink.py
│
└── tests/ (later for future improvements)
    ├── test_config.py
    ├── test_event_identity.py
    ├── test_payload_codec.py
    └── test_mongo_document.py
```

### Why it is separated this way

- `compose.yaml`: local infrastructure only.
- `config.py`: environment-dependent settings; application code does not hard-code hosts/passwords.
- `kafka_clients.py`: one place for Kafka client authentication/configuration.
- `relay.py`: only Kafka A -> Kafka B responsibility.
- `payload_codec.py`: only payload interpretation responsibility. This is where Avro/Protobuf support can be added later.
- `mongo_document.py`: defines the Mongo document envelope separately from Kafka networking.
- `sink.py`: Kafka B -> MongoDB orchestration.
- `tests/`: tests deterministic logic without requiring live Kafka or MongoDB.

## Source Kafka schema is currently unknown

This project deliberately does **not** assume the Kafka A payload is JSON at the relay stage.

Kafka stores message values as bytes. `relay.py` forwards:

```python
key=message.key()
value=message.value()
headers=...
```

Therefore Kafka A -> Kafka B works without knowing whether the value is:

- plain JSON
- Avro
- Protobuf
- JSON Schema + Schema Registry framing
- UTF-8 text
- another binary format

Before deciding how MongoDB should interpret the value, run:

```bash
kafka-source-inspect
```

The inspector uses a temporary consumer group and samples a small number of messages without changing the real relay consumer group's offsets.

### Sink payload modes

`SINK_VALUE_FORMAT=auto` is the safe default while the format is unknown.

- valid plain JSON -> stored as a BSON object/array/value with `payload_format=json`
- valid UTF-8 but not JSON -> stored as `payload_format=raw_text`
- non-UTF8 binary -> stored losslessly as BSON Binary with `payload_format=raw_binary`
- Kafka tombstone (`None`) -> stored as `payload_format=kafka_null`

Once the source owner confirms that `product_view` is plain JSON, set:

```env
SINK_VALUE_FORMAT=json
```

Then non-JSON payloads are violations of the known data contract and are sent to the DLQ.

If the source is later confirmed as Avro/Protobuf, add the corresponding deserializer in `payload_codec.py`; the relay does not need to change.

## MongoDB design

MongoDB runs in Docker for local testing but has authentication enabled.

Two users are intentionally separated:

```text
root_admin  -> Mongo administration only
kafka_app   -> readWrite only on kafka_db
```

The Python sink uses `kafka_app`, not the root account. Production can replace the local URI with MongoDB Atlas or another Mongo cluster without changing the Python application logic.

Example document:

```javascript
{
  _event_id: "source:provided-kafka-a:product_view:2:123",
  _pipeline: {
    source: {
      system_id: "provided-kafka-a",
      topic: "product_view",
      partition: 2,
      offset: 123,
      timestamp_ms: 1700000000000
    },
    target: {
      topic: "product_view_local",
      partition: 1,
      offset: 88,
      timestamp_ms: 1700000000100
    },
    stored_at: ISODate("...")
  },
  message_key: BinData(...),
  message_headers: [...],
  payload_format: "json",
  payload: {
    product_id: "P100",
    price: 50
  }
}
```

The source payload is placed under `payload`, so source fields cannot overwrite pipeline metadata such as `_event_id` or `_pipeline`.

## Delivery semantics and idempotency

### Relay

```text
consume Kafka A
-> produce Kafka B
-> wait for Kafka B acknowledgement
-> commit Kafka A offset
```

If Kafka B delivery fails, Kafka A is not committed.

### Sink

```text
consume Kafka B
-> upsert MongoDB
-> commit Kafka B offset
```

MongoDB has a unique index on `_event_id`.

`_event_id` is derived from the original Kafka A identity:

```text
source:<source-system>:<topic>:<partition>:<offset>
```

This matters if the relay crashes after Kafka B accepted the event but before the Kafka A offset was committed. Kafka A can replay the same event, but MongoDB still sees the same `_event_id` and does not create a duplicate document.

## Local setup

### 1. Copy the environment file

```bash
cp .env.example .env
```

Set the actual provided Kafka A password in `.env`:

```env
SOURCE_KAFKA_PASSWORD=...
```

Do not commit `.env`.

The local Kafka B password is intentionally only a local development credential. Do not reuse it in production.

### 2. Prepare JAAS and the external Docker network

```bash
./scripts/prepare_local.sh
```

This:

1. reads `TARGET_KAFKA_USERNAME` / `TARGET_KAFKA_PASSWORD` from `.env`
2. creates `config/kafka/kafka_server_jaas.conf`
3. creates `streaming-network` if it does not already exist

The generated JAAS file is ignored by Git.

### 3. Start Kafka, AKHQ and MongoDB

```bash
docker compose up -d kafka-0 kafka-1 kafka-2 akhq mongo
```

Check:

```bash
docker compose ps
```

AKHQ:

```text
http://localhost:8180
```

Default local development login in `.env.example`:

```text
username: admin
password: admin
```

### 4. Create the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

### 5. Run tests

```bash
pytest
```

### 6. Create the target topic and DLQ

```bash
kafka-create-topics
```

It creates, idempotently:

```text
product_view_local
product_view_local_dlq
```

with 3 partitions and replication factor 3 by default.

You can verify both topics in AKHQ.

### 7. Inspect Kafka A before making schema assumptions

```bash
kafka-source-inspect
```

If it clearly reports `plain_json: yes`, you can later choose `SINK_VALUE_FORMAT=json`.

If it looks binary or possibly Schema Registry-framed, keep `auto` until the serialization contract/schema is confirmed.

### 8. Run the relay

Terminal / PyCharm run configuration 1:

```bash
kafka-relay
```

Expected logs include:

```text
Read source topic=product_view partition=... offset=...
Produced target topic=product_view_local partition=... offset=...
Committed source topic=product_view partition=... offset=...
```

Open AKHQ and inspect `product_view_local` to verify the records arrived in your Kafka B.

### 9. Run the Mongo sink

Terminal / PyCharm run configuration 2:

```bash
kafka-mongo-sink
```

Expected logs include:

```text
Mongo inserted event_id=source:provided-kafka-a:product_view:...
```

or, on replay:

```text
Mongo already-existed event_id=...
```

### 10. Verify MongoDB

```bash
./scripts/verify_local.sh
```

Or manually:

```bash
docker exec -it mongo-local mongosh \
  -u kafka_app \
  -p local-app-password \
  --authenticationDatabase kafka_db \
  kafka_db
```

Then:

```javascript
db.product_views.countDocuments({})
db.product_views.find({}).limit(5).pretty()
```

## Optional: run Python services in Docker too

Once local/PyCharm debugging works:

```bash
docker compose --profile apps up --build
```

The same code now uses the Docker network addresses:

```text
Kafka B: kafka-0:9092,kafka-1:9092,kafka-2:9092
MongoDB: mongo:27017
```

No application source code changes are needed.

## Production migration

The local infrastructure is for repeatable testing. The application design is the reusable part.

### Without Kubernetes

For example:

```text
relay container ----> AWS MSK / Confluent Cloud
sink container  ----> MongoDB Atlas
runtime           ----> ECS/Fargate / VM / systemd
secrets           ----> AWS Secrets Manager / Vault
logs              ----> CloudWatch / another central logger
```

### With Kubernetes

For example:

```text
Deployment: relay
Deployment: mongo-sink
Secrets / ConfigMaps
        |
        +----> managed Kafka
        +----> managed MongoDB
```

Kubernetes changes how containers are scheduled, restarted and scaled. It does not require rewriting `relay.py`, `sink.py`, or the payload/idempotency logic.

### What should change in production

- TLS, usually `SASL_SSL` rather than `SASL_PLAINTEXT`
- production credentials/secrets
- managed or properly operated multi-node Kafka/MongoDB
- monitoring and alerting
- retry/backoff policy and operational runbooks
- confirmed schema/data contract and Schema Registry integration when applicable
- CI/CD and environment-specific deployment configuration

## DoD evidence

For the assignment submission, capture at least:

1. GitHub repository with this project, excluding `.env` and generated JAAS credentials.
2. Relay terminal showing Kafka A records delivered to Kafka B and source offsets committed.
3. AKHQ screenshot showing records in `product_view_local`.
4. Sink terminal showing Mongo inserts/upserts.
5. `mongosh` output showing `product_views` documents and a non-zero count.

This directly demonstrates both required paths:

```text
Kafka A -> your Kafka B
```

and:

```text
your Kafka B -> MongoDB
```
