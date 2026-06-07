# Event-Driven Operational Data Store (ODS)

A Proof of Concept for a near real-time, event-driven Operational Data Store for a banking architecture. Demonstrates three approaches to stream processing using PostgreSQL, Debezium (CDC), Apache Kafka, and Python.

## Architecture

```
core-banking-db (Postgres, :5432)
    └── Debezium (:8083) — CDC via logical replication
            └── Kafka (:9092) — topics: src.client.customers, src.payments.transactions, etc.
                    ├── transformer (Docker service, scalable)
                    │       └── Maps Debezium payloads → BCDM schema in ods-db
                    └── raw_sink.py (Experiment C only)
                            └── Writes raw records; SQL views do transformation on read

ods-db (Postgres, :5433) — schema: bcdm
    ├── bcdm.party        (customers → INDIVIDUAL Party)
    ├── bcdm.arrangement  (accounts/loans)
    └── bcdm.event        (transactions → PAYMENT_TRANSACTION Event)
```

**BCDM** (Barclays Conceptual Data Model) is the canonical target schema. The `transformer` maps Debezium CDC payloads (`op: c/u/r`) to BCDM entities. Delete operations (`op: d`) are ignored.

### Key Ports

| Service         | Port |
|-----------------|------|
| core-banking-db | 5432 |
| ods-db          | 5433 |
| Kafka           | 9092 |
| Debezium REST   | 8083 |


---

## Prerequisites

- Docker and Docker Compose
- Python 3.10+
- `jq` and `curl`

---

## Setup

Copy the environment template and fill in your values:

```bash
cp .env.example .env
```

---

## Quick Start

`fresh_test.sh` is the primary entrypoint. It handles venv creation, starts all Docker services, initialises both databases, registers the Debezium connector, and runs measurements.

```bash
./fresh_test.sh A   # Experiment A: Single Node Bottleneck
./fresh_test.sh B   # Experiment B: Distributed Stream Processing
./fresh_test.sh C   # Experiment C: Data Virtualization
```

Run without arguments for an interactive menu:

```bash
./fresh_test.sh
```

To stop everything and return to a clean state:

```bash
./teardown.sh
```

This removes all containers, volumes, generated logs, and the `venv` directory.

---

## Manual Setup (without `fresh_test.sh`)

### 1. Python environment

```bash
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
```

### 2. Start infrastructure

```bash
docker compose -f infrastructure/docker-compose.yaml up -d --build
```

### 3. Initialise databases

```bash
docker compose -f infrastructure/docker-compose.yaml exec -T core-banking-db \
  psql -U admin -d core_banking < init-scripts/init-source.sql

docker compose -f infrastructure/docker-compose.yaml exec -T ods-db \
  psql -U admin -d ods < init-scripts/init-ods.sql
```

### 4. Register the Debezium connector

Wait for the Debezium REST API to be ready, then register the connector:

```bash
curl -sf -X POST -H "Content-Type: application/json" \
  -d @infrastructure/connectors/register-postgres-source.json \
  http://localhost:8083/connectors | jq .
```

---

## Experiments

### Experiment A: Single Node Bottleneck

**Goal:** Demonstrate how a single-threaded stream processor handles a burst of transactions.

1. Scale to 1 transformer:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml up --scale transformer=1 -d
   ```
2. Start the latency monitor:
   ```bash
   ./venv/bin/python src/tools/measure_latency.py
   ```
3. In a separate terminal, flood the system:
   ```bash
   ./venv/bin/python src/tools/generator.py
   ```
4. **Observe:** The queue backs up, with maximum latency reaching ~2.5 seconds.

---

### Experiment B: Distributed Stream Processing

**Goal:** Demonstrate horizontal scaling by partitioning the Kafka topic and adding stream processors.

1. Repartition the Kafka topic:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml exec -T kafka \
     kafka-topics --bootstrap-server kafka:29092 \
     --alter --topic src.payments.transactions --partitions 10
   ```
2. Scale to 10 transformers:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml up --scale transformer=10 -d
   ```
3. Re-run the monitor and generator as in Experiment A.
4. **Observe:** 10 containers process the burst in parallel, reducing maximum latency to ~0.5 seconds.

---

### Experiment C: Data Virtualization

**Goal:** Demonstrate the trade-off of bypassing stream processing and using SQL views for on-the-fly transformation.

1. Stop the stream processors:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml up --scale transformer=0 -d
   ```
2. Create landing tables and BCDM virtual views in the ODS:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml exec -T ods-db \
     psql -U admin -d ods < init-scripts/setup_approach3.sql
   ```
3. Start the raw sink:
   ```bash
   ./venv/bin/python src/raw_sink/raw_sink.py &
   ```
4. Flood the system:
   ```bash
   ./venv/bin/python src/tools/generator.py
   ```
5. Compare physical table vs. virtual view read latency:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml exec -T ods-db \
     psql -U admin -d ods -c "EXPLAIN ANALYZE SELECT * FROM bcdm.event;"

   docker compose -f infrastructure/docker-compose.yaml exec -T ods-db \
     psql -U admin -d ods -c "EXPLAIN ANALYZE SELECT * FROM bcdm.virtual_event;"
   ```
6. **Observe:** The virtual view takes significantly longer due to on-the-fly column mapping and UUID generation.

---

## Dashboard

The dashboard provides a live view of the full pipeline — Source DB → Debezium → Kafka → Transformer → ODS — with animated data flow, latency trends, and a transaction injection button.

> **Infrastructure must be running** before starting the dashboard.

```bash
source venv/bin/activate
uvicorn src.dashboard.app:app --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000** in a browser. The dashboard polls all pipeline stages every 2 seconds via Server-Sent Events and displays live KPIs: in-flight records, end-to-end latency, throughput, and total ODS record count.

To stop: `Ctrl+C` in the terminal running `uvicorn`.

---

## Generating Synthetic Load

`src/tools/generator.py` opens 10 concurrent connections to `core-banking-db` and injects random `payments.transactions`. Run it **during** an experiment (not before setup) to trigger a load burst:

```bash
./venv/bin/python src/tools/generator.py
```

---

## Tests

Tests live in `src/transformer/` and must be run from that directory:

```bash
cd src/transformer && python -m pytest test_transformer.py -v
```
