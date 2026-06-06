# Event-Driven Operational Data Store (ODS)

This repository contains a Proof of Concept (PoC) for a near real-time, event-driven Operational Data Store tailored for a banking architecture. It utilizes PostgreSQL, Debezium, Apache Kafka, and Python.

## Prerequisites
- Docker and Docker Compose
- Python 3.10+
- `psycopg2-binary` and `confluent-kafka` Python packages

## 1. Initial Setup
Start the infrastructure (Postgres, Zookeeper, Kafka, Debezium, and 1 Transformer):
```bash
cd infrastructure
docker compose up -d
```

Once running, register the Debezium Postgres Source Connector:
```bash
jq '.config' connectors/register-postgres-source.json > config.json
curl -X POST -H "Content-Type: application/json" -d @config.json http://localhost:8083/connectors
```

Set up your local Python environment to run the measurement tools:
```bash
python3 -m venv venv
source venv/bin/activate
pip install psycopg2-binary confluent-kafka
```

---

## 2. Generating Synthetic Data
To properly evaluate the architecture, we need a mechanism to simulate high volumes of banking events (e.g., millions of payment transactions). 

We have provided `generator.py` for this purpose. This script utilizes Python's `threading` module to create 10 concurrent connections to the `core-banking-db`, injecting thousands of random `payments.transactions` instantly.

*Note: You do not need to run this script manually before starting the experiments. Instead, you will execute this script **during** the experiments to trigger a sudden "burst" of data, allowing you to measure exactly how the architecture handles the sudden load spike.*

---

## 3. Experiment A: Single Node Bottleneck
**Goal:** Demonstrate how a single-threaded stream processor handles a burst of transactions.

1. Ensure only 1 transformer is running: `docker compose up --scale transformer=1 -d`
2. Run the latency monitor:
   ```bash
   ./venv/bin/python src/tools/measure_latency.py
   ```
3. In a separate terminal, flood the system with 2,000 transactions:
   ```bash
   ./venv/bin/python src/tools/generator.py
   ```
4. **Observe:** The latency monitor will show the queue backing up, with maximum latency reaching ~2.5 seconds.

---

## 4. Experiment B: Distributed Stream Processing (Approach 1)
**Goal:** Demonstrate horizontal scaling by partitioning the Kafka topic and adding stream processors.

1. Partition the Kafka topic to allow parallel consumption:
   ```bash
   docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic src.payments.transactions --partitions 10
   ```
2. Scale the stream processors to 10 nodes:
   ```bash
   docker compose up --scale transformer=10 -d
   ```
3. Re-run the monitor and the generator exactly as in Experiment A.
4. **Observe:** The 10 containers will process the burst in parallel, reducing maximum latency to ~0.5 seconds.

---

## 5. Experiment C: Data Virtualization (Approach 3)
**Goal:** Demonstrate the impact of bypassing stream processing and using SQL Views for on-the-fly transformation.

1. Stop the stream processors: `docker compose up --scale transformer=0 -d`
2. Create the raw landing tables and BCDM Virtual Views in the ODS:
   ```bash
   docker compose exec -T ods-db psql -U admin -d ods < ../init-scripts/setup_approach3.sql
   ```
3. Start the raw sink connector (which performs no transformations):
   ```bash
   ./venv/bin/python src/raw_sink/raw_sink.py &
   ```
4. Run the generator to flood the system. Ingestion latency will remain at ~2.5s because the database insertion bottleneck remains.
5. **Measure Read Latency:** Compare the execution time of reading from the physical table versus the virtual view:
   ```bash
   docker compose exec -T ods-db psql -U admin -d ods -c "EXPLAIN ANALYZE SELECT * FROM bcdm.event;"
   docker compose exec -T ods-db psql -U admin -d ods -c "EXPLAIN ANALYZE SELECT * FROM bcdm.virtual_event;"
   ```
6. **Observe:** The virtual view takes significantly longer to read due to on-the-fly column mappings and UUID generation.
