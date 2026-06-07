# Event-Driven Operational Data Store (ODS)

This repository contains a Proof of Concept (PoC) for a near real-time, event-driven Operational Data Store tailored for a banking architecture. It uses PostgreSQL, Debezium, Apache Kafka, and Python.

## Prerequisites

- Docker and Docker Compose
- Python 3.10+
- `jq` and `curl`

---

---

## Running the Real-Time Dashboard

The dashboard provides a live visual of the full pipeline — Source DB → Debezium → Kafka → Transformer → ODS — including animated data flow, latency trends, and a transaction injection button suitable for executive presentations.

**Requires the infrastructure to be running first** (see Initial Setup below).

```bash
# Install dashboard dependencies (first time only)
source venv/bin/activate
pip install fastapi uvicorn requests

# Start the dashboard
cd src/dashboard
uvicorn app:app --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000** in a browser. The dashboard will:
- Poll all five pipeline stages every 2 seconds via Server-Sent Events
- Animate particles along the pipeline proportional to live throughput
- Display real-time KPIs: in-flight records, end-to-end latency, throughput, and total ODS records
- Allow transaction injection (25 / 50 / 100 / 500) directly from the UI

To stop the dashboard: `Ctrl+C` in the terminal running `uvicorn`.

## 1. Quick Start

The `fresh_test.sh` script automates the full setup. Pass the experiment you want to run as an argument:

```bash
./fresh_test.sh A   # Single Node Bottleneck
./fresh_test.sh B   # Distributed Stream Processing
./fresh_test.sh C   # Data Virtualization
```

Or run it without arguments for an interactive menu:

```bash
./fresh_test.sh
```

For any experiment it will:
1. Check that `docker`, `jq`, and `curl` are available
2. Create a Python `venv` and install dependencies from `requirements.txt` — including `psycopg2-binary` and `confluent-kafka` — (if not already present)
3. Start all Docker services and wait for them to be healthy
4. Initialise both databases from the `init-scripts/` directory
5. Wait for the Debezium REST API and register the Postgres source connector
6. Configure the infrastructure for the chosen experiment and run the appropriate measurements

To stop everything and return to a clean state:

```bash
./teardown.sh
```

This removes all containers, volumes, generated logs, temporary files, and the `venv` directory.

---

## 2. Generating Synthetic Data

`src/tools/generator.py` simulates high volumes of banking events. It uses Python's `threading` module to open 10 concurrent connections to `core-banking-db` and inject thousands of random `payments.transactions`.

> **Note:** You do not need to run this manually before an experiment. Run it *during* an experiment to trigger a sudden data burst and measure how the architecture responds under load.

---

## 3. Experiment A: Single Node Bottleneck

**Goal:** Demonstrate how a single-threaded stream processor handles a burst of transactions.

1. Ensure only 1 transformer is running:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml up --scale transformer=1 -d
   ```
2. Run the latency monitor:
   ```bash
   ./venv/bin/python src/tools/measure_latency.py
   ```
3. In a separate terminal, flood the system with transactions:
   ```bash
   ./venv/bin/python src/tools/generator.py
   ```
4. **Observe:** The latency monitor will show the queue backing up, with maximum latency reaching ~2.5 seconds.

---

## 4. Experiment B: Distributed Stream Processing (Approach 1)

**Goal:** Demonstrate horizontal scaling by partitioning the Kafka topic and adding stream processors.

> Run automatically via `./fresh_test.sh B`. Follow the steps below to run the individual steps manually.

1. Partition the Kafka topic to allow parallel consumption:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml exec -T kafka \
     kafka-topics --bootstrap-server kafka:29092 \
     --alter --topic src.payments.transactions --partitions 10
   ```
2. Scale the stream processors to 10 nodes:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml up --scale transformer=10 -d
   ```
3. Re-run the monitor and the generator exactly as in Experiment A.
4. **Observe:** The 10 containers process the burst in parallel, reducing maximum latency to ~0.5 seconds.

---

## 5. Experiment C: Data Virtualization (Approach 3)

**Goal:** Demonstrate the impact of bypassing stream processing and using SQL Views for on-the-fly transformation.

1. Stop the stream processors:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml up --scale transformer=0 -d
   ```
2. Create the raw landing tables and BCDM virtual views in the ODS:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml exec -T ods-db \
     psql -U admin -d ods < init-scripts/setup_approach3.sql
   ```
3. Start the raw sink connector (performs no transformations):
   ```bash
   ./venv/bin/python src/raw_sink/raw_sink.py &
   ```
4. Run the generator to flood the system. Ingestion latency will remain at ~2.5s because the database insertion bottleneck remains.
5. **Measure Read Latency** — compare physical table vs. virtual view:
   ```bash
   docker compose -f infrastructure/docker-compose.yaml exec -T ods-db \
     psql -U admin -d ods -c "EXPLAIN ANALYZE SELECT * FROM bcdm.event;"

   docker compose -f infrastructure/docker-compose.yaml exec -T ods-db \
     psql -U admin -d ods -c "EXPLAIN ANALYZE SELECT * FROM bcdm.virtual_event;"
   ```
6. **Observe:** The virtual view takes significantly longer to read due to on-the-fly column mappings and UUID generation.
