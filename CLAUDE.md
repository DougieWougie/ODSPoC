# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A Proof of Concept for a near real-time, event-driven **Operational Data Store (ODS)** for a banking architecture. It uses PostgreSQL (source + ODS), Debezium (CDC), Apache Kafka, and Python to demonstrate three different architectural approaches to stream processing and data transformation.

## Running experiments

All infrastructure runs in Docker. The `fresh_test.sh` script is the primary entrypoint — it handles venv setup, starts services, initialises databases, registers Debezium, and runs measurements:

```bash
./fresh_test.sh A   # Single Node Bottleneck (1 transformer)
./fresh_test.sh B   # Distributed Stream Processing (10 transformers, 10 Kafka partitions)
./fresh_test.sh C   # Data Virtualization (SQL views, no transformers)
./teardown.sh       # Full teardown: containers, volumes, venv, logs
```

To start infrastructure without running an experiment:
```bash
docker compose -f infrastructure/docker-compose.yaml up -d --build
docker compose -f infrastructure/docker-compose.yaml down -v
```

## Running tests

Tests live in `src/transformer/` and must be run from that directory (they do a local import of `transformer`):

```bash
cd src/transformer && python -m pytest test_transformer.py -v
```

The test file mocks both `psycopg2` and `confluent_kafka` at the module level before importing `transformer.py`, because `transformer.py` opens DB connections at import time.

## Generating synthetic load

```bash
./venv/bin/python src/tools/generator.py      # 10 concurrent threads, floods payments.transactions
./venv/bin/python src/tools/measure_latency.py  # Polls and prints end-to-end latency
```

These are intended to be run *while* an experiment is active, not before setup.

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

**BCDM** (Barclays Conceptual Data Model) is the canonical target schema. `transformer.py` maps source Debezium CDC payloads (`op: c/u/r`) to BCDM entities. Delete operations (`op: d`) are intentionally ignored.

**Experiment C** bypasses the transformer entirely: `raw_sink.py` writes un-transformed records into landing tables, and SQL views in `init-scripts/setup_approach3.sql` perform column mapping on read.

## Key ports

| Service        | Port  |
|----------------|-------|
| core-banking-db | 5432 |
| ods-db          | 5433 |
| Kafka           | 9092 |
| Debezium REST   | 8083 |


## Python environment

Dependencies are managed via `requirements.txt` (FastAPI, uvicorn, psycopg2-binary, confluent-kafka). The venv is at `./venv/` and is created/destroyed by `fresh_test.sh` / `teardown.sh`. Install manually with:

```bash
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
```
