# Operational Data Store (ODS) Technology Stack

This document outlines the main technologies driving the ODS Proof of Concept architecture.

## Core Data & Streaming Layer

*   **PostgreSQL** (PostgreSQL License): Used for both the Source system (`core-banking-db`) to simulate the transactional database, and the Target system (`ods-db`) to act as the Operational Data Store.
*   **Debezium** (Apache License 2.0): Serves as the Change Data Capture (CDC) connector. It connects directly to the Postgres Write-Ahead Log (WAL) to detect and stream database row-level changes in real-time.
*   **Apache Kafka** (Apache License 2.0): The central message broker that receives the CDC event streams from Debezium and queues them in partitioned topics (e.g., `src.payments.transactions`).

## What is Debezium?

Debezium is an open-source distributed platform for Change Data Capture (CDC). In this architecture, it continuously monitors the source PostgreSQL database's Write-Ahead Log (WAL). Instead of relying on periodic polling or expensive queries, Debezium instantly detects every row-level `INSERT`, `UPDATE`, or `DELETE` operation as it happens. It then serializes the change into a structured event payload and streams it directly to Apache Kafka. This ensures the downstream Operational Data Store receives low-latency, strictly ordered data updates without impacting the performance of the source banking database.

## Processing & Tooling Layer

*   **Python** (Python Software Foundation License): The primary language used for building out the surrounding logic. This includes:
    *   **Stream Processors** (`transformer.py` and `raw_sink.py`) which consume event payloads from Kafka, perform necessary transformations, and bulk insert into the ODS Postgres instance.
    *   **Synthetic Load Generator** which injects high-volume bursts of transactions.
    *   **Latency Monitor** which empirically measures the end-to-end traversal time of the pipeline.
*   **FastAPI / Uvicorn** (MIT License / BSD License): Used to run the live dashboard backend (`src.dashboard.app:app`), utilizing Server-Sent Events (SSE) to push and display real-time pipeline KPIs to the frontend.

## Infrastructure

*   **Docker & Docker Compose** (Apache License 2.0): Entirely containerizes the architecture. This allows the source database, ODS database, Zookeeper, Kafka broker, Debezium connect service, and the horizontally scaled Python stream processors to easily run locally and mirror a production environment.
