# Design Decisions & Latency Optimization Options

This document details the options, recommendations, and decisions for optimizing the latency of the three architecture approaches for the Operational Data Store (ODS). It also provides a concrete implementation plan.

## 1. Baseline (Single Stream Processor)

### Options
1.  **Batch Processing (Recommended):** Instead of processing and committing one record at a time, fetch multiple messages from Kafka and execute a single bulk `INSERT` using `psycopg2.extras.execute_values()`. Set `conn.autocommit = False`.
2.  **Asynchronous Database Driver:** Replace `psycopg2` with `asyncpg` to unblock the thread during I/O operations.
3.  **Disable Granular Logging (Recommended):** Remove `logger.info()` logs for every message as Python I/O is a significant bottleneck. 
4.  **Prepared Statements:** Pre-compile queries to save Postgres parsing time.

### Design Decision
**Implement Batch Processing and Disable Granular Logging.**
*Rationale:* Batching provides the highest return on investment for reducing I/O and network round-trips without needing to completely rewrite the application logic to be asynchronous. Removing granular logs eliminates local I/O bottlenecks.

---

## 2. Distributed Stream Processing (Approach 1)

### Options
1.  **Inherit Baseline Optimizations (Recommended):** Apply the batching and logging optimizations from the Baseline.
2.  **Kafka Consumer Tuning (Recommended):** Adjust `fetch.min.bytes` and `fetch.max.wait.ms` to increase the payload size per consumer fetch.
3.  **Rewrite in a Performant Language:** Migrate the `transformer` logic from Python to Java (Kafka Streams), Rust, or Go to bypass Python's Global Interpreter Lock (GIL).

### Design Decision
**Inherit Baseline Optimizations and Tune Kafka.**
*Rationale:* Rewriting the codebase in a new language introduces too much risk and overhead for a PoC. Scaling Python workers horizontally with batched database inserts will easily resolve the latency issues for this architecture model.

---

## 3. Data Virtualization (Approach 3)

### Options
1.  **Batch Ingestion (Recommended):** Update `raw_sink.py` to batch inserts using `execute_values`, exactly like the Baseline approach.
2.  **Materialized Views:** Convert `bcdm.virtual_event` to a `MATERIALIZED VIEW` that refreshes periodically via `pg_cron`.
3.  **Remove MD5 UUID Casting (Recommended):** Drop `md5(txn_id::text)::uuid` from the view and use the raw integer IDs if the downstream systems permit it.
4.  **Database Indexing (Recommended):** Add indexes to `raw.payments_transactions` (e.g., on `txn_time`) to optimize the query planner's filtering operations through the view.

### Design Decision
**Implement Batch Ingestion, Remove MD5 Casting, and Add Indexes.**
*Rationale:* A Materialized View sacrifices the "real-time" aspect of the PoC, making it eventually consistent. Dropping the expensive MD5 hashing compute during read operations keeps the view real-time while drastically lowering read latency. Adding indexes will further optimize the read path.

---

## Implementation Plan & Prompts

The following prompts can be used to execute the chosen design decisions for each experiment sequentially.

### Step 1: Optimize Baseline & Distributed Stream Processors
**Target:** `src/transformer/transformer.py`
**Prompt:**
> Please optimize `src/transformer/transformer.py` to reduce database ingestion latency. 
> 1. Disable `autocommit` on the PostgreSQL connection.
> 2. Modify the Kafka polling loop to collect batches of up to 500 messages using `consumer.consume(num_messages=500, timeout=1.0)`.
> 3. Implement bulk inserts using `psycopg2.extras.execute_values()` for both the client customers and payments transactions. Commit the transaction after each batch.
> 4. Comment out or lower the logging level for the per-record transformation logs (e.g., `logger.info(f"🔄 Transformed...")`).

### Step 2: Optimize the Data Virtualization Ingestion Sink
**Target:** `src/raw_sink/raw_sink.py`
**Prompt:**
> Please optimize `src/raw_sink/raw_sink.py` for high-throughput batch ingestion.
> 1. Disable `autocommit`.
> 2. Update the `consumer.poll()` logic to pull batches of up to 500 messages at a time using `consumer.consume()`.
> 3. Use `psycopg2.extras.execute_values()` to insert the batch of raw records into the `raw.payments_transactions` table in a single transaction. Commit the batch.

### Step 3: Optimize the Data Virtualization Read-Path View
**Target:** `init-scripts/setup_approach3.sql`
**Prompt:**
> Please optimize `init-scripts/setup_approach3.sql` to lower read latency on the virtual view.
> 1. In `bcdm.virtual_event`, replace the costly `md5(txn_id::text)::uuid` generation with just returning the `txn_id::text` as the `event_id`.
> 2. Add standard B-Tree indexes to the `raw.payments_transactions` table for `txn_time` and `currency` to optimize downstream queries filtering by time or currency.
