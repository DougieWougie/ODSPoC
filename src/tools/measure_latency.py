import psycopg2
import statistics
import time
from datetime import datetime

LOST_THRESHOLD_SECS = 30  # missing records older than this are "potentially lost" rather than in-flight


def percentile(sorted_data, p):
    k = (len(sorted_data) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(sorted_data) - 1)
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * (k - lo)


def _measure_table(core_cur, ods_cur, label, core_sql, core_id_col, ods_table, source_system):
    core_cur.execute(core_sql)
    core_records = core_cur.fetchall()
    if not core_records:
        print(f"[{label}] No source records found.", flush=True)
        return

    core_ids = [r[0] for r in core_records]

    ods_cur.execute(
        f"SELECT source_record_id::int, integration_timestamp FROM {ods_table} "
        f"WHERE source_system=%s AND source_record_id::int = ANY(%s)",
        (source_system, core_ids),
    )
    ods_dict = {r[0]: r[1] for r in ods_cur.fetchall()}

    latencies = []
    in_flight = 0
    potentially_lost = 0
    now = datetime.now()

    for record_id, source_time in core_records:
        if record_id in ods_dict:
            latencies.append((ods_dict[record_id] - source_time).total_seconds())
        else:
            age = (now - source_time).total_seconds()
            if age < LOST_THRESHOLD_SECS:
                in_flight += 1
            else:
                potentially_lost += 1

    if not latencies:
        print(
            f"[{label}] No matched records | In-flight: {in_flight} | "
            f"Potentially lost (>{LOST_THRESHOLD_SECS}s): {potentially_lost}",
            flush=True,
        )
        return

    s = sorted(latencies)
    avg = sum(latencies) / len(latencies)
    stddev = statistics.stdev(latencies) if len(latencies) > 1 else 0.0

    print(
        f"[{label}] "
        f"Matched: {len(latencies)} | "
        f"In-flight: {in_flight} | "
        f"Potentially lost (>{LOST_THRESHOLD_SECS}s): {potentially_lost} | "
        f"Latency — "
        f"Avg: {avg:.3f}s  "
        f"P50: {percentile(s, 50):.3f}s  "
        f"P95: {percentile(s, 95):.3f}s  "
        f"P99: {percentile(s, 99):.3f}s  "
        f"Max: {s[-1]:.3f}s  "
        f"StdDev: {stddev:.3f}s",
        flush=True,
    )


def measure():
    core_conn = ods_conn = None
    try:
        core_conn = psycopg2.connect(host="localhost", port="5432", user="admin", password="password", dbname="core_banking")
        ods_conn = psycopg2.connect(host="localhost", port="5433", user="admin", password="password", dbname="ods")
        core_cur = core_conn.cursor()
        ods_cur = ods_conn.cursor()

        _measure_table(
            core_cur, ods_cur,
            label="Transactions",
            core_sql="SELECT txn_id, txn_time FROM payments.transactions ORDER BY txn_id DESC LIMIT 1000",
            core_id_col="txn_id",
            ods_table="bcdm.event",
            source_system="CORE_BANKING_PAYMENTS",
        )
        _measure_table(
            core_cur, ods_cur,
            label="Customers   ",
            core_sql="SELECT customer_id, created_at FROM client.customers ORDER BY customer_id DESC LIMIT 1000",
            core_id_col="customer_id",
            ods_table="bcdm.party",
            source_system="CORE_BANKING_CLIENT",
        )

    except Exception as e:
        print(f"Error: {e}", flush=True)
    finally:
        if core_conn:
            core_conn.close()
        if ods_conn:
            ods_conn.close()


if __name__ == "__main__":
    print("Monitoring Latency End-to-End (Source DB -> Debezium -> Kafka -> Transformer -> ODS)...")
    for _ in range(20):
        measure()
        time.sleep(2)
