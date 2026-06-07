import argparse
import os
import psycopg2
import random
import threading
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inject synthetic banking transactions into core-banking-db.",
    )
    parser.add_argument(
        "-n", "--records",
        type=int,
        default=int(os.getenv("GENERATOR_RECORDS", 2000)),
        metavar="N",
        help="Total number of transactions to insert (default: 2000).",
    )
    parser.add_argument(
        "-t", "--threads",
        type=int,
        default=int(os.getenv("GENERATOR_THREADS", 10)),
        metavar="T",
        help="Number of concurrent DB connections (default: 10).",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("DB_SOURCE_HOST", "127.0.0.1"),
        help="core-banking-db host (default: $DB_SOURCE_HOST or 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        default=os.getenv("DB_SOURCE_PORT", "5432"),
        help="core-banking-db port (default: $DB_SOURCE_PORT or 5432).",
    )
    parser.add_argument(
        "--dbname",
        default=os.getenv("DB_SOURCE_NAME", "core_banking"),
        help="Database name (default: $DB_SOURCE_NAME or core_banking).",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("DB_USER", "admin"),
        help="Database user (default: $DB_USER or admin).",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("DB_PASSWORD", "password"),
        help="Database password (default: $DB_PASSWORD or password).",
    )
    return parser.parse_args()


def generate_data(thread_id: int, records_per_thread: int, dsn: dict) -> None:
    conn = psycopg2.connect(**dsn)
    conn.autocommit = True
    cursor = conn.cursor()
    for _ in range(records_per_thread):
        sender = random.randint(100, 999)
        receiver = random.randint(100, 999)
        amount = round(random.uniform(10.0, 5000.0), 2)
        cursor.execute(
            "INSERT INTO payments.transactions "
            "(sender_account_id, receiver_account_id, amount, currency) "
            "VALUES (%s, %s, %s, 'GBP')",
            (sender, receiver, amount),
        )
    cursor.close()
    conn.close()
    print(f"Thread {thread_id} finished inserting {records_per_thread} records.")


def main() -> None:
    args = parse_args()

    dsn = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password,
    }

    print(
        f"Starting generation of {args.records} transactions "
        f"across {args.threads} concurrent threads..."
    )
    start = time.time()

    records_per_thread, remainder = divmod(args.records, args.threads)
    threads = []
    for i in range(args.threads):
        # Distribute any remainder across the first threads
        count = records_per_thread + (1 if i < remainder else 0)
        t = threading.Thread(target=generate_data, args=(i, count, dsn))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    elapsed = time.time() - start
    print(
        f"Done. Total time: {elapsed:.2f}s  "
        f"Rate: {args.records / elapsed:.0f} transactions/sec"
    )


if __name__ == "__main__":
    main()
