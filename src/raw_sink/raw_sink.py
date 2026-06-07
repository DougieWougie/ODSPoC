import json
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer

# Fast raw sink: No data mapping, no UUID generation, no transformation logic.
ods_conn = psycopg2.connect(host="localhost", port="5433", user="admin", password="password", dbname="ods")
ods_conn.autocommit = False # Disabled for batch processing
cursor = ods_conn.cursor()

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'raw-sink-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['src.payments.transactions'])

print("Starting Raw Data Sink (Approach 3)...")

while True:
    messages = consumer.consume(num_messages=500, timeout=1.0)
    if not messages:
        continue

    batch = []
    for msg in messages:
        if msg is None or msg.error():
            continue

        val = json.loads(msg.value().decode('utf-8'))
        payload = val.get('payload')
        if not payload or payload.get('op') not in ('c', 'u', 'r'): 
            continue
            
        after = payload.get('after')
        if after:
            batch.append((
                after.get('txn_id'),
                after.get('sender_account_id'),
                after.get('receiver_account_id'),
                after.get('amount'),
                after.get('currency'),
                after.get('txn_time')
            ))

    if batch:
        try:
            # Batch insert straight into the raw landing table
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO raw.payments_transactions (txn_id, sender_account_id, receiver_account_id, amount, currency, txn_time)
                VALUES %s
                ON CONFLICT (txn_id) DO NOTHING
                """,
                batch,
                template="(%s, %s, %s, %s, %s, to_timestamp(%s / 1000000.0))"
            )
            ods_conn.commit()
        except Exception as e:
            print(f"Error: {e}")
            ods_conn.rollback()
