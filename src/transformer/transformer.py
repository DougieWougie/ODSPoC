import json
import logging
import uuid
import psycopg2
import psycopg2.extras
import time
from confluent_kafka import Consumer, KafkaError

# Configure basic logging, set to WARNING to reduce I/O overhead from per-message logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to the ODS database
def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host="ods-db",
                port="5432",
                user="admin",
                password="password",
                dbname="ods"
            )
            conn.autocommit = False # Disabled for batch processing
            logger.warning("Connected to ODS Database.") # Keep as warning so we see it
            return conn
        except psycopg2.OperationalError:
            logger.warning("Waiting for ODS DB to become available...")
            time.sleep(3)

ods_conn = get_db_connection()
cursor = ods_conn.cursor()

def process_batch(messages):
    client_customers = []
    payments_transactions = []
    
    for msg in messages:
        if msg is None or msg.error():
            continue
            
        try:
            val_str = msg.value().decode('utf-8')
            val = json.loads(val_str)
            
            payload = val.get('payload')
            if not payload:
                continue
                
            op = payload.get('op')
            # We only care about creates ('c') and updates ('u') for this demo, ignoring deletes ('d')
            if op not in ('c', 'u', 'r'): # 'r' is for snapshot reads
                continue
                
            after = payload.get('after')
            if not after:
                continue
                
            topic = msg.topic()
            
            # Route to appropriate transformation list
            if topic == "src.client.customers":
                client_customers.append((
                    str(uuid.uuid4()),
                    'INDIVIDUAL',
                    after.get('first_name'),
                    after.get('last_name'),
                    'CORE_BANKING_CLIENT',
                    str(after.get('customer_id'))
                ))
            elif topic == "src.payments.transactions":
                payments_transactions.append((
                    str(uuid.uuid4()),
                    'PAYMENT_TRANSACTION',
                    after.get('amount'),
                    after.get('currency'),
                    'CORE_BANKING_PAYMENTS',
                    str(after.get('txn_id'))
                ))
            else:
                pass
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    try:
        if client_customers:
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO bcdm.party (party_id, party_type, first_name, last_name, source_system, source_record_id)
                VALUES %s
                """,
                client_customers
            )
        
        if payments_transactions:
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO bcdm.event (event_id, event_type, event_amount, currency, source_system, source_record_id)
                VALUES %s
                """,
                payments_transactions
            )
            
        if client_customers or payments_transactions:
            ods_conn.commit()
            
    except Exception as e:
        logger.error(f"Error inserting batch: {e}")
        ods_conn.rollback()

def main():
    consumer = Consumer({
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'bcdm-transformer-group',
        'auto.offset.reset': 'earliest' # Start from the beginning if no offset exists
    })

    # Subscribe to the Kafka topics populated by Debezium
    topics = [
        'src.client.customers',
        'src.crm.interactions',
        'src.payments.transactions',
        'src.lending.loans'
    ]
    consumer.subscribe(topics)

    logger.warning(f"Starting BCDM Transformer Service... Listening on {topics}")

    try:
        while True:
            # Consume messages in batches of up to 500
            messages = consumer.consume(num_messages=500, timeout=1.0)
            if not messages:
                continue

            process_batch(messages)

    finally:
        consumer.close()
        cursor.close()
        ods_conn.close()

if __name__ == '__main__':
    # Wait briefly for Kafka to be fully ready before subscribing
    time.sleep(5)
    main()
