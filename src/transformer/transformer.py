import json
import logging
import uuid
import psycopg2
import time
from confluent_kafka import Consumer, KafkaError

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
            conn.autocommit = True
            logger.info("Connected to ODS Database.")
            return conn
        except psycopg2.OperationalError:
            logger.warning("Waiting for ODS DB to become available...")
            time.sleep(3)

ods_conn = get_db_connection()
cursor = ods_conn.cursor()

def process_client_customer(after_payload):
    """ Maps a source client.customers record to the BCDM Party entity """
    party_id = str(uuid.uuid4())
    cursor.execute("""
        INSERT INTO bcdm.party (party_id, party_type, first_name, last_name, source_system, source_record_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        party_id,
        'INDIVIDUAL',
        after_payload.get('first_name'),
        after_payload.get('last_name'),
        'CORE_BANKING_CLIENT',
        str(after_payload.get('customer_id'))
    ))
    logger.info(f"🔄 Transformed Customer {after_payload.get('customer_id')} -> BCDM Party (ID: {party_id})")

def process_payments_transaction(after_payload):
    """ Maps a source payments.transactions record to the BCDM Event entity """
    event_id = str(uuid.uuid4())
    cursor.execute("""
        INSERT INTO bcdm.event (event_id, event_type, event_amount, currency, source_system, source_record_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        event_id,
        'PAYMENT_TRANSACTION',
        after_payload.get('amount'),
        after_payload.get('currency'),
        'CORE_BANKING_PAYMENTS',
        str(after_payload.get('txn_id'))
    ))
    logger.info(f"🔄 Transformed Transaction {after_payload.get('txn_id')} -> BCDM Event (ID: {event_id})")

def process_message(msg):
    try:
        val_str = msg.value().decode('utf-8')
        val = json.loads(val_str)
        
        # Debezium payloads are wrapped in a 'payload' object
        payload = val.get('payload')
        if not payload:
            return
            
        op = payload.get('op')
        # We only care about creates ('c') and updates ('u') for this demo, ignoring deletes ('d')
        if op not in ('c', 'u', 'r'): # 'r' is for snapshot reads
            return
            
        after = payload.get('after')
        if not after:
            return
            
        topic = msg.topic()
        
        # Route to appropriate transformation function
        if topic == "src.client.customers":
            process_client_customer(after)
        elif topic == "src.payments.transactions":
            process_payments_transaction(after)
        else:
            # Placeholder for CRM and Lending domains
            logger.info(f"Received message on {topic}, transformation not yet mapped.")
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")

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

    logger.info(f"Starting BCDM Transformer Service... Listening on {topics}")

    try:
        while True:
            # Poll for new messages every 1 second
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                else:
                    logger.error(msg.error())
                    continue

            process_message(msg)

    finally:
        consumer.close()
        cursor.close()
        ods_conn.close()

if __name__ == '__main__':
    # Wait briefly for Kafka to be fully ready before subscribing
    time.sleep(5)
    main()
