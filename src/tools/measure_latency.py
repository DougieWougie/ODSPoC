import psycopg2
import time
import sys

def measure():
    try:
        core_conn = psycopg2.connect(host="localhost", port="5432", user="admin", password="password", dbname="core_banking")
        ods_conn = psycopg2.connect(host="localhost", port="5433", user="admin", password="password", dbname="ods")
        
        core_cur = core_conn.cursor()
        ods_cur = ods_conn.cursor()
        
        # Get the latest 1000 transactions from the source system
        core_cur.execute("SELECT txn_id, txn_time FROM payments.transactions ORDER BY txn_id DESC LIMIT 1000")
        core_records = core_cur.fetchall()
        
        # Get all transformed records from the ODS (now querying the view)
        ods_cur.execute("SELECT source_record_id::int, integration_timestamp FROM bcdm.event WHERE source_system='CORE_BANKING_PAYMENTS'")
        ods_records = ods_cur.fetchall()
        
        # Create a dictionary for O1 lookup
        ods_dict = {r[0]: r[1] for r in ods_records}
        
        latencies = []
        missing = 0
        for txn_id, txn_time in core_records:
            if txn_id in ods_dict:
                integration_time = ods_dict[txn_id]
                latency = (integration_time - txn_time).total_seconds()
                latencies.append(latency)
            else:
                missing += 1
                
        if not latencies:
            print("No matching records found for latency measurement.", flush=True)
            return
            
        avg_lat = sum(latencies) / len(latencies)
        max_lat = max(latencies)
        min_lat = min(latencies)
        
        print(f"Analyzed {len(latencies)} recent trans. | In-flight (Pending): {missing} | Avg Latency: {avg_lat:.4f}s | Max: {max_lat:.4f}s | Min: {min_lat:.4f}s", flush=True)
        
    except Exception as e:
        print(f"Error: {e}", flush=True)
    finally:
        core_conn.close()
        ods_conn.close()

if __name__ == "__main__":
    print("Monitoring Latency End-to-End (Source DB -> Debezium -> Kafka -> Transformer -> ODS)...")
    for _ in range(20): # Run for about a minute
        measure()
        time.sleep(2)
