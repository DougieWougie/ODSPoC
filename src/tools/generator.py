import psycopg2
import random
import time
import threading
import sys

NUM_RECORDS = 2000
NUM_THREADS = 10

def generate_data(thread_id, records_per_thread):
    conn = psycopg2.connect(host="127.0.0.1", port="5432", user="admin", password="password", dbname="core_banking")
    conn.autocommit = True
    cursor = conn.cursor()
    for _ in range(records_per_thread):
        sender = random.randint(100, 999)
        receiver = random.randint(100, 999)
        amount = round(random.uniform(10.0, 5000.0), 2)
        cursor.execute(f"INSERT INTO payments.transactions (sender_account_id, receiver_account_id, amount, currency) VALUES ({sender}, {receiver}, {amount}, 'GBP')")
    cursor.close()
    conn.close()
    print(f"Thread {thread_id} finished inserting {records_per_thread} records.")

if __name__ == "__main__":
    print(f"Starting generation of {NUM_RECORDS} transactions across {NUM_THREADS} concurrent threads...")
    start = time.time()
    threads = []
    records_per_thread = NUM_RECORDS // NUM_THREADS
    for i in range(NUM_THREADS):
        t = threading.Thread(target=generate_data, args=(i, records_per_thread))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    end = time.time()
    print(f"Total time: {end - start:.2f} seconds. Rate: {NUM_RECORDS/(end-start):.2f} transactions/sec")
