import subprocess
import os
import time
import json
import asyncio
from fastapi import FastAPI, WebSocket, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import psycopg2

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INFRA_DIR = os.path.join(BASE_DIR, "infrastructure")
INIT_SCRIPTS_DIR = os.path.join(BASE_DIR, "init-scripts")
TOOLS_DIR = os.path.join(BASE_DIR, "src", "tools")

app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

@app.get("/")
def read_root():
    with open(os.path.join(os.path.dirname(__file__), "static", "index.html"), "r") as f:
        return HTMLResponse(f.read())

def run_cmd(cmd: str):
    print(f"Running: {cmd}", flush=True)
    subprocess.run(cmd, shell=True, cwd=BASE_DIR)

@app.post("/api/experiment/{exp_id}")
async def start_experiment(exp_id: str, background_tasks: BackgroundTasks):
    if exp_id == "A":
        run_cmd(f"docker compose -f infrastructure/docker-compose.yaml up --scale transformer=1 -d")
    elif exp_id == "B":
        run_cmd(f"docker compose -f infrastructure/docker-compose.yaml exec -T kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic src.payments.transactions --partitions 10")
        run_cmd(f"docker compose -f infrastructure/docker-compose.yaml up --scale transformer=10 -d")
    elif exp_id == "C":
        run_cmd(f"docker compose -f infrastructure/docker-compose.yaml up --scale transformer=0 -d")
        run_cmd(f"docker compose -f infrastructure/docker-compose.yaml exec -T ods-db psql -U admin -d ods < init-scripts/setup_approach3.sql")
        # Kill any existing raw_sink.py
        run_cmd("pkill -f raw_sink.py || true")
        subprocess.Popen(["python3", os.path.join(BASE_DIR, "src", "raw_sink", "raw_sink.py")], cwd=BASE_DIR)
        
    return {"status": "started", "experiment": exp_id}

@app.post("/api/generate")
async def trigger_generator():
    subprocess.Popen(["python3", os.path.join(TOOLS_DIR, "generator.py")], cwd=BASE_DIR)
    return {"status": "generating"}

def measure_latency_loop():
    try:
        core_conn = psycopg2.connect(host="127.0.0.1", port="5432", user="admin", password="password", dbname="core_banking")
        ods_conn = psycopg2.connect(host="127.0.0.1", port="5433", user="admin", password="password", dbname="ods")
        
        core_cur = core_conn.cursor()
        ods_cur = ods_conn.cursor()
        
        core_cur.execute("SELECT txn_id, txn_time FROM payments.transactions ORDER BY txn_id DESC LIMIT 1000")
        core_records = core_cur.fetchall()
        
        ods_cur.execute("SELECT source_record_id::int, integration_timestamp FROM bcdm.event WHERE source_system='CORE_BANKING_PAYMENTS'")
        ods_records = ods_cur.fetchall()
        
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
                
        core_conn.close()
        ods_conn.close()
                
        if not latencies:
            return {"missing": missing, "avg": 0, "max": 0, "min": 0, "count": 0}
            
        return {
            "missing": missing,
            "avg": sum(latencies) / len(latencies),
            "max": max(latencies),
            "min": min(latencies),
            "count": len(latencies)
        }
    except Exception as e:
        print(f"Latency error: {e}", flush=True)
        return {"error": str(e)}

@app.websocket("/ws/metrics")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            metrics = measure_latency_loop()
            await websocket.send_json(metrics)
            await asyncio.sleep(1)
    except Exception as e:
        print("WebSocket disconnected", e)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8085)
