import asyncio
import json
import random
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import requests
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import statistics as _stats

app = FastAPI()
BASE = Path(__file__).parent

CORE = dict(host="localhost", port=5432, user="admin", password="password", dbname="core_banking")
ODS  = dict(host="localhost", port=5433, user="admin", password="password", dbname="ods")
DEBEZIUM = "http://localhost:8083"
CURRENCIES = ["GBP", "USD", "EUR", "JPY", "CHF", "AUD", "CAD"]

_latency_history: list[float] = []
LOST_THRESHOLD_SECS = 30


def _percentile(sorted_data: list, p: float) -> float:
    k = (len(sorted_data) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(sorted_data) - 1)
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * (k - lo)

EXPERIMENT_FILE = BASE.parent.parent / ".experiment"

EXPERIMENT_META = {
    "A": {
        "name": "Single Node Bottleneck",
        "detail": "1 transformer",
        "requested": 1,
    },
    "B": {
        "name": "Distributed Stream Processing",
        "detail": "10 transformers · 10 Kafka partitions",
        "requested": 10,
    },
    "C": {
        "name": "Data Virtualization",
        "detail": "SQL views, no stream processors",
        "requested": 0,
    },
}

ALLOWED_TABLES = {
    "event": (
        "SELECT event_id::text, event_type, event_amount, currency, event_timestamp, "
        "EXTRACT(EPOCH FROM (integration_timestamp - event_timestamp)) AS latency_s "
        "FROM bcdm.event ORDER BY integration_timestamp DESC LIMIT %s"
    ),
    "party": (
        "SELECT party_id::text, party_type, first_name, last_name, source_system, integration_timestamp "
        "FROM bcdm.party ORDER BY integration_timestamp DESC LIMIT %s"
    ),
    "arrangement": (
        "SELECT arrangement_id::text, product_category, balance, status, source_system, integration_timestamp "
        "FROM bcdm.arrangement ORDER BY integration_timestamp DESC LIMIT %s"
    ),
}


def source_metrics() -> dict:
    try:
        conn = psycopg2.connect(**CORE)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM payments.transactions")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM payments.transactions WHERE txn_time > NOW() - INTERVAL '10 seconds'")
        recent = cur.fetchone()[0]
        cur.close(); conn.close()
        return {"ok": True, "total": total, "rate": round(recent / 10, 1)}
    except Exception as e:
        return {"ok": False, "total": 0, "rate": 0, "err": str(e)}


def debezium_metrics() -> dict:
    try:
        names = requests.get(f"{DEBEZIUM}/connectors", timeout=2).json()
        if not names:
            return {"ok": False, "status": "NO CONNECTORS", "connectors": []}
        all_ok, conns = True, []
        for name in names:
            sd = requests.get(f"{DEBEZIUM}/connectors/{name}/status", timeout=2).json()
            cs = sd.get("connector", {}).get("state", "UNKNOWN")
            ts = (sd.get("tasks") or [{}])[0].get("state", "UNKNOWN")
            if cs != "RUNNING" or ts != "RUNNING":
                all_ok = False
            conns.append({"name": name, "connector": cs, "task": ts})
        return {"ok": all_ok, "status": "RUNNING" if all_ok else "DEGRADED", "connectors": conns}
    except Exception as e:
        return {"ok": False, "status": "UNREACHABLE", "connectors": [], "err": str(e)}


def kafka_metrics() -> dict:
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        topic = "src.payments.transactions"
        meta = admin.list_topics(topic=topic, timeout=3)
        if topic not in meta.topics:
            return {"ok": False, "status": "TOPIC MISSING", "lag": 0, "total": 0}

        partitions = list(meta.topics[topic].partitions.keys())
        tps = [TopicPartition(topic, p) for p in partitions]

        mon = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "dashboard-mon"})
        total_high = sum(mon.get_watermark_offsets(tp, timeout=2)[1] for tp in tps)
        mon.close()

        tfm = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "bcdm-transformer-group"})
        committed = tfm.committed(tps, timeout=2)
        tfm.close()
        consumed = sum(tp.offset for tp in committed if tp.offset and tp.offset >= 0)
        lag = max(0, total_high - consumed)

        return {"ok": True, "status": "HEALTHY", "total": total_high, "lag": lag}
    except Exception as e:
        return {"ok": False, "status": "ERROR", "lag": 0, "total": 0, "err": str(e)}


def ods_metrics() -> dict:
    try:
        conn = psycopg2.connect(**ODS)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bcdm.event WHERE source_system='CORE_BANKING_PAYMENTS'")
        total = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM bcdm.event "
            "WHERE source_system='CORE_BANKING_PAYMENTS' "
            "AND integration_timestamp > NOW() - INTERVAL '10 seconds'"
        )
        recent = cur.fetchone()[0]
        cur.close(); conn.close()
        return {"ok": True, "total": total, "rate": round(recent / 10, 1)}
    except Exception as e:
        return {"ok": False, "total": 0, "rate": 0, "err": str(e)}


def latency_metrics() -> dict:
    _empty = {"avg": 0, "p50": 0, "p95": 0, "p99": 0, "stddev": 0,
              "min": 0, "max": 0, "in_flight": 0, "potentially_lost": 0}
    cc = oc = None
    try:
        cc = psycopg2.connect(**CORE)
        oc = psycopg2.connect(**ODS)
        c_cur = cc.cursor()
        o_cur = oc.cursor()

        c_cur.execute("SELECT txn_id, txn_time FROM payments.transactions ORDER BY txn_id DESC LIMIT 500")
        core_rows = c_cur.fetchall()
        if not core_rows:
            return _empty

        core_ids = [r[0] for r in core_rows]
        o_cur.execute(
            "SELECT source_record_id::int, integration_timestamp "
            "FROM bcdm.event WHERE source_system='CORE_BANKING_PAYMENTS' "
            "AND source_record_id::int = ANY(%s)",
            (core_ids,),
        )
        ods_map = {r[0]: r[1] for r in o_cur.fetchall()}

        lats, in_flight, potentially_lost = [], 0, 0
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for txn_id, txn_time in core_rows:
            if txn_id in ods_map:
                lats.append((ods_map[txn_id] - txn_time).total_seconds())
            else:
                age = (now - txn_time).total_seconds()
                if age < LOST_THRESHOLD_SECS:
                    in_flight += 1
                else:
                    potentially_lost += 1

        if not lats:
            return {**_empty, "in_flight": in_flight, "potentially_lost": potentially_lost}

        s = sorted(lats)
        return {
            "avg":             round(sum(lats) / len(lats), 3),
            "p50":             round(_percentile(s, 50), 3),
            "p95":             round(_percentile(s, 95), 3),
            "p99":             round(_percentile(s, 99), 3),
            "stddev":          round(_stats.stdev(lats) if len(lats) > 1 else 0, 3),
            "min":             round(s[0], 3),
            "max":             round(s[-1], 3),
            "in_flight":       in_flight,
            "potentially_lost": potentially_lost,
        }
    except Exception as e:
        return {**_empty, "err": str(e)}
    finally:
        if cc: cc.close()
        if oc: oc.close()


def party_metrics() -> dict:
    _empty = {"p99": 0, "in_flight": 0, "potentially_lost": 0}
    cc = oc = None
    try:
        cc = psycopg2.connect(**CORE)
        oc = psycopg2.connect(**ODS)
        c_cur = cc.cursor()
        o_cur = oc.cursor()

        c_cur.execute("SELECT customer_id, created_at FROM client.customers ORDER BY customer_id DESC LIMIT 500")
        core_rows = c_cur.fetchall()
        if not core_rows:
            return _empty

        core_ids = [r[0] for r in core_rows]
        o_cur.execute(
            "SELECT source_record_id::int, integration_timestamp "
            "FROM bcdm.party WHERE source_system='CORE_BANKING_CLIENT' "
            "AND source_record_id::int = ANY(%s)",
            (core_ids,),
        )
        ods_map = {r[0]: r[1] for r in o_cur.fetchall()}

        lats, in_flight, potentially_lost = [], 0, 0
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for cust_id, created_at in core_rows:
            if cust_id in ods_map:
                lats.append((ods_map[cust_id] - created_at).total_seconds())
            else:
                age = (now - created_at).total_seconds()
                if age < LOST_THRESHOLD_SECS:
                    in_flight += 1
                else:
                    potentially_lost += 1

        s = sorted(lats)
        return {
            "p99":             round(_percentile(s, 99), 3) if s else 0,
            "in_flight":       in_flight,
            "potentially_lost": potentially_lost,
        }
    except Exception as e:
        return {**_empty, "err": str(e)}
    finally:
        if cc: cc.close()
        if oc: oc.close()


def ods_records_query(table: str, limit: int) -> list[dict]:
    conn = psycopg2.connect(**ODS)
    cur = conn.cursor()
    cur.execute(ALLOWED_TABLES[table], (limit,))
    cols = [d[0] for d in cur.description]
    rows = []
    for row in cur.fetchall():
        d = {}
        for k, v in zip(cols, row):
            d[k] = v.isoformat() if hasattr(v, 'isoformat') else v
        rows.append(d)
    cur.close()
    conn.close()
    return rows


def experiment_metrics() -> dict:
    try:
        if not EXPERIMENT_FILE.exists():
            return {"experiment": None}
        exp = EXPERIMENT_FILE.read_text().strip().upper()
        meta = EXPERIMENT_META.get(exp)
        if not meta:
            return {"experiment": None}
        result = subprocess.run(
            [
                "docker", "ps",
                "--filter", "label=com.docker.compose.service=transformer",
                "--filter", "status=running",
                "--format", "{{.Names}}",
            ],
            capture_output=True,
            text=True,
            timeout=2,
        )
        running = len([l for l in result.stdout.strip().splitlines() if l])
        return {
            "experiment": exp,
            "name": meta["name"],
            "detail": meta["detail"],
            "running": running,
            "requested": meta["requested"],
        }
    except Exception as e:
        return {"experiment": None, "err": str(e)}


async def event_stream():
    while True:
        loop = asyncio.get_event_loop()
        src, deb, kfk, ods, lat, exp, pty = await asyncio.gather(
            loop.run_in_executor(None, source_metrics),
            loop.run_in_executor(None, debezium_metrics),
            loop.run_in_executor(None, kafka_metrics),
            loop.run_in_executor(None, ods_metrics),
            loop.run_in_executor(None, latency_metrics),
            loop.run_in_executor(None, experiment_metrics),
            loop.run_in_executor(None, party_metrics),
        )

        if lat["avg"] > 0:
            _latency_history.append(lat["avg"])
            if len(_latency_history) > 60:
                _latency_history.pop(0)

        payload = {
            "ts": datetime.utcnow().isoformat(),
            "source": src,
            "debezium": deb,
            "kafka": kfk,
            "ods": ods,
            "latency": lat,
            "party": pty,
            "history": _latency_history[-30:],
            "experiment": exp,
        }
        yield f"data: {json.dumps(payload)}\n\n"
        await asyncio.sleep(2)


@app.get("/stream")
async def stream():
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/generate")
async def generate(count: int = 50):
    def _insert():
        conn = psycopg2.connect(**CORE)
        cur = conn.cursor()
        for _ in range(count):
            cur.execute(
                "INSERT INTO payments.transactions "
                "(sender_account_id, receiver_account_id, amount, currency, txn_time) "
                "VALUES (%s, %s, %s, %s, NOW())",
                (
                    random.randint(1000, 9999),
                    random.randint(1000, 9999),
                    round(random.uniform(10.0, 50000.0), 2),
                    random.choice(CURRENCIES),
                ),
            )
        conn.commit()
        cur.close(); conn.close()

    await asyncio.get_event_loop().run_in_executor(None, _insert)
    return {"inserted": count}


@app.get("/ods-records")
async def ods_records(table: str = "event", limit: int = Query(25, ge=1, le=100)):
    if table not in ALLOWED_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"table must be one of {list(ALLOWED_TABLES)}",
        )
    try:
        rows = await asyncio.get_event_loop().run_in_executor(
            None, ods_records_query, table, limit
        )
        return rows
    except Exception as e:
        logging.exception("ods_records query failed")
        raise HTTPException(status_code=500, detail="internal error")


@app.get("/")
async def root():
    return HTMLResponse((BASE / "static" / "index.html").read_text())


app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")
