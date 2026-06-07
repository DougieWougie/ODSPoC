#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/infrastructure"
DC="docker compose -f $INFRA_DIR/docker-compose.yaml"

# ─── Usage ────────────────────────────────────────────────────────────────────
usage() {
  echo "Usage: $0 [A|B|C]"
  echo ""
  echo "  A  Single Node Bottleneck       (1 transformer)"
  echo "  B  Distributed Stream Processing (10 transformers, 10 partitions)"
  echo "  C  Data Virtualization           (0 transformers, SQL views)"
  echo ""
  echo "If no argument is given, an interactive menu is shown."
  exit 1
}

# ─── Experiment selection ─────────────────────────────────────────────────────
if [[ $# -gt 1 ]]; then
  usage
fi

if [[ $# -eq 1 ]]; then
  EXPERIMENT="${1^^}"  # normalise to uppercase
else
  echo "╔══════════════════════════════════════════════════╗"
  echo "║     ODS Proof of Concept — Experiment Runner     ║"
  echo "╠══════════════════════════════════════════════════╣"
  echo "║  A) Single Node Bottleneck                       ║"
  echo "║     1 transformer, demonstrates queue back-up    ║"
  echo "║                                                  ║"
  echo "║  B) Distributed Stream Processing                ║"
  echo "║     10 transformers + 10 Kafka partitions        ║"
  echo "║                                                  ║"
  echo "║  C) Data Virtualization                          ║"
  echo "║     SQL views, no stream processors              ║"
  echo "╚══════════════════════════════════════════════════╝"
  echo ""
  read -rp "Select experiment [A/B/C]: " EXPERIMENT
  EXPERIMENT="${EXPERIMENT^^}"
fi

case "$EXPERIMENT" in
  A|B|C) ;;
  *) echo "ERROR: Invalid selection '$EXPERIMENT'. Must be A, B, or C."; usage ;;
esac

echo ""
echo "Running Experiment $EXPERIMENT..."
echo ""

# ─── 0. Prerequisites ─────────────────────────────────────────────────────────
echo "[0/5] Checking prerequisites..."
for cmd in docker jq curl; do
  command -v "$cmd" &>/dev/null || { echo "ERROR: '$cmd' not found."; exit 1; }
done

# ─── 1. Python venv ───────────────────────────────────────────────────────────
echo "[1/5] Setting up Python virtual environment..."
if [[ ! -d "$SCRIPT_DIR/venv" ]]; then
  python3 -m venv "$SCRIPT_DIR/venv"
  "$SCRIPT_DIR/venv/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt"
fi

# ─── 2. Start infrastructure ──────────────────────────────────────────────────
echo "[2/5] Starting Docker services..."
$DC down -v --remove-orphans 2>/dev/null || true
$DC up -d --build

echo "      Waiting for databases to be healthy..."
for container in core-banking-db ods-db; do
  echo -n "      $container: "
  until [[ "$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null)" == "healthy" ]]; do
    echo -n "."
    sleep 2
  done
  echo " ready"
done

# ─── 3. (Databases already initialised by Docker entrypoint) ─────────────────

# ─── 3. Pre-create topics, register Debezium, configure experiment ───────────
echo "[3/4] Registering Debezium connector..."
until curl -sf http://localhost:8083/connectors > /dev/null; do
  echo "      Waiting for Debezium..."; sleep 3;
done

# Pre-create the transactions topic with the partition count for this experiment.
# Debezium creates topics lazily (only on first WAL event), so on empty tables
# the topic would never appear before the generator runs. Pre-creating it lets
# Debezium adopt the existing topic with the correct partition layout.
case "$EXPERIMENT" in
  B) PARTITIONS=10 ;;
  *) PARTITIONS=1  ;;
esac
echo "      Pre-creating src.payments.transactions with $PARTITIONS partition(s)..."
$DC exec -T kafka \
  kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic src.payments.transactions \
  --partitions $PARTITIONS \
  --replication-factor 1 2>/dev/null || true

curl -sf -X POST -H "Content-Type: application/json" \
  -d @"$INFRA_DIR/connectors/register-postgres-source.json" http://localhost:8083/connectors \
  | jq .

echo "      Configuring infrastructure for Experiment $EXPERIMENT..."
case "$EXPERIMENT" in
  A)
    $DC up --scale transformer=1 -d
    ;;
  B)
    $DC up --scale transformer=10 -d
    ;;
  C)
    $DC up --scale transformer=0 -d
    ;;
esac

echo "$EXPERIMENT" > "$SCRIPT_DIR/.experiment"

# ─── 4. Run measurements ──────────────────────────────────────────────────────
echo "[4/4] Running latency measurement and data generator..."

pkill -f "measure_latency.py" 2>/dev/null || true
pkill -f "raw_sink.py"        2>/dev/null || true

if [[ "$EXPERIMENT" == "C" ]]; then
  # Experiment C: raw sink instead of transformer, then compare read latency
  "$SCRIPT_DIR/venv/bin/python" src/raw_sink/raw_sink.py \
    > "$SCRIPT_DIR/raw_sink.log" 2>&1 &
  RAW_SINK_PID=$!
  echo "      raw_sink.py running (PID $RAW_SINK_PID)"

  sleep 5
  "$SCRIPT_DIR/venv/bin/python" src/tools/generator.py \
    > "$SCRIPT_DIR/generator.log" 2>&1
  sleep 10

  echo ""
  echo "=== Read Latency: Physical Table ==="
  $DC exec -T ods-db psql -U admin -d ods \
    -c "EXPLAIN ANALYZE SELECT * FROM bcdm.event;" \
    | tee "$SCRIPT_DIR/explain_table.txt"

  echo ""
  echo "=== Read Latency: Virtual View ==="
  $DC exec -T ods-db psql -U admin -d ods \
    -c "EXPLAIN ANALYZE SELECT * FROM bcdm.virtual_event;" \
    | tee "$SCRIPT_DIR/explain_view.txt"
else
  # Experiments A and B: latency monitor + generator
  "$SCRIPT_DIR/venv/bin/python" src/tools/measure_latency.py \
    > "$SCRIPT_DIR/measure_latency.log" 2>&1 &
  LATENCY_PID=$!
  echo "      measure_latency.py running (PID $LATENCY_PID)"

  sleep 5
  "$SCRIPT_DIR/venv/bin/python" src/tools/generator.py \
    > "$SCRIPT_DIR/generator.log" 2>&1
  sleep 40

  echo ""
  echo "=== Latency Results (Experiment $EXPERIMENT) ==="
  cat "$SCRIPT_DIR/measure_latency.log"
fi
