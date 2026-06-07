#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Tearing down the PoC infrastructure..."

# ─── 1. Docker ────────────────────────────────────────────────────────────────
echo "  [1/4] Stopping and removing containers, networks, and volumes..."
docker compose -f "$SCRIPT_DIR/infrastructure/docker-compose.yaml" down -v --remove-orphans

# Remove the locally-built transformer image (created by 'build: ../src/transformer')
if docker image inspect ods-transformer &>/dev/null 2>&1; then
  echo "        Removing transformer Docker image..."
  docker image rm ods-transformer
fi

# ─── 2. Generated log files ───────────────────────────────────────────────────
echo "  [2/4] Removing generated log files..."
rm -f \
  "$SCRIPT_DIR/generator.log" \
  "$SCRIPT_DIR/generator_verification.log" \
  "$SCRIPT_DIR/measure_latency.log" \
  "$SCRIPT_DIR/measure_latency_verification.log" \
  "$SCRIPT_DIR/raw_sink.log"

# ─── 3. Generated / temporary files ──────────────────────────────────────────
echo "  [3/4] Removing generated and temporary files..."
rm -f \
  "$SCRIPT_DIR/config.json" \
  "$SCRIPT_DIR/explain_table.txt" \
  "$SCRIPT_DIR/explain_view.txt" \
  "$SCRIPT_DIR/mock_data.sql" \
  "$SCRIPT_DIR/.experiment"

# ─── 4. Python virtual environment ───────────────────────────────────────────
echo "  [4/4] Removing Python virtual environment..."
rm -rf "$SCRIPT_DIR/venv"

# ─── Python cache ─────────────────────────────────────────────────────────────
find "$SCRIPT_DIR/src" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$SCRIPT_DIR/src" -name "*.pyc" -delete 2>/dev/null || true

echo ""
echo "Teardown complete. Run './fresh_test.sh' for a clean start."
