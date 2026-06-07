# Experiment-Aware Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the ODS dashboard display the active experiment (A/B/C) in a sub-header banner and show live transformer replica counts on the Transformer tile.

**Architecture:** `fresh_test.sh` writes the experiment letter to a `.experiment` state file. `app.py` reads that file and queries Docker for running transformer replicas; the result is included in the SSE stream. The frontend renders a sub-header banner and an extended Transformer tile from the streamed data.

**Tech Stack:** Python 3 / FastAPI / Server-Sent Events, subprocess (stdlib), Chart.js, vanilla JS

---

## File Map

| File | Change |
|---|---|
| `fresh_test.sh` | Write `.experiment` after replica scaling (~line 108) |
| `teardown.sh` | Remove `.experiment` in section 3 |
| `src/dashboard/app.py` | Add `import subprocess`, `EXPERIMENT_META`, `EXPERIMENT_FILE`, `experiment_metrics()`, wire into `event_stream()` |
| `src/dashboard/test_app.py` | New — tests for `experiment_metrics()` |
| `src/dashboard/static/index.html` | Add banner HTML + CSS, add Transformer tile elements, update `renderData()` |

---

### Task 1: Write and clear the `.experiment` state file

**Files:**
- Modify: `fresh_test.sh:95-108`
- Modify: `teardown.sh:28-34`

- [ ] **Step 1: Add state file write to `fresh_test.sh`**

  Open `fresh_test.sh`. After the closing `;;` and `esac` of the experiment `case` block (currently ending around line 108), add one line:

  ```bash
  case "$EXPERIMENT" in
    A)
      $DC up --scale transformer=1 -d
      ;;
    B)
      $DC exec -T kafka \
        kafka-topics --bootstrap-server kafka:29092 \
        --alter --topic src.payments.transactions --partitions 10 2>/dev/null || true
      $DC up --scale transformer=10 -d
      ;;
    C)
      $DC up --scale transformer=0 -d
      ;;
  esac

  echo "$EXPERIMENT" > "$SCRIPT_DIR/.experiment"
  ```

- [ ] **Step 2: Add state file removal to `teardown.sh`**

  In `teardown.sh`, inside the section 3 `rm -f` block (lines 30-34), add `.experiment`:

  ```bash
  rm -f \
    "$SCRIPT_DIR/config.json" \
    "$SCRIPT_DIR/explain_table.txt" \
    "$SCRIPT_DIR/explain_view.txt" \
    "$SCRIPT_DIR/mock_data.sql" \
    "$SCRIPT_DIR/.experiment"
  ```

- [ ] **Step 3: Smoke-test manually**

  ```bash
  echo "B" > .experiment
  cat .experiment   # should print: B
  rm .experiment
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add fresh_test.sh teardown.sh
  git commit -m "feat: write .experiment state file on experiment start, remove on teardown"
  ```

---

### Task 2: Add `experiment_metrics()` to `app.py` (TDD)

**Files:**
- Create: `src/dashboard/test_app.py`
- Modify: `src/dashboard/app.py`

- [ ] **Step 1: Write the failing tests**

  Create `src/dashboard/test_app.py`:

  ```python
  import subprocess
  from pathlib import Path
  from unittest.mock import patch, MagicMock
  import app


  def _mock_proc(stdout: str) -> MagicMock:
      m = MagicMock()
      m.stdout = stdout
      return m


  def test_experiment_metrics_no_file(tmp_path):
      with patch.object(app, 'EXPERIMENT_FILE', tmp_path / ".experiment"):
          result = app.experiment_metrics()
      assert result == {"experiment": None}


  def test_experiment_metrics_exp_a(tmp_path):
      f = tmp_path / ".experiment"
      f.write_text("A\n")
      with patch.object(app, 'EXPERIMENT_FILE', f), \
           patch('subprocess.run', return_value=_mock_proc("infrastructure-transformer-1\n")):
          result = app.experiment_metrics()
      assert result["experiment"] == "A"
      assert result["running"] == 1
      assert result["requested"] == 1
      assert result["name"] == "Single Node Bottleneck"
      assert "detail" in result


  def test_experiment_metrics_exp_b_all_running(tmp_path):
      f = tmp_path / ".experiment"
      f.write_text("B")
      names = "\n".join(f"infrastructure-transformer-{i}" for i in range(1, 11)) + "\n"
      with patch.object(app, 'EXPERIMENT_FILE', f), \
           patch('subprocess.run', return_value=_mock_proc(names)):
          result = app.experiment_metrics()
      assert result["experiment"] == "B"
      assert result["running"] == 10
      assert result["requested"] == 10


  def test_experiment_metrics_exp_b_degraded(tmp_path):
      f = tmp_path / ".experiment"
      f.write_text("B")
      names = "\n".join(f"infrastructure-transformer-{i}" for i in range(1, 9)) + "\n"
      with patch.object(app, 'EXPERIMENT_FILE', f), \
           patch('subprocess.run', return_value=_mock_proc(names)):
          result = app.experiment_metrics()
      assert result["running"] == 8
      assert result["requested"] == 10


  def test_experiment_metrics_exp_c(tmp_path):
      f = tmp_path / ".experiment"
      f.write_text("C")
      with patch.object(app, 'EXPERIMENT_FILE', f), \
           patch('subprocess.run', return_value=_mock_proc("")):
          result = app.experiment_metrics()
      assert result["experiment"] == "C"
      assert result["running"] == 0
      assert result["requested"] == 0
      assert result["name"] == "Data Virtualization"


  def test_experiment_metrics_unknown_letter(tmp_path):
      f = tmp_path / ".experiment"
      f.write_text("Z")
      with patch.object(app, 'EXPERIMENT_FILE', f):
          result = app.experiment_metrics()
      assert result == {"experiment": None}


  def test_experiment_metrics_docker_error(tmp_path):
      f = tmp_path / ".experiment"
      f.write_text("A")
      with patch.object(app, 'EXPERIMENT_FILE', f), \
           patch('subprocess.run', side_effect=Exception("docker not found")):
          result = app.experiment_metrics()
      assert result["experiment"] is None
      assert "err" in result
  ```

- [ ] **Step 2: Run tests — confirm they all fail**

  ```bash
  cd src/dashboard && python -m pytest test_app.py -v
  ```

  Expected: `AttributeError: module 'app' has no attribute 'EXPERIMENT_FILE'` (or similar) on every test.

- [ ] **Step 3: Add `import subprocess`, constants, and `experiment_metrics()` to `app.py`**

  At the top of `src/dashboard/app.py`, add `import subprocess` after the existing imports (after line 10):

  ```python
  import subprocess
  ```

  After `_latency_history: list[float] = []` (currently line 21), add:

  ```python
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
  ```

  After the `latency_metrics()` function (before `async def event_stream()`), add:

  ```python
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
  ```

- [ ] **Step 4: Run tests — confirm all pass**

  ```bash
  cd src/dashboard && python -m pytest test_app.py -v
  ```

  Expected output: 7 tests, all `PASSED`.

- [ ] **Step 5: Commit**

  ```bash
  git add src/dashboard/app.py src/dashboard/test_app.py
  git commit -m "feat: add experiment_metrics() with Docker replica count"
  ```

---

### Task 3: Wire `experiment_metrics()` into the SSE stream

**Files:**
- Modify: `src/dashboard/app.py:140-165`

- [ ] **Step 1: Update `event_stream()` gather call**

  In `src/dashboard/app.py`, replace the `event_stream()` function's gather block and variable unpacking (currently lines 143–149):

  **Before:**
  ```python
  src, deb, kfk, ods, lat = await asyncio.gather(
      loop.run_in_executor(None, source_metrics),
      loop.run_in_executor(None, debezium_metrics),
      loop.run_in_executor(None, kafka_metrics),
      loop.run_in_executor(None, ods_metrics),
      loop.run_in_executor(None, latency_metrics),
  )
  ```

  **After:**
  ```python
  src, deb, kfk, ods, lat, exp = await asyncio.gather(
      loop.run_in_executor(None, source_metrics),
      loop.run_in_executor(None, debezium_metrics),
      loop.run_in_executor(None, kafka_metrics),
      loop.run_in_executor(None, ods_metrics),
      loop.run_in_executor(None, latency_metrics),
      loop.run_in_executor(None, experiment_metrics),
  )
  ```

- [ ] **Step 2: Add `experiment` key to SSE payload**

  In the same function, add `"experiment": exp` to the `payload` dict (currently lines 156–164):

  **Before:**
  ```python
  payload = {
      "ts": datetime.utcnow().isoformat(),
      "source": src,
      "debezium": deb,
      "kafka": kfk,
      "ods": ods,
      "latency": lat,
      "history": _latency_history[-30:],
  }
  ```

  **After:**
  ```python
  payload = {
      "ts": datetime.utcnow().isoformat(),
      "source": src,
      "debezium": deb,
      "kafka": kfk,
      "ods": ods,
      "latency": lat,
      "history": _latency_history[-30:],
      "experiment": exp,
  }
  ```

- [ ] **Step 3: Verify tests still pass**

  ```bash
  cd src/dashboard && python -m pytest test_app.py -v
  ```

  Expected: 7 tests, all `PASSED`.

- [ ] **Step 4: Commit**

  ```bash
  git add src/dashboard/app.py
  git commit -m "feat: include experiment metadata in SSE stream payload"
  ```

---

### Task 4: Add sub-header experiment banner to the frontend

**Files:**
- Modify: `src/dashboard/static/index.html`

- [ ] **Step 1: Add banner CSS to the `<style>` block**

  In `src/dashboard/static/index.html`, insert the following inside `<style>`, after the `/* ── Theme toggle ─` block (before the closing `</style>`):

  ```css
  /* ── Experiment banner ───────────────────────────────── */
  #exp-banner {
    display: none;
    align-items: center;
    gap: 12px;
    padding: 7px 48px;
    background: rgba(0,212,255,0.06);
    border-bottom: 1px solid var(--border);
    transition: background 0.35s, border-color 0.35s;
  }

  [data-theme="light"] #exp-banner {
    background: rgba(0,100,200,0.05);
  }

  .exp-active-label {
    font-size: 9px;
    color: var(--dim);
    text-transform: uppercase;
    letter-spacing: 2px;
    flex-shrink: 0;
  }

  .exp-chip {
    font-family: 'Roboto Mono', monospace;
    font-size: 11px;
    font-weight: 700;
    color: var(--primary);
    background: rgba(0,212,255,0.12);
    padding: 3px 9px;
    border-radius: 4px;
    flex-shrink: 0;
  }

  [data-theme="light"] .exp-chip {
    background: rgba(0,100,200,0.1);
  }

  .exp-desc {
    font-size: 11px;
    color: var(--dim);
  }

  .exp-desc strong {
    color: var(--text);
    font-weight: 500;
  }
  ```

- [ ] **Step 2: Add banner HTML after `</header>`**

  In `src/dashboard/static/index.html`, insert after the closing `</header>` tag (currently line 411) and before `<!-- Pipeline -->`:

  ```html
  <!-- Experiment banner -->
  <div id="exp-banner">
    <span class="exp-active-label">Active</span>
    <span class="exp-chip" id="exp-chip">—</span>
    <span class="exp-desc" id="exp-desc">—</span>
  </div>
  ```

- [ ] **Step 3: Update `renderData()` to drive the banner**

  In `src/dashboard/static/index.html`, inside `renderData(d)`, add the following block after the `// ── Segment rates for particles` block (after line ~845):

  ```javascript
  // ── Experiment banner
  const exp = d.experiment;
  if (exp && exp.experiment) {
    document.getElementById('exp-banner').style.display = 'flex';
    setText('exp-chip', `EXP ${exp.experiment}`);
    document.getElementById('exp-desc').innerHTML =
      `<strong>${exp.name}</strong> — ${exp.detail}`;
  } else {
    document.getElementById('exp-banner').style.display = 'none';
  }
  ```

- [ ] **Step 4: Manual smoke test**

  Start the dashboard (run from the project root):
  ```bash
  echo "B" > .experiment
  cd src/dashboard && ../../venv/bin/uvicorn app:app --host 0.0.0.0 --port 8080
  ```
  Open http://localhost:8080. Confirm the slim banner reads:
  > **Active  EXP B  Distributed Stream Processing — 10 transformers · 10 Kafka partitions**

  Then:
  ```bash
  rm .experiment
  ```
  Wait one SSE tick (~2 s) and confirm the banner disappears.

- [ ] **Step 5: Commit**

  ```bash
  git add src/dashboard/static/index.html
  git commit -m "feat: add experiment sub-header banner driven by SSE stream"
  ```

---

### Task 5: Add running/requested count to the Transformer tile

**Files:**
- Modify: `src/dashboard/static/index.html`

- [ ] **Step 1: Add count elements to the Transformer node HTML**

  In `src/dashboard/static/index.html`, locate the Transformer node (the `<div class="node" id="n-tfm">` block). After the existing `<div class="node-sub">BCDM Mapping</div>` line, insert:

  ```html
  <div class="node-sub" id="m-tfm-count"
       style="display:none; font-family:'Roboto Mono',monospace; color:var(--primary); font-size:13px; font-weight:700; margin-top:6px;">— / —</div>
  <div class="node-sub" id="m-tfm-count-label"
       style="display:none; margin-top:2px;">running / requested</div>
  ```

- [ ] **Step 2: Update `renderData()` to populate the tile**

  In `src/dashboard/static/index.html`, find the `// ── Transformer` section inside `renderData()` (currently lines ~828-832). Replace it with:

  ```javascript
  // ── Transformer
  const tfmOk = ods.ok;
  const tfmActive = tfmOk && ods.rate > 0;
  const tfmCountEl = document.getElementById('m-tfm-count');
  const tfmLabelEl = document.getElementById('m-tfm-count-label');

  if (exp && exp.experiment === 'C') {
    setBadge('b-tfm', 'warn');
    setActive('n-tfm', false);
    setText('m-tfm-status', 'SQL VIEWS');
    document.getElementById('m-tfm-status').style.color = 'var(--warning)';
    tfmCountEl.style.display = '';
    tfmLabelEl.style.display = '';
    setText('m-tfm-count', '— / —');
    setText('m-tfm-count-label', 'No stream processor');
  } else {
    setBadge('b-tfm', tfmOk ? (tfmActive ? 'ok' : 'warn') : 'err');
    setActive('n-tfm', tfmActive);
    setText('m-tfm-status', tfmActive ? 'ACTIVE' : tfmOk ? 'IDLE' : 'OFFLINE');
    document.getElementById('m-tfm-status').style.color = '';
    if (exp && exp.experiment) {
      tfmCountEl.style.display = '';
      tfmLabelEl.style.display = '';
      setText('m-tfm-count', `${exp.running} / ${exp.requested}`);
      setText('m-tfm-count-label', 'running / requested');
    } else {
      tfmCountEl.style.display = 'none';
      tfmLabelEl.style.display = 'none';
    }
  }
  ```

- [ ] **Step 3: Manual smoke test — Experiment A**

  ```bash
  echo "A" > .experiment
  cd src/dashboard && ../../venv/bin/uvicorn app:app --host 0.0.0.0 --port 8080
  ```

  Open http://localhost:8080. Confirm:
  - Banner shows `EXP A · Single Node Bottleneck — 1 transformer`
  - Transformer tile status shows `ACTIVE` or `IDLE`
  - Below status: `1 / 1` in primary colour, then `running / requested` in dim

- [ ] **Step 4: Manual smoke test — Experiment C**

  ```bash
  rm .experiment
  echo "C" > .experiment
  ```

  Wait one SSE tick. Confirm:
  - Banner shows `EXP C · Data Virtualization — SQL views, no stream processors`
  - Transformer tile metric shows `SQL VIEWS` in warning colour
  - Below: `— / —` then `No stream processor`

- [ ] **Step 5: Manual smoke test — no experiment**

  ```bash
  rm .experiment
  ```

  Wait one SSE tick. Confirm:
  - Banner is hidden
  - Transformer tile shows only `ACTIVE`/`IDLE`/`OFFLINE` with no count rows

- [ ] **Step 6: Run full test suite**

  ```bash
  cd src/dashboard && python -m pytest test_app.py -v
  ```

  Expected: 7 tests, all `PASSED`.

- [ ] **Step 7: Commit**

  ```bash
  git add src/dashboard/static/index.html
  git commit -m "feat: show running/requested transformer count on tile per experiment"
  ```
