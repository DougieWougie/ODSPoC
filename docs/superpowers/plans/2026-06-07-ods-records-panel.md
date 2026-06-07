# ODS Records Panel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clicking the ODS pipeline tile expands an inline panel that live-polls and displays the most recent records from `bcdm.event`, `bcdm.party`, and `bcdm.arrangement`.

**Architecture:** A new `GET /ods-records` FastAPI endpoint queries the ODS database and returns rows as JSON. The frontend polls it every 2 seconds while the panel is open. The panel is an inline HTML section inserted between the pipeline and the KPIs, toggled by clicking the ODS tile.

**Tech Stack:** Python / FastAPI / psycopg2 (backend); vanilla JS / HTML / CSS (frontend); pytest + FastAPI TestClient / httpx (tests).

---

## File Map

| File | Change |
|---|---|
| `src/dashboard/app.py` | Add `ALLOWED_TABLES` dict, `ods_records_query()` function, `GET /ods-records` endpoint; add `HTTPException` to FastAPI import |
| `src/dashboard/requirements.txt` | Add `httpx` (required by Starlette TestClient) |
| `src/dashboard/test_app.py` | Add 5 tests for the new endpoint |
| `src/dashboard/static/index.html` | Add panel CSS, `#ods-panel` HTML, ODS tile changes, panel JS functions |

---

## Task 1: Add httpx and write failing backend tests

**Files:**
- Modify: `src/dashboard/requirements.txt`
- Modify: `src/dashboard/test_app.py`

- [ ] **Step 1.1: Add httpx to dashboard requirements**

Open `src/dashboard/requirements.txt` and append:

```
httpx==0.27.0
```

- [ ] **Step 1.2: Install it**

```bash
cd src/dashboard && ../../venv/bin/pip install httpx==0.27.0
```

Expected: `Successfully installed httpx-0.27.0` (or "already satisfied")

- [ ] **Step 1.3: Write the failing tests**

Append to `src/dashboard/test_app.py` (keep all existing tests, add below):

```python
from fastapi.testclient import TestClient


def test_ods_records_invalid_table():
    client = TestClient(app.app)
    resp = client.get("/ods-records?table=injected_table")
    assert resp.status_code == 400
    assert "table must be one of" in resp.json()["detail"]


def test_ods_records_event():
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value
    mock_cur.description = [
        ("event_id",), ("event_type",), ("event_amount",),
        ("currency",), ("event_timestamp",), ("latency_s",),
    ]
    mock_cur.fetchall.return_value = [
        ("a3f2b1c9-0000-0000-0000-000000000001", "PAYMENT_TRANSACTION", 100.0, "GBP", None, 0.18),
    ]
    with patch("psycopg2.connect", return_value=mock_conn):
        client = TestClient(app.app)
        resp = client.get("/ods-records?table=event&limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["event_type"] == "PAYMENT_TRANSACTION"
    assert data[0]["latency_s"] == 0.18
    assert data[0]["currency"] == "GBP"


def test_ods_records_party():
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value
    mock_cur.description = [
        ("party_id",), ("party_type",), ("first_name",),
        ("last_name",), ("source_system",), ("integration_timestamp",),
    ]
    mock_cur.fetchall.return_value = [
        ("pid-1234", "INDIVIDUAL", "Alice", "Smith", "CORE_BANKING_CLIENT", None),
    ]
    with patch("psycopg2.connect", return_value=mock_conn):
        client = TestClient(app.app)
        resp = client.get("/ods-records?table=party&limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["first_name"] == "Alice"
    assert data[0]["party_type"] == "INDIVIDUAL"


def test_ods_records_arrangement():
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value
    mock_cur.description = [
        ("arrangement_id",), ("product_category",), ("balance",),
        ("status",), ("source_system",), ("integration_timestamp",),
    ]
    mock_cur.fetchall.return_value = [
        ("aid-9999", "CHECKING_ACCOUNT", 5000.0, "ACTIVE", "CORE_BANKING", None),
    ]
    with patch("psycopg2.connect", return_value=mock_conn):
        client = TestClient(app.app)
        resp = client.get("/ods-records?table=arrangement&limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["product_category"] == "CHECKING_ACCOUNT"
    assert data[0]["balance"] == 5000.0


def test_ods_records_db_error():
    with patch("psycopg2.connect", side_effect=Exception("connection refused")):
        client = TestClient(app.app)
        resp = client.get("/ods-records?table=event")
    assert resp.status_code == 500
    assert "connection refused" in resp.json()["detail"]
```

- [ ] **Step 1.4: Run and confirm all 5 new tests fail**

```bash
cd src/dashboard && ../../venv/bin/python -m pytest test_app.py -v -k "ods_records"
```

Expected: 5 failures — `AttributeError: module 'app' has no attribute ...` or `404` errors.

---

## Task 2: Implement the backend endpoint

**Files:**
- Modify: `src/dashboard/app.py`

- [ ] **Step 2.1: Add HTTPException to the FastAPI import**

In `src/dashboard/app.py`, change line 7:

```python
from fastapi import FastAPI
```

to:

```python
from fastapi import FastAPI, HTTPException
```

- [ ] **Step 2.2: Add ALLOWED_TABLES dict after the EXPERIMENT_META block**

After the closing `}` of `EXPERIMENT_META` (around line 50), insert:

```python
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
```

- [ ] **Step 2.3: Add ods_records_query() after the party_metrics() function**

After the closing `finally` block of `party_metrics()` (before `experiment_metrics`), insert:

```python
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
```

- [ ] **Step 2.4: Add the /ods-records endpoint after the /generate endpoint**

After the closing brace of the `generate()` function (before `@app.get("/")`), insert:

```python
@app.get("/ods-records")
async def ods_records(table: str = "event", limit: int = 25):
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
        raise HTTPException(status_code=500, detail=str(e))
```

- [ ] **Step 2.5: Run the 5 new tests and confirm they all pass**

```bash
cd src/dashboard && ../../venv/bin/python -m pytest test_app.py -v -k "ods_records"
```

Expected: 5 passed.

- [ ] **Step 2.6: Run the full test suite to confirm nothing is broken**

```bash
cd src/dashboard && ../../venv/bin/python -m pytest test_app.py -v
```

Expected: all tests pass.

- [ ] **Step 2.7: Commit**

```bash
git add src/dashboard/app.py src/dashboard/requirements.txt src/dashboard/test_app.py
git commit -m "feat: add GET /ods-records endpoint for live record browsing"
```

---

## Task 3: Frontend — CSS and panel HTML

**Files:**
- Modify: `src/dashboard/static/index.html`

- [ ] **Step 3.1: Add panel CSS**

In `src/dashboard/static/index.html`, find the comment `/* ── Footer ──` inside `<style>` and insert the following block immediately before it:

```css
  /* ── ODS Records Panel ──────────────────────────────────── */
  .ods-panel {
    margin: 0 48px 24px;
    background: var(--surface);
    border: 1px solid var(--border-hi);
    border-radius: 14px;
    overflow: hidden;
    box-shadow: 0 0 40px rgba(0,212,255,0.07);
  }

  .ods-panel-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 14px 20px;
    border-bottom: 1px solid var(--border);
    background: rgba(0,212,255,0.04);
  }

  .panel-tabs { display: flex; gap: 6px; }

  .panel-tab {
    padding: 5px 14px; border-radius: 6px;
    font-size: 11px; font-weight: 600;
    font-family: 'Roboto Mono', monospace;
    cursor: pointer; border: 1px solid transparent;
    background: none; color: var(--dim);
    transition: all 0.2s;
  }

  .panel-tab.active {
    background: rgba(0,212,255,0.15);
    border-color: rgba(0,212,255,0.3);
    color: var(--primary);
  }

  .panel-controls { display: flex; align-items: center; gap: 16px; }

  .panel-limit-sel {
    background: var(--surface-2);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 5px 10px; border-radius: 6px;
    font-size: 12px; font-family: 'Roboto Mono', monospace;
    cursor: pointer; outline: none;
  }
  .panel-limit-sel:hover { border-color: var(--border-hi); }

  .panel-collapse-btn {
    font-size: 11px; color: var(--dim); cursor: pointer;
    background: none; border: none; padding: 4px 8px; font-family: inherit;
  }
  .panel-collapse-btn:hover { color: var(--primary); }

  .records-table { width: 100%; border-collapse: collapse; }

  .records-table th {
    text-align: left; padding: 10px 16px;
    font-size: 10px; font-weight: 600; color: var(--dim);
    text-transform: uppercase; letter-spacing: 1px;
    border-bottom: 1px solid var(--border);
    background: rgba(0,0,0,0.15);
    font-family: 'Roboto Mono', monospace;
  }

  .records-table td {
    padding: 9px 16px;
    border-bottom: 1px solid rgba(0,212,255,0.05);
    font-family: 'Roboto Mono', monospace;
    font-size: 11px; color: var(--text);
  }

  .records-table tr:last-child td { border-bottom: none; }
  .records-table tr:hover td { background: rgba(0,212,255,0.03); }

  .ods-panel-footer {
    padding: 8px 20px;
    border-top: 1px solid var(--border);
    background: rgba(0,0,0,0.1);
    font-size: 10px; color: var(--dim);
    display: flex; justify-content: space-between;
  }

  .node.clickable { cursor: pointer; }
  .node.clickable:hover { border-color: var(--border-hi); }

  .node-hint {
    font-size: 9px; color: var(--primary);
    text-align: center; margin-top: 6px;
    opacity: 0.7; letter-spacing: 0.5px;
  }
```

- [ ] **Step 3.2: Make the ODS tile clickable**

Find the existing ODS node:

```html
    <div class="node" id="n-ods">
      <div class="node-badge" id="b-ods"></div>
      <div class="node-icon">🏛️</div>
      <div class="node-name">ODS</div>
      <div class="node-metric" id="m-ods-total">—</div>
      <div class="node-sub" id="m-ods-rate">— rec/s</div>
    </div>
```

Replace with:

```html
    <div class="node clickable" id="n-ods" onclick="toggleOdsPanel()" title="Click to view ODS records">
      <div class="node-badge" id="b-ods"></div>
      <div class="node-icon">🏛️</div>
      <div class="node-name">ODS</div>
      <div class="node-metric" id="m-ods-total">—</div>
      <div class="node-sub" id="m-ods-rate">— rec/s</div>
      <div class="node-hint" id="ods-hint">▾ view records</div>
    </div>
```

- [ ] **Step 3.3: Insert the panel HTML**

Find the comment `<!-- KPIs -->` in the HTML body and insert the following block immediately before it:

```html
<!-- ODS Records Panel -->
<div id="ods-panel" class="ods-panel" style="display:none">
  <div class="ods-panel-header">
    <div class="panel-tabs">
      <button class="panel-tab active" data-table="event" onclick="switchOdsTab(this)">bcdm.event</button>
      <button class="panel-tab" data-table="party" onclick="switchOdsTab(this)">bcdm.party</button>
      <button class="panel-tab" data-table="arrangement" onclick="switchOdsTab(this)">bcdm.arrangement</button>
    </div>
    <div class="panel-controls">
      <div class="live-badge"><div class="live-dot"></div>live</div>
      <select class="panel-limit-sel" id="panel-limit" onchange="fetchOdsRecords()">
        <option value="10">10 rows</option>
        <option value="25" selected>25 rows</option>
        <option value="50">50 rows</option>
        <option value="100">100 rows</option>
      </select>
      <button class="panel-collapse-btn" onclick="toggleOdsPanel()">▲ collapse</button>
    </div>
  </div>
  <table class="records-table">
    <thead id="ods-thead"><tr></tr></thead>
    <tbody id="ods-tbody">
      <tr><td colspan="6" style="text-align:center;color:var(--dim);padding:24px">Loading…</td></tr>
    </tbody>
  </table>
  <div class="ods-panel-footer">
    <span id="panel-row-desc">—</span>
    <span id="panel-updated">—</span>
  </div>
</div>

```

- [ ] **Step 3.4: Commit the HTML/CSS changes**

```bash
git add src/dashboard/static/index.html
git commit -m "feat: add ODS records panel HTML and CSS"
```

---

## Task 4: Frontend — JavaScript

**Files:**
- Modify: `src/dashboard/static/index.html`

- [ ] **Step 4.1: Add panel JS**

Find the comment `/* ── Generate transactions` inside the `<script>` block (near the bottom) and insert the following block immediately before it:

```javascript
/* ── ODS Records Panel ───────────────────────────────────── */
let _odsTable = 'event';
let _odsPollId = null;
let _odsPanelOpen = false;

const ODS_COLUMNS = {
  event:       ['event_id', 'event_type', 'event_amount', 'currency', 'event_timestamp', 'latency_s'],
  party:       ['party_id', 'party_type', 'first_name', 'last_name', 'source_system', 'integration_timestamp'],
  arrangement: ['arrangement_id', 'product_category', 'balance', 'status', 'source_system', 'integration_timestamp'],
};

const ODS_HEADERS = {
  event:       ['event_id', 'event_type', 'amount', 'currency', 'event_timestamp', 'integrated'],
  party:       ['party_id', 'party_type', 'first_name', 'last_name', 'source_system', 'integration_timestamp'],
  arrangement: ['arrangement_id', 'product_category', 'balance', 'status', 'source_system', 'integration_timestamp'],
};

function toggleOdsPanel() {
  _odsPanelOpen = !_odsPanelOpen;
  const panel = document.getElementById('ods-panel');
  const hint  = document.getElementById('ods-hint');
  if (_odsPanelOpen) {
    panel.style.display = '';
    hint.textContent = '▲ collapse';
    fetchOdsRecords();
    startOdsPoll();
  } else {
    panel.style.display = 'none';
    hint.textContent = '▾ view records';
    stopOdsPoll();
  }
}

function startOdsPoll() {
  stopOdsPoll();
  _odsPollId = setInterval(fetchOdsRecords, 2000);
}

function stopOdsPoll() {
  if (_odsPollId !== null) {
    clearInterval(_odsPollId);
    _odsPollId = null;
  }
}

function switchOdsTab(btn) {
  document.querySelectorAll('.panel-tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  _odsTable = btn.dataset.table;
  document.getElementById('ods-tbody').innerHTML =
    '<tr><td colspan="6" style="text-align:center;color:var(--dim);padding:24px">Loading…</td></tr>';
  fetchOdsRecords();
}

async function fetchOdsRecords() {
  const limit = document.getElementById('panel-limit').value;
  try {
    const r    = await fetch(`/ods-records?table=${_odsTable}&limit=${limit}`);
    const data = await r.json();
    if (!r.ok) { renderOdsError(data.detail || 'Unknown error'); return; }
    renderOdsRecords(data);
  } catch (err) {
    renderOdsError(err.message);
  }
}

function renderOdsError(msg) {
  const cols = ODS_COLUMNS[_odsTable].length;
  document.getElementById('ods-tbody').innerHTML =
    `<tr><td colspan="${cols}" style="text-align:center;color:var(--danger);padding:24px">⚠ ${msg}</td></tr>`;
}

function _fmtUuid(s)    { return s ? s.slice(0, 8) + '…' : '—'; }
function _fmtTs(s)      { return s ? s.replace('T', ' ').slice(0, 19) : '—'; }
function _fmtLatency(n) { return (n === null || n === undefined) ? '—' : `+${parseFloat(n).toFixed(2)}s`; }
function _fmtAmount(n)  {
  return (n === null || n === undefined) ? '—'
    : parseFloat(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function renderOdsRecords(rows) {
  const headers = ODS_HEADERS[_odsTable];
  const cols    = ODS_COLUMNS[_odsTable];

  document.getElementById('ods-thead').innerHTML =
    '<tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr>';

  if (!rows.length) {
    document.getElementById('ods-tbody').innerHTML =
      `<tr><td colspan="${cols.length}" style="text-align:center;color:var(--dim);padding:24px">No records yet</td></tr>`;
  } else {
    document.getElementById('ods-tbody').innerHTML = rows.map(row => {
      const cells = cols.map(col => {
        const v = row[col];
        if (col.endsWith('_id'))
          return `<td style="color:var(--dim);font-size:10px">${_fmtUuid(v)}</td>`;
        if (col === 'latency_s')
          return `<td style="color:var(--success)">${_fmtLatency(v)}</td>`;
        if (col === 'event_amount' || col === 'balance')
          return `<td style="color:var(--success)">${_fmtAmount(v)}</td>`;
        if (col === 'event_type' || col === 'party_type' || col === 'product_category')
          return `<td style="color:var(--primary)">${v || '—'}</td>`;
        if (col.endsWith('_timestamp'))
          return `<td style="color:var(--dim);font-size:10px">${_fmtTs(v)}</td>`;
        return `<td>${v || '—'}</td>`;
      });
      return '<tr>' + cells.join('') + '</tr>';
    }).join('');
  }

  const now = new Date().toLocaleTimeString('en-GB', { hour12: false });
  setText('panel-row-desc', `Showing ${rows.length} most recent records · sorted by integration_timestamp DESC`);
  setText('panel-updated', `Updated ${now}`);
}

```

- [ ] **Step 4.2: Commit the JS**

```bash
git add src/dashboard/static/index.html
git commit -m "feat: add ODS records panel JS — toggle, poll, render"
```

---

## Task 5: Verify end-to-end

- [ ] **Step 5.1: Run the full test suite one final time**

```bash
cd src/dashboard && ../../venv/bin/python -m pytest test_app.py -v
```

Expected: all tests pass (the 6 original + 5 new = 11 total).

- [ ] **Step 5.2: Start the dashboard and verify the feature**

If the infrastructure is running (`docker compose ... up`), launch the dashboard:

```bash
cd src/dashboard && ../../venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

Open `http://localhost:8000`. Verify:

1. ODS tile shows a pointer cursor and `▾ view records` hint below the metrics.
2. Clicking the tile expands the panel below the pipeline; hint changes to `▲ collapse`.
3. Panel shows `bcdm.event` tab by default with rows loading within 2 seconds.
4. Switching to `bcdm.party` or `bcdm.arrangement` fetches and renders different columns.
5. Changing the row count selector immediately refreshes the table.
6. Clicking the tile again (or the `▲ collapse` button) hides the panel and stops polling.
7. KPIs and the latency chart are still visible below the panel (scroll down).

- [ ] **Step 5.3: Final commit**

If any tweaks were needed during verification, commit them now with an appropriate message. If no changes were needed, this step is done.
