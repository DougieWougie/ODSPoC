# Simulate Daily Usage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a browser-side continuous simulation mode to the dashboard Actions section, with configurable random batch size and interval ranges and a start/stop toggle.

**Architecture:** Pure frontend change — a `▸ Simulate` toggle button reveals a hidden `#sim-controls` row with four number inputs and a start/stop button. A JS `setTimeout` loop calls the existing `POST /generate?count=N` endpoint repeatedly with random count and delay values read from those inputs. No backend changes.

**Tech Stack:** Vanilla JS, HTML, CSS — all inline in `src/dashboard/static/index.html`.

---

## File Structure

- **Modify:** `src/dashboard/static/index.html`
  - CSS block: add `.sim-toggle-btn`, `.sim-input`, `.sim-start-btn` styles
  - HTML `.actions` block: add `▸ Simulate` toggle button and hidden `#sim-controls` row
  - JS block: add `_simRunning`, `_simTotal`, `rand()`, `toggleSimControls()`, `toggleSim()`, `startSim()`, `stopSim()`, `simTick()`

No other files are touched.

---

### Task 1: Add CSS for simulation elements

**Files:**
- Modify: `src/dashboard/static/index.html` (CSS `<style>` block, after the `#gen-btn.burst` rule at line ~344)

- [ ] **Step 1: Add the CSS rules**

In `src/dashboard/static/index.html`, find the rule `#gen-btn.burst {` (around line 341) and add the following CSS immediately after its closing `}`:

```css
  .sim-toggle-btn {
    background: transparent;
    border: 1px solid var(--border);
    color: var(--dim);
    padding: 12px 20px;
    font-size: 13px; font-weight: 600; font-family: inherit;
    letter-spacing: 0.5px;
    border-radius: 9px; cursor: pointer;
    transition: border-color 0.2s, color 0.2s;
  }
  .sim-toggle-btn:hover:not(:disabled) { border-color: var(--border-hi); color: var(--primary); }
  .sim-toggle-btn:disabled { opacity: 0.45; cursor: not-allowed; }
  .sim-toggle-btn.active { border-color: var(--border-hi); color: var(--primary); }

  .sim-input {
    background: var(--surface-2);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 12px 10px; border-radius: 9px;
    font-size: 14px; font-family: 'Roboto Mono', monospace;
    width: 72px; text-align: center;
    outline: none;
    transition: border-color 0.2s;
  }
  .sim-input:hover, .sim-input:focus { border-color: var(--border-hi); }
  .sim-input:disabled { opacity: 0.45; }

  .sim-start-btn {
    background: linear-gradient(135deg, #00552a, #00a855);
    border: 1px solid rgba(0,168,85,0.3);
    color: #fff;
    padding: 13px 28px;
    font-size: 13px; font-weight: 700; font-family: inherit;
    letter-spacing: 1.5px; text-transform: uppercase;
    border-radius: 9px; cursor: pointer;
    transition: opacity 0.2s, transform 0.1s, box-shadow 0.2s;
  }
  .sim-start-btn:hover:not(:disabled) {
    opacity: 0.92; transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(0,168,85,0.3);
  }
  .sim-start-btn:active:not(:disabled) { transform: translateY(0); }
  .sim-start-btn:disabled { opacity: 0.45; cursor: not-allowed; }
  .sim-start-btn.running {
    background: linear-gradient(135deg, #6b0000, #d42040);
    border-color: rgba(212,32,64,0.3);
  }
```

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/static/index.html
git commit -m "feat: add CSS for simulation controls"
```

---

### Task 2: Add HTML — toggle button and hidden sim-controls row

**Files:**
- Modify: `src/dashboard/static/index.html` (`.actions` div, around line 704)

- [ ] **Step 1: Add the `▸ Simulate` toggle button**

Find the `.actions` div, which currently ends with:
```html
  <button id="gen-btn" onclick="generate()">⚡ Inject Now</button>
  <div class="action-status" id="action-status"></div>
</div>
```

Replace it with:
```html
  <button id="gen-btn" onclick="generate()">⚡ Inject Now</button>
  <div class="action-status" id="action-status"></div>
  <button id="sim-toggle-btn" class="sim-toggle-btn" onclick="toggleSimControls()">▸ Simulate</button>

  <div id="sim-controls" style="display:none; flex-basis:100%; width:100%; align-items:center; gap:16px; padding-top:4px; flex-wrap:wrap;">
    <span class="action-label">Batch</span>
    <input type="number" id="sim-min-txn" class="sim-input" value="1"    min="1"   title="Min transactions per injection">
    <span style="color:var(--dim);font-size:13px">→</span>
    <input type="number" id="sim-max-txn" class="sim-input" value="20"   min="1"   title="Max transactions per injection">
    <span class="action-label" style="margin-left:8px">Interval (ms)</span>
    <input type="number" id="sim-min-ms"  class="sim-input" value="500"  min="100" title="Min ms between injections">
    <span style="color:var(--dim);font-size:13px">→</span>
    <input type="number" id="sim-max-ms"  class="sim-input" value="3000" min="100" title="Max ms between injections">
    <button id="sim-btn" class="sim-start-btn" onclick="toggleSim()">▶ Start Simulation</button>
    <div class="action-status" id="sim-status"></div>
  </div>
</div>
```

Note: `#sim-controls` uses inline `display:none` to start hidden. The `toggleSimControls()` function switches it to `display:flex`.

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/static/index.html
git commit -m "feat: add simulate toggle button and hidden controls row HTML"
```

---

### Task 3: Add JS state, `rand()`, and `toggleSimControls()`

**Files:**
- Modify: `src/dashboard/static/index.html` (JS `<script>` block, after the `generate()` function at line ~1275)

- [ ] **Step 1: Add state variables and helpers**

Find the end of the `generate()` function closing brace and add the following immediately after:

```javascript
/* ── Simulation ───────────────────────────────────────────── */
let _simRunning = false;
let _simTotal   = 0;
let _simOpen    = false;

function rand(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function toggleSimControls() {
  _simOpen = !_simOpen;
  const el  = document.getElementById('sim-controls');
  const btn = document.getElementById('sim-toggle-btn');
  el.style.display   = _simOpen ? 'flex' : 'none';
  btn.textContent    = _simOpen ? '▾ Simulate' : '▸ Simulate';
  btn.classList.toggle('active', _simOpen);
}
```

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/static/index.html
git commit -m "feat: add simulation state variables and toggleSimControls"
```

---

### Task 4: Add `startSim()` and `stopSim()`

**Files:**
- Modify: `src/dashboard/static/index.html` (JS block, immediately after `toggleSimControls()`)

- [ ] **Step 1: Add `toggleSim()`, `startSim()`, and `stopSim()`**

Add immediately after `toggleSimControls()`:

```javascript
function toggleSim() {
  if (_simRunning) stopSim();
  else startSim();
}

function startSim() {
  const minTxn = parseInt(document.getElementById('sim-min-txn').value);
  const maxTxn = parseInt(document.getElementById('sim-max-txn').value);
  const minMs  = parseInt(document.getElementById('sim-min-ms').value);
  const maxMs  = parseInt(document.getElementById('sim-max-ms').value);
  const status = document.getElementById('sim-status');

  if (minTxn >= maxTxn) {
    status.textContent = '✗ Min batch must be less than max batch';
    status.style.color = 'var(--danger)';
    return;
  }
  if (minMs >= maxMs) {
    status.textContent = '✗ Min interval must be less than max interval';
    status.style.color = 'var(--danger)';
    return;
  }

  _simRunning = true;
  _simTotal   = 0;

  const btn = document.getElementById('sim-btn');
  btn.textContent = '⏹ Stop';
  btn.classList.add('running');

  document.querySelectorAll('.sim-input').forEach(el => el.disabled = true);
  document.getElementById('sim-toggle-btn').disabled = true;
  document.getElementById('gen-btn').disabled = true;

  status.textContent = 'Starting…';
  status.style.color = 'var(--dim)';

  simTick();
}

function stopSim() {
  _simRunning = false;

  const btn = document.getElementById('sim-btn');
  btn.textContent = '▶ Start Simulation';
  btn.classList.remove('running');

  document.querySelectorAll('.sim-input').forEach(el => el.disabled = false);
  document.getElementById('sim-toggle-btn').disabled = false;
  document.getElementById('gen-btn').disabled = false;

  const status = document.getElementById('sim-status');
  status.textContent = `Stopped — ${_simTotal.toLocaleString()} total injected`;
  status.style.color = 'var(--dim)';
}
```

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/static/index.html
git commit -m "feat: add startSim and stopSim functions"
```

---

### Task 5: Add `simTick()`

**Files:**
- Modify: `src/dashboard/static/index.html` (JS block, immediately after `stopSim()`)

- [ ] **Step 1: Add `simTick()`**

Add immediately after `stopSim()`:

```javascript
async function simTick() {
  if (!_simRunning) return;

  const minTxn = parseInt(document.getElementById('sim-min-txn').value);
  const maxTxn = parseInt(document.getElementById('sim-max-txn').value);
  const minMs  = parseInt(document.getElementById('sim-min-ms').value);
  const maxMs  = parseInt(document.getElementById('sim-max-ms').value);

  const count  = rand(minTxn, maxTxn);
  const delay  = rand(minMs, maxMs);
  const status = document.getElementById('sim-status');

  try {
    const r    = await fetch(`/generate?count=${count}`, { method: 'POST' });
    const data = await r.json();
    if (r.ok) {
      _simTotal += data.inserted;
      status.textContent = `Simulating… ${_simTotal.toLocaleString()} injected`;
      status.style.color = 'var(--success)';
    } else {
      status.textContent = `⚠ ${data.detail || 'server error'} — continuing`;
      status.style.color = 'var(--danger)';
    }
  } catch (err) {
    status.textContent = `⚠ ${err.message} — continuing`;
    status.style.color = 'var(--danger)';
  }

  if (_simRunning) setTimeout(simTick, delay);
}
```

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/static/index.html
git commit -m "feat: add simTick async loop"
```

---

### Task 6: Manual verification

The simulation feature has no automated frontend tests (no JS test framework in this project). Verify manually against the running dashboard.

- [ ] **Step 1: Start the dashboard**

```bash
cd /home/dougiewougie/Projects/architecture/ods
./venv/bin/uvicorn src.dashboard.app:app --host 0.0.0.0 --port 8000 --reload
```

Open `http://localhost:8000` in a browser.

- [ ] **Step 2: Verify toggle reveal**

Click `▸ Simulate`. The controls row should appear with four inputs (defaults: 1, 20, 500, 3000) and a green `▶ Start Simulation` button. Button text should change to `▾ Simulate`.

Click `▾ Simulate` again. Controls row should hide. Button text reverts to `▸ Simulate`.

- [ ] **Step 3: Verify validation**

Open controls. Set Min batch = 20, Max batch = 5. Click `▶ Start Simulation`. Expect error: "✗ Min batch must be less than max batch". Sim does not start.

Set Min interval = 3000, Max interval = 500. Click Start. Expect error: "✗ Min interval must be less than max interval". Sim does not start.

- [ ] **Step 4: Verify simulation runs**

With infrastructure running (docker compose up), set defaults (1→20 txn, 500→3000 ms). Click `▶ Start Simulation`. Expect:
- Status shows `Simulating… N injected`, count climbing each tick
- Button turns red `⏹ Stop`
- Inputs are disabled
- `▸ Simulate` toggle is disabled
- `⚡ Inject Now` is disabled
- Source DB txn/s counter on the pipeline nodes rises
- Particles animate on the pipeline canvas

- [ ] **Step 5: Verify stop**

Click `⏹ Stop`. Expect:
- Status shows `Stopped — N total injected`
- Button reverts to green `▶ Start Simulation`
- All inputs and buttons re-enabled

- [ ] **Step 6: Verify error recovery**

With infrastructure down, start simulation. Expect status shows `⚠ … — continuing` in danger colour but loop keeps running (doesn't crash or freeze).

- [ ] **Step 7: Final commit**

```bash
git add src/dashboard/static/index.html
git commit -m "feat: simulate daily usage — configurable random transaction drip"
```
