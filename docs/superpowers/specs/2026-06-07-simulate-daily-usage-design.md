# Simulate Daily Usage — Design Spec

**Date:** 2026-06-07  
**Status:** Approved

---

## Overview

Add a continuous transaction simulation mode to the ODS dashboard. The user can configure randomised batch size and interval ranges, then start a drip of transactions that runs until explicitly stopped. This lets the pipeline be observed under sustained, realistic-looking load without manually clicking "Inject Now" repeatedly.

---

## UI Layout

The existing **Actions** section is extended in two parts:

### 1. Simulate toggle button

A `▸ Simulate` button sits alongside the existing `⚡ Inject Now` button. Clicking it shows or hides the `#sim-controls` row. Styled as a secondary button (outlined, not filled). Disabled while a simulation is running.

### 2. `#sim-controls` row (hidden by default)

Revealed below the main actions row when the toggle is clicked. Contains:

| Element | Default | Description |
|---|---|---|
| Min batch input (`#sim-min-txn`) | 1 | Minimum transactions per injection |
| Max batch input (`#sim-max-txn`) | 20 | Maximum transactions per injection |
| Min interval input (`#sim-min-ms`) | 500 | Minimum ms between injections |
| Max interval input (`#sim-max-ms`) | 3000 | Maximum ms between injections |
| Start/Stop button (`#sim-btn`) | `▶ Start Simulation` | Toggles the sim loop |
| Status span (`#sim-status`) | — | Live feedback text |

Inputs are disabled while simulation is running. All inputs use the existing `select#count-sel` visual style.

---

## JS Simulation Loop

State:

```
let _simRunning = false;
let _simTotal   = 0;
```

### `startSim()`

1. Validates that minTxn < maxTxn and minMs < maxMs — shows error and returns if not
2. Sets `_simRunning = true`, resets `_simTotal = 0`
3. Updates button to `⏹ Stop`, disables inputs and the `▸ Simulate` toggle
4. Calls `simTick()`

### `simTick()`

1. Guards: `if (!_simRunning) return`
2. Reads the four input values
3. Picks `count = rand(minTxn, maxTxn)` and `delay = rand(minMs, maxMs)`
4. POSTs to existing `/generate?count=N`
5. On success: accumulates `_simTotal`, updates status to `Simulating… N injected`
6. On error: updates status in `--danger` colour, **loop continues**
7. Schedules `setTimeout(simTick, delay)`

### `stopSim()`

1. Sets `_simRunning = false`
2. Updates button to `▶ Start Simulation`, re-enables inputs and toggle
3. Updates status to `Stopped — N total injected`

### Interaction with existing controls

- `⚡ Inject Now` is disabled while `_simRunning` is true
- `▸ Simulate` toggle is disabled while `_simRunning` is true

---

## Error Handling & Edge Cases

| Scenario | Behaviour |
|---|---|
| `/generate` returns an error | Status shows error in `--danger` colour; loop continues |
| min ≥ max (batch or interval) | Validation error on Start; sim does not start |
| Tab closed / page refreshed | Simulation stops — browser-managed state by design |
| Controls hidden mid-run | Toggle is disabled while running; not possible |

---

## Scope

- **No changes to `app.py`** — reuses the existing `POST /generate?count=N` endpoint
- All new code is in `src/dashboard/static/index.html` (CSS + JS)
- No new endpoints, no server state
