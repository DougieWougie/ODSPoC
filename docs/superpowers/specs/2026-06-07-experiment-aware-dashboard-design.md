# Experiment-Aware Dashboard Design

**Date:** 2026-06-07
**Status:** Approved

## Overview

The ODS dashboard currently has no awareness of which experiment (A, B, or C) is running. This design adds:

1. A sub-header banner identifying the active experiment by name and key parameters.
2. An enhanced Transformer tile showing live replica counts (running / requested).

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| How dashboard learns experiment | State file (`.experiment`) written by `fresh_test.sh` | Ground truth — the script knows exactly what it set up |
| How running count is obtained | Docker API via `subprocess` | Shows actual live state (e.g., `8 / 10` if replicas crash) |
| Experiment indicator placement | Sub-header banner below `<header>` | Always visible, descriptive without cluttering the pipeline view |
| Sub-header content | Letter chip + full name + key params | Useful context when presenting the PoC to unfamiliar audiences |
| Transformer tile style | Count display: `running / requested` | Directly answers the "how many are running" question; clean for all three experiments |

## Component Design

### 1. State file — `fresh_test.sh` and `teardown.sh`

`fresh_test.sh` writes the experiment letter to `.experiment` in the project root immediately after the `case` block that scales transformer replicas (~line 108):

```bash
echo "$EXPERIMENT" > "$SCRIPT_DIR/.experiment"
```

`teardown.sh` removes it on teardown:

```bash
rm -f "$SCRIPT_DIR/.experiment"
```

### 2. Backend — `experiment_metrics()` in `app.py`

New function added alongside the existing metric functions. Included in the `asyncio.gather()` call inside `event_stream()` and published in the SSE payload as the `"experiment"` key.

**Experiment metadata table (hardcoded):**

| Experiment | Name | Detail | Requested |
|---|---|---|---|
| A | Single Node Bottleneck | 1 transformer | 1 |
| B | Distributed Stream Processing | 10 transformers · 10 Kafka partitions | 10 |
| C | Data Virtualization | SQL views, no stream processors | 0 |

**Logic:**
- Read `.experiment` from the project root (two levels up from `app.py`).
- If absent or unrecognised, return `{"experiment": null}`.
- Look up metadata by letter.
- Count running transformer containers via:
  ```
  docker ps --filter label=com.docker.compose.service=transformer --filter status=running --format {{.Names}}
  ```
  Count non-empty lines in stdout.
- Return: `{"experiment": "B", "name": "...", "detail": "...", "running": 10, "requested": 10}`
- On any exception return `{"experiment": null, "err": str(e)}`.

**SSE payload addition:**
```python
payload = {
    ...existing keys...,
    "experiment": exp,   # result of experiment_metrics()
}
```

### 3. Frontend — Sub-header banner

New `<div id="exp-banner">` inserted immediately after `</header>`, before the pipeline section. Hidden by default (`display:none`).

**Rendered content when active (Exp B example):**

```
Active  [ EXP B ]  Distributed Stream Processing — 10 transformers · 10 Kafka partitions
```

**Styling:**
- Background: `rgba(0,212,255,0.06)` (matching existing surface tones)
- Bottom border: `1px solid var(--border)`
- Experiment chip: `--primary` text, `rgba(0,212,255,0.12)` background, `border-radius: 4px`
- Description text: `var(--dim)` with experiment name in `var(--text)`
- Padding: `7px 48px` (matching header/pipeline horizontal rhythm)

**`renderData()` behaviour:**
- If `d.experiment?.experiment` is non-null: set `display: flex`, update chip text and description.
- Otherwise: set `display: none`.

### 4. Frontend — Transformer tile

Two new elements added below `#m-tfm-status` in the Transformer node:

```html
<div id="m-tfm-count" class="node-sub" style="display:none; font-family:'Roboto Mono'; color:var(--primary); font-size:13px; font-weight:700; margin-top:4px;">— / —</div>
<div id="m-tfm-count-label" class="node-sub" style="display:none;">running / requested</div>
```

**`renderData()` behaviour:**

| Condition | `m-tfm-status` | `m-tfm-count` | `m-tfm-count-label` |
|---|---|---|---|
| No experiment file | ACTIVE / IDLE / OFFLINE (existing) | hidden | hidden |
| Exp A or B, ODS writing | ACTIVE | `running / requested` (e.g., `10 / 10`) | `running / requested` |
| Exp A or B, ODS idle | IDLE | `running / requested` | `running / requested` |
| Exp C | SQL VIEWS (warning colour) | `— / —` | `No stream processor` |

## File Change Summary

| File | Change |
|---|---|
| `fresh_test.sh` | Write `.experiment` after scaling |
| `teardown.sh` | Remove `.experiment` on teardown |
| `src/dashboard/app.py` | Add `experiment_metrics()`, add `import subprocess`, include in gather + payload |
| `src/dashboard/static/index.html` | Add banner HTML + CSS, add tile elements, update `renderData()` |

## Out of Scope

- Persisting experiment history or results.
- Showing Kafka partition count on the Kafka tile (separate concern).
- Any change to how experiments are started or measured.
