# ODS Records Panel Design

**Date:** 2026-06-07
**Status:** Approved

## Overview

Clicking the ODS tile on the dashboard expands an inline panel directly below the pipeline. The panel shows live records from the three BCDM tables (`bcdm.event`, `bcdm.party`, `bcdm.arrangement`), auto-refreshing every 2 seconds via a new polling endpoint. Everything above and below the panel (pipeline, KPIs, chart) remains visible.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| UI pattern | Inline expansion below pipeline | Dashboard context (pipeline, KPIs) stays visible; no overlay clutter |
| Data delivery | Separate polling endpoint (`GET /ods-records`) | Keeps SSE stream lean; only queries DB when panel is open |
| Default tab | `bcdm.event` | Transactions are the primary load under test |
| Refresh rate | 2 s (matches SSE cadence) | Feels live without hammering the DB |
| Row count | User-selectable: 10 / 25 / 50 / 100 | Lets user control verbosity; default 25 |
| Sort order | `integration_timestamp DESC` | Most recent records first — most relevant during live experiments |

## Component Design

### 1. Backend — `GET /ods-records` in `app.py`

New FastAPI endpoint. Query params:

| Param | Type | Default | Values |
|---|---|---|---|
| `table` | str | `event` | `event`, `party`, `arrangement` |
| `limit` | int | 25 | 10, 25, 50, 100 |

**Queries per table:**

- `bcdm.event` — `SELECT event_id, event_type, event_amount, currency, event_timestamp, EXTRACT(EPOCH FROM (integration_timestamp - event_timestamp)) AS latency_s FROM bcdm.event ORDER BY integration_timestamp DESC LIMIT %s`
- `bcdm.party` — `SELECT party_id, party_type, first_name, last_name, source_system, integration_timestamp FROM bcdm.party ORDER BY integration_timestamp DESC LIMIT %s`
- `bcdm.arrangement` — `SELECT arrangement_id, product_category, balance, status, source_system, integration_timestamp FROM bcdm.arrangement ORDER BY integration_timestamp DESC LIMIT %s`

**Response:** JSON array of row dicts. Column names match the SELECT list. UUIDs returned as strings.

**Error handling:** Return `{"error": str(e)}` with HTTP 500 on DB failure. The frontend shows an error state in the panel rather than crashing.

**Validation:** `table` must be one of the three allowed values; return HTTP 400 otherwise (prevents SQL injection via table name).

### 2. Frontend — ODS tile

The `#n-ods` node gets:
- `cursor: pointer` style
- A `title` attribute: `"Click to view ODS records"`
- `onclick="toggleOdsPanel()"` handler
- A subtle downward chevron label below the tile when the panel is closed (`▾ view records`), swapping to `▲ collapse` when open

No change to the tile's existing metric display.

### 3. Frontend — Inline panel (`#ods-panel`)

Inserted in the HTML immediately after `<div class="pipeline-section">` and before `<div class="kpis">`. Hidden by default (`display: none`).

**Structure:**

```
#ods-panel
  .ods-panel-header
    .panel-tabs          — bcdm.event | bcdm.party | bcdm.arrangement
    .panel-controls      — live dot · row count <select> · ▲ collapse button
  <table class="records-table">
    <thead>              — columns depend on active tab (see below)
    <tbody id="ods-tbody">
  .panel-footer          — "Showing N most recent records · sorted by integration_timestamp DESC" · "Updated Xs ago"
```

**Columns per tab:**

| Tab | Columns |
|---|---|
| `bcdm.event` | event_id (truncated to 8 chars + …), event_type, event_amount, currency, event_timestamp, integrated (+Xs delta) |
| `bcdm.party` | party_id (truncated), party_type, first_name, last_name, source_system, integration_timestamp |
| `bcdm.arrangement` | arrangement_id (truncated), product_category, balance, status, source_system, integration_timestamp |

The `integrated` column on the event tab shows the per-row latency as `+Xs`. The delta is computed in SQL (`EXTRACT(EPOCH FROM (integration_timestamp - event_timestamp))`) and returned as a float; the frontend formats it as `+0.18s`.

**Auto-refresh:**

`startOdsPoll()` / `stopOdsPoll()` manage a `setInterval` at 2000 ms. Polling starts when the panel opens, stops when it closes. On each tick, fetch `/ods-records?table=<active>&limit=<selected>`, then re-render `<tbody>` and update the footer timestamp.

**Tab switching:** Clicking a tab updates the active tab state, clears the tbody, and immediately fires a fresh fetch (no wait for the next 2 s tick).

**Row count selector:** `onchange` immediately fires a fresh fetch with the new limit.

**Error state:** If the fetch fails or returns `{"error": ...}`, replace tbody with a single full-width row: `⚠ Could not load records — <error message>`.

**Styling:** Matches the existing dashboard design system (same CSS variables, `--surface`, `--border`, `--primary`, `--dim`, `Roboto Mono` for data cells). Panel has `border-color: var(--border-hi)` and a faint `box-shadow` to distinguish it as interactive content.

## File Change Summary

| File | Change |
|---|---|
| `src/dashboard/app.py` | Add `GET /ods-records` endpoint with table/limit params and validation |
| `src/dashboard/static/index.html` | Add `#ods-panel` HTML + CSS, `toggleOdsPanel()`, `startOdsPoll()`, `stopOdsPoll()`, render logic; add `onclick` + cursor + chevron hint to `#n-ods` |

## Out of Scope

- Filtering or searching records within the panel.
- Clicking a row to see full record detail.
- Showing Source DB records for comparison.
- Pagination beyond the row-count selector.
