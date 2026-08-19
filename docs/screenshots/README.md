# Screenshots

Free Edition resources aren't permanently live (compute is serverless and
quota-capped, and the account may be reclaimed if inactive), so the evidence
that everything ran lives here as screenshots, referenced from the main README.

## Capture checklist

### Stage 1 — Bronze
- [ ] `01-landing-volume.png` — Catalog Explorer: the `landing` volume showing per-board NDJSON files
- [ ] `02-bronze-run.png` — the Auto Loader notebook after a run: batch metrics cell output
- [ ] `03-bronze-table.png` — Catalog Explorer: `jobs_bronze.postings_raw` with table + column comments visible
- [ ] `04-bronze-rerun-noop.png` — second run ingesting 0 new files (exactly-once evidence)

### Stage 2 — Silver
- [ ] `10-pipeline-graph.png` — Lakeflow pipeline DAG
- [ ] `11-expectations.png` — expectations results panel (kept/dropped row counts)
- [ ] `12-expectation-fail.png` — the deliberate fail-expectation demo stopping the pipeline

### Stage 3 — Gold
- [ ] `20-pipeline-graph-full.png` — full bronze→silver→gold DAG
- [ ] `21-lineage.png` — UC lineage graph for one gold table

### Stage 4 — Job
- [ ] `30-job-run.png` — job run with both tasks green

### Stage 5 — Dashboard
- [ ] `40-dashboard.png` — the DBSQL dashboard over gold

Capture at ~1600px wide, PNG, no account ids / workspace URLs in frame if
avoidable (crop the browser chrome).
