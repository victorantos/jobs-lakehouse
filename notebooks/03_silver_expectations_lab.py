# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver verification & expectations lab
# MAGIC
# MAGIC Run AFTER the pipeline's first successful update (`pipelines/README.md`
# MAGIC has the creation steps). Three parts:
# MAGIC
# MAGIC 1. **Verify the silver contract** — dedupe, conformance, salary trust
# MAGIC    rules, all against queries you can eyeball.
# MAGIC 2. **Read expectation metrics** from the pipeline event log — data
# MAGIC    quality as *data*, not as a dashboard screenshot.
# MAGIC 3. **Break things on purpose** — a guarded demo that makes a drop
# MAGIC    expectation visibly drop and a fail expectation stop the pipeline,
# MAGIC    plus the recovery procedure. This is the part interviews ask about.

# COMMAND ----------

import sys, os

repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import (
    BRONZE_POSTINGS, EVENT_LOG_TABLE, LANDING_ROOT,
    SILVER_POSTINGS, SILVER_POSTING_SKILLS,
    SILVER_SALARY_FACTS, SILVER_SALARY_QUARANTINE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1a — The funnel: bronze rows → typed → one row per posting
# MAGIC Bronze counts every exported version; silver's CDC step collapses to
# MAGIC one current row per (board, source_id). With a single export batch the
# MAGIC two are close; after your next export they diverge — that gap IS the
# MAGIC latest-wins dedupe working.

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'bronze rows' AS stage, COUNT(*) AS n FROM {BRONZE_POSTINGS}
    UNION ALL
    SELECT 'silver postings (current per source posting)', COUNT(*) FROM {SILVER_POSTINGS}
    UNION ALL
    SELECT 'silver postings, canonical only (cross-board dedup)', COUNT(*)
    FROM {SILVER_POSTINGS} WHERE is_canonical
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b — Cross-board duplicates, measured in silver
# MAGIC Against the full dataset this should echo the profiling numbers (303
# MAGIC computer↔solar + 198 coffee↔computer postings sharing a SourceUrl).
# MAGIC The committed samples deliberately include ~50 of those pairs (a plain
# MAGIC stride would almost never catch both halves — see
# MAGIC `tools/export_postings.py cut_samples`), so expect ~50 groups here.

# COMMAND ----------

display(spark.sql(f"""
    SELECT array_sort(collect_set(board))     AS boards_in_group,
           CASE WHEN dup_key LIKE 'url:%' THEN 'source_url' ELSE 'fingerprint' END AS matched_by,
           COUNT(DISTINCT dup_key)            AS dup_groups,
           SUM(CASE WHEN NOT is_canonical THEN 1 ELSE 0 END) AS redundant_postings
    FROM {SILVER_POSTINGS}
    WHERE is_cross_board_duplicate
    GROUP BY 1, 2 ORDER BY dup_groups DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1c — Conformance: 120+ raw spellings → 11 canonical values
# MAGIC The `other`/`unknown` share is the honest tail. The warn-expectation
# MAGIC `employment_conformed` tracks exactly this number per run in the event
# MAGIC log — improve `src/conform.py`, re-run, watch the metric move.

# COMMAND ----------

display(spark.sql(f"""
    SELECT employment_type, COUNT(*) AS postings,
           ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM {SILVER_POSTINGS}
    GROUP BY 1 ORDER BY postings DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT seniority, seniority_source, COUNT(*) AS postings
    FROM {SILVER_POSTINGS}
    GROUP BY 1, 2 ORDER BY postings DESC LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1d — Salary trust rules at work
# MAGIC Every effective currency/period carries a confidence flag. Look for
# MAGIC `inferred_geo_over_default` — those are gen_a rows whose published
# MAGIC "USD" the pipeline overruled because the geography disagreed (the
# MAGIC measured lying-default bug).

# COMMAND ----------

display(spark.sql(f"""
    SELECT salary_currency, salary_currency_confidence,
           salary_period, salary_period_confidence, COUNT(*) AS rows
    FROM {SILVER_SALARY_FACTS}
    GROUP BY 1, 2, 3, 4 ORDER BY rows DESC LIMIT 25
"""))

# COMMAND ----------

# What got rejected, and why — the quarantine is the debuggable half of the
# drop expectations on salary_facts.
display(spark.sql(f"""
    SELECT reject_reason, board, COUNT(*) AS rows,
           MIN(salary_mid_annual_usd) AS min_annual_usd,
           MAX(salary_mid_annual_usd) AS max_annual_usd
    FROM {SILVER_SALARY_QUARANTINE}
    GROUP BY 1, 2 ORDER BY rows DESC
"""))

# COMMAND ----------

# Top skills across the three encodings — coffee's JSON-in-string and the
# clones' tag tables land in the same column.
display(spark.sql(f"""
    SELECT skill, COUNT(DISTINCT posting_key) AS postings,
           array_sort(collect_set(board)) AS boards
    FROM {SILVER_POSTING_SKILLS}
    GROUP BY skill ORDER BY postings DESC LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Expectation metrics from the event log
# MAGIC The pipeline publishes its event log to Unity Catalog
# MAGIC (`_pipeline_event_log`, configured in the pipeline's advanced
# MAGIC settings). Every update appends `flow_progress` events whose
# MAGIC `data_quality.expectations` payload carries pass/fail counts per
# MAGIC expectation per table. This query unpacks the latest update — the same
# MAGIC numbers the pipeline UI shows, but queryable, joinable and plottable.

# COMMAND ----------

display(spark.sql(f"""
    WITH latest_update AS (
        SELECT origin.update_id AS id
        FROM {EVENT_LOG_TABLE}
        WHERE event_type = 'create_update'
        ORDER BY timestamp DESC LIMIT 1
    )
    SELECT ex.dataset,
           ex.name AS expectation,
           SUM(ex.passed_records) AS passed,
           SUM(ex.failed_records) AS failed,
           ROUND(100 * SUM(ex.failed_records)
                 / NULLIF(SUM(ex.passed_records + ex.failed_records), 0), 2)
               AS failed_pct
    FROM (
        SELECT explode(from_json(
                   details:flow_progress:data_quality:expectations,
                   'array<struct<name:string,dataset:string,passed_records:int,failed_records:int>>'
               )) AS ex
        FROM {EVENT_LOG_TABLE} e, latest_update
        WHERE e.event_type = 'flow_progress'
          AND e.origin.update_id = latest_update.id
    )
    GROUP BY 1, 2 ORDER BY dataset, expectation
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Break it on purpose: the drop demo and the fail demo
# MAGIC
# MAGIC Two poison files, two different failure philosophies:
# MAGIC
# MAGIC | | poison A (absurd salary) | poison B (missing id) |
# MAGIC |---|---|---|
# MAGIC | violates | `annual_in_bounds` on `salary_facts` | `source_identity_present` on `postings_typed` |
# MAGIC | expectation type | **expect_or_drop** | **expect_or_fail** |
# MAGIC | result | row flows to silver, salary dropped + quarantined; pipeline SUCCEEDS | update FAILS, transaction rolled back, nothing written |
# MAGIC | philosophy | per-row garbage is survivable — quarantine and count it | a broken *contract* poisons everything — stop the world |
# MAGIC
# MAGIC **Procedure:** flip ONE flag below → run the cell → run notebook 02
# MAGIC (bronze ingests the poison) → start the pipeline → observe (drop: see
# MAGIC the quarantine + metrics above; fail: the update errors on
# MAGIC `postings_typed`, screenshot it) → then follow the recovery cell.

# COMMAND ----------

import json
from datetime import datetime, timezone

WRITE_DROP_POISON = False   # poison A -> salary_facts drops it, pipeline green
WRITE_FAIL_POISON = False   # poison B -> postings_typed FAILS the update

now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
poison_dir = os.path.join(LANDING_ROOT, "career.computer")

if WRITE_DROP_POISON:
    row = {"board": "career.computer", "schema_gen": "gen_b",
           "exported_at": now_iso, "Id": 99999901,
           "Title": "DEMO poison A — absurd salary", "CompanyName": "Demo Co",
           "JobType": "Full-time", "Location": "Berlin, DE",
           "SalaryMin": 2, "SalaryMax": 3, "SalaryCurrency": "EUR",
           "SalaryPeriod": "yearly", "Status": "Active",
           "CreatedAt": now_iso}
    path = os.path.join(poison_dir, "poison_a_drop_demo.ndjson")
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")
    print(f"wrote {path} — now run notebook 02, then start the pipeline")

if WRITE_FAIL_POISON:
    row = {"board": "career.computer", "schema_gen": "gen_b",
           "exported_at": now_iso,  # note: NO Id field
           "Title": "DEMO poison B — missing source id", "CompanyName": "Demo Co",
           "JobType": "Full-time", "Location": "Berlin, DE",
           "Status": "Active", "CreatedAt": now_iso}
    path = os.path.join(poison_dir, "poison_b_fail_demo.ndjson")
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")
    print(f"wrote {path} — now run notebook 02, then start the pipeline "
          f"and watch the update FAIL on postings_typed")

if not (WRITE_DROP_POISON or WRITE_FAIL_POISON):
    print("Both flags False — nothing written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recovery from the FAIL demo — and why it's a lesson, not a chore
# MAGIC
# MAGIC The failed update rolled back: silver is intact, but the poison row is
# MAGIC now permanently in bronze (bronze is append-only — "we received
# MAGIC garbage" is a fact it faithfully recorded). Every new pipeline update
# MAGIC will re-hit the fail expectation. Your options are exactly the
# MAGIC production options:
# MAGIC
# MAGIC 1. **Fix the data at the source and re-land it** — not possible here:
# MAGIC    the file itself was the lie. So:
# MAGIC 2. **Quarantine instead of fail** — decide the contract violation is
# MAGIC    survivable after all: change the expectation to `expect_or_drop`,
# MAGIC    re-run. Honest, but it weakens the contract for all future data.
# MAGIC 3. **Purge and replay** (right answer for a learning env): delete the
# MAGIC    poison file below, reset bronze (notebook 02's RESET lever),
# MAGIC    re-run 01 → 02, then **Full refresh** the pipeline. Cheap here —
# MAGIC    that cheapness is the medallion replay story working as designed.

# COMMAND ----------

REMOVE_POISON_FILES = False

if REMOVE_POISON_FILES:
    for name in ("poison_a_drop_demo.ndjson", "poison_b_fail_demo.ndjson"):
        p = os.path.join(poison_dir, name)
        if os.path.exists(p):
            os.remove(p)
            print(f"removed {p}")
    print("Poison files gone from landing. If poison B was already ingested: "
          "notebook 02 → RESET_BRONZE=True → run reset cell → run 01 (both "
          "halves) → run 02 → pipeline Full refresh.")
else:
    print("REMOVE_POISON_FILES is False — nothing touched.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Screenshots to take (docs/screenshots/README.md)
# MAGIC * `10-pipeline-graph.png` — the DAG: bronze → typed → current → postings → skills/salary
# MAGIC * `11-expectations.png` — pipeline UI data-quality panel, or the metrics query above
# MAGIC * `12-expectation-fail.png` — the failed update from poison B
# MAGIC
# MAGIC **Next stage:** gold marts join this same pipeline — weekly volume,
# MAGIC salary bands, skill trends, duplication rate.
