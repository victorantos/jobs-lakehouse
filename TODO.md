# TODO — remaining work

Handoff brief: written so a fresh Claude Code session (or future me) can pick
any item up without re-deriving context. Read first: `README.md`
(architecture + measured data profile), `LEARNING.md` (concept notes),
`pipelines/README.md` (pipeline creation/iteration). Stages 1–2 are built,
smoke-tested and pushed; stages 3–5 are designed but not built. Work one
stage at a time and stop between stages — that's the standing agreement.

## Working conventions (apply to every item)

- **Verify Databricks APIs against current docs before writing pipeline
  code** — the LDP surface is mid-migration. Known-good as of 2026-08:
  `from pyspark import pipelines as dp`, `@dp.table` /
  `@dp.materialized_view`, `dp.create_auto_cdc_flow`, expectations
  `@dp.expect[_all][_or_drop|_or_fail]`, multi-schema publishing via fully
  qualified `name=` (pipeline settings use `schema`, not legacy `target`).
- **Logic goes in `src/` as pure DataFrame functions; pipeline files stay
  declarative.** Extend `tools/smoke_test_silver_logic.py` (or a gold
  sibling) and run it green BEFORE committing — it needs pyspark + Java 17
  locally (`python3 -m venv .venv && pip install pyspark`). It has already
  caught real bugs (ANSI `cast` vs `try_cast`).
- Serverless-only constraints hold: `Trigger.AvailableNow` for streams, one
  active pipeline (gold joins the SAME pipeline), ≤5 concurrent job tasks,
  2X-Small warehouse, small data.
- Every new table: UC comment + column comments (schema DDL in the
  decorator) + at least one expectation. Update README (status tables, repo
  map) and LEARNING.md stubs in the same commit.
- Commits: no Co-Authored-By trailer in this repo. Push triggers nothing
  (no CI here yet).
- Full-data exports need the SQL Server credential dance (session-scoped
  wrapper + stash; see project memory / stage-1 chat) — the committed
  samples are enough for all pipeline development.

## Workspace verification (user, ~30 min — stages 1–2 are code-complete but
not yet proven in a workspace)

- [ ] Pull the Git folder; run notebooks `00` → `01` (subset=first-half) →
      `02`; re-run 02 (expect 0 rows); re-run 01 (subset=all) + 02 (expect
      only new files). Screenshots `01`–`04` per `docs/screenshots/README.md`.
- [ ] Create the pipeline per `pipelines/README.md` (catalog `workspace`,
      schema `jobs_silver`, serverless, event_log block from
      `pipeline-settings.json`). Start it; all 7 silver tables green.
- [ ] Run `notebooks/03_silver_expectations_lab`: verify ~50 cross-board dup
      groups, ~20 salary quarantine rows, expectation metrics query works.
      Run the poison A (drop) and poison B (fail) demos + recovery.
      Screenshots `10`–`12`.
- [ ] Any workspace-vs-docs mismatch (API drift, settings UI changes):
      fix repo docs/code, note it in LEARNING.md if conceptually interesting.

## Stage 3 — Gold marts (next build stage)

Add gold to the SAME pipeline: new file `pipelines/gold.py` (the pipeline's
source glob already includes `pipelines/**`), publishing to
`workspace.jobs_gold` via fully qualified names. Add table-name constants to
`src/config.py`. All marts are materialized views over silver; put any
nontrivial logic in `src/gold_transforms.py` + extend the smoke test.

- [ ] `jobs_gold.weekly_postings` — postings per board per ISO week:
      `date_trunc('week', posted_at)`, filter `is_canonical`, columns:
      week, board, new_postings, distinct_companies, remote_share,
      salary_coverage_pct (postings with usable salary / total — the
      coverage-as-KPI story). Expectations: week not null + not future
      (drop), counts >= 0 (fail is overkill here — warn).
      Coffee history reaches 2024-02, computer 2024-07 — trends are real.
- [ ] `jobs_gold.salary_bands` — by (board, country, seniority,
      employment_type): from `salary_facts` filtered `is_canonical`;
      p25/p50/p75 of `salary_mid_annual_usd` (use `percentile_approx`),
      n_postings, plus a `small_sample` flag for n < 5 instead of hiding
      rows (honesty over polish). Expectation: p25 <= p50 <= p75 (warn),
      n > 0 (drop).
- [ ] `jobs_gold.skill_trends` — by (skill, board, week): posting counts +
      share of that board-week's postings; keep only skills with some
      minimum total (e.g. >= 10 postings overall) to kill one-off tags —
      log the threshold in the table comment. Expectation: share between
      0 and 1 (fail — a broken ratio means broken logic).
- [ ] `jobs_gold.duplication_rate` — by board pair and week: cross-board
      dup groups, redundant postings, rate = redundant / total; split by
      matched_by (source_url vs fingerprint). This is the mart that turns
      the dedupe work into a headline number.
- [ ] Extend smoke test with gold checks (band ordering, shares in [0,1],
      weekly sums reconcile to silver counts).
- [ ] README: status flips, gold section in the layer table, mess-catalog
      column pointers. Screenshots `20`–`21` (full DAG + UC lineage).

## Stage 4 — Databricks Job

- [ ] Job `jobs-lakehouse-daily`, two sequential tasks: (1) notebook task
      `02_bronze_autoloader`, (2) pipeline task (update the pipeline).
      Serverless. Well under the 5-task cap.
- [ ] Schedule: daily, pick a time after the boards' export would land
      (owner runs exports manually today, so a paused schedule +
      run-now is fine; document both).
- [ ] Commit the job config as `jobs/job-settings.json` (UI-first creation
      guide like pipelines/README.md; JSON for reference/CLI).
- [ ] Fill LEARNING.md §8 (jobs: tasks/DAG, why notebook+pipeline in one
      job, what the concurrency cap forces). Screenshot `30`.

## Stage 5 — Dashboard + final polish

- [ ] One DBSQL dashboard over `jobs_gold` on the 2X-Small warehouse
      (auto-stop on): weekly volume by board, salary bands (filter
      small_sample out in the viz, say so on the tile), top skills trend,
      duplication rate, salary coverage. Export dashboard JSON to
      `dashboards/` with a README (Free Edition can't keep it live —
      the export + screenshots ARE the artifact).
- [ ] Fill LEARNING.md §9 (warehouse: T-shirt compute, Photon, result
      cache, why it's separate from notebook compute).
- [ ] Capture remaining screenshots (`40`), embed the best ones in
      README (currently links only), final README/LEARNING proofread.
- [ ] Optional but high-value: GitHub Action running
      `tools/smoke_test_silver_logic.py` on PRs (pyspark + temurin-17 on
      ubuntu-latest works) — turns the local test into CI.

## Backlog (no stage, grab when useful)

- [ ] Incremental export path: document/run `--since` exports so Auto
      Loader's incrementality shows on real deltas (today: one full export).
- [ ] Add the other three live boards (career.energy / .repair / .estate) —
      one line in `src/config.py` BOARDS + one entry in
      `tools/export_postings.py` BOARDS (they're gen_c clones).
- [ ] Employment/seniority conformance tail: `other`/`unknown` shares are
      visible in notebook 03 — extend `src/conform.py` rules if the tail
      is fat; the warn-expectations will show the improvement per run.
- [ ] `salary_quarantine` review: if `unknown_currency` grows, extend the
      geo-currency map instead of the FX table (nulls, not typos, dominate).
