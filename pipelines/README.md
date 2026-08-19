# The Lakeflow Declarative Pipeline

One pipeline carries silver (stage 2) and, from stage 3, gold — Free Edition
allows one active pipeline per type, and one DAG is the honest design
anyway. Source files here; shared logic in `src/` (the Git-folder root is
importable from pipeline code).

## Create it (UI, Free Edition — one time)

Prerequisites: notebooks 00–02 have run (schemas exist; bronze has data).

1. Left nav → **Jobs & Pipelines** → **Create** → **ETL pipeline**.
2. Name: `jobs-lakehouse`.
3. **Source code**: pick this repo's Git folder → `pipelines/` folder
   (or just `pipelines/silver.py`).
4. **Default catalog**: `workspace`; **default schema**: `jobs_silver`.
   Make sure the setting is the *schema* field (default publishing mode) —
   the code uses fully qualified names and publishes across schemas.
5. Compute: serverless (the only option on Free Edition). Leave
   **Development** mode on while iterating.
6. **Advanced settings → JSON**: add the `event_log` block from
   `pipeline-settings.json` — it publishes the event log as
   `workspace.jobs_silver._pipeline_event_log`, which notebook 03 queries.
7. **Start**. First update processes all of bronze; later updates are
   incremental (the typed table streams from bronze, the CDC step upserts,
   materialized views recompute).

Then run `notebooks/03_silver_expectations_lab` for verification queries,
expectation metrics, and the drop/fail demos.

## What's in the DAG (stage 2)

| table (workspace.jobs_silver.*) | kind | role | expectations |
|---|---|---|---|
| `ref_fx_rates` | MV | pinned FX dimension | **fail**: positive rate, ISO shape |
| `postings_typed` | streaming | typing + conformance + trust rules | **fail**: source identity contract; warn: title/company present |
| `postings_current` | streaming (auto CDC) | latest-wins per (board, source_id) | warn: key present |
| `postings` | MV | duplicate groups, canonical flags — the consumer table | warn: date sanity, country coverage, conformance coverage |
| `posting_skills` | MV | 3 skill encodings exploded | **drop**: empty skill, null key; warn: length |
| `salary_facts` | MV | usable salaries → annual USD | **drop**: FX referential, period known, plausibility bounds |
| `salary_quarantine` | MV | what salary_facts rejected, with reasons | warn: reason present |

## Iterating

Edit `pipelines/silver.py` (or `src/conform.py`), pull the Git folder,
click **Start** again. Development mode reuses warm compute and relaxes
retries. Materialized views fully recompute; the streaming tables only
process new bronze rows — to reprocess everything after a logic change in
`postings_typed`, use **Full refresh** (⚠ full refresh also replays the
CDC step; bronze is the replay source, nothing is lost).
