# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze: incremental ingest with Auto Loader
# MAGIC
# MAGIC Ingests every NDJSON file in the landing volume that hasn't been
# MAGIC ingested before into the Delta table `jobs_bronze.postings_raw` —
# MAGIC exactly-once, with no bookkeeping code of ours. Background:
# MAGIC `LEARNING.md §3` (Auto Loader) and `§4` (streaming as incremental batch).
# MAGIC
# MAGIC **Bronze contract:** rows land exactly as exported — no casts, no
# MAGIC renames, no filters, no opinions. All seven boards go into ONE table
# MAGIC even though they have three different schemas; the union of their
# MAGIC columns is sparse and ragged, and that is fine. Bronze's product is
# MAGIC *reproducibility*: when a Silver rule changes (and one will), we replay
# MAGIC from here instead of re-exporting seven production databases.
# MAGIC The only additions are *provenance* columns (`_source_file`,
# MAGIC `_ingested_at`) — metadata about the act of ingestion, not the data.

# COMMAND ----------

import sys, os

repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.append(repo_root)

# `F` is the conventional alias for Spark's function library — the equivalent
# of System.Linq for DataFrames. F.col("x") is a column *expression* (an AST
# node, not data): Spark composes these into a query plan and only executes
# on an action — same deferred-execution model as IQueryable.
from pyspark.sql import functions as F

from src.config import (
    BRONZE_POSTINGS, BRONZE_CHECKPOINT, BRONZE_SCHEMA_LOC, LANDING_ROOT,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The read side
# MAGIC
# MAGIC `format("cloudFiles")` = Auto Loader. Each option is a decision:
# MAGIC
# MAGIC | option | value | why |
# MAGIC |---|---|---|
# MAGIC | `cloudFiles.format` | `json` | NDJSON: the JSON reader is line-per-record by default (`multiLine` off). |
# MAGIC | `cloudFiles.schemaLocation` | in the `ops` volume | where the inferred schema history lives; enables controlled evolution. |
# MAGIC | `cloudFiles.inferColumnTypes` | `false` | **everything lands as STRING.** Typing is an *interpretation* and interpretations belong to Silver, where they're versioned and replayable. A mistyped bronze column is forever; a mistyped silver cast is a re-run. |
# MAGIC | `cloudFiles.schemaEvolutionMode` | `addNewColumns` | a file with an unseen column **fails the run on purpose**, records the widened schema, and the next run proceeds. New fields surface loudly instead of vanishing silently. |
# MAGIC | `pathGlobFilter` | `*.ndjson` | only ingest export files, whatever else strays into the volume. |
# MAGIC
# MAGIC `_rescued_data` (automatic, last column): any value that doesn't fit the
# MAGIC expected schema for its row is preserved there as JSON instead of being
# MAGIC dropped. Bronze's "nothing is lost" promise is literally this column.
# MAGIC
# MAGIC `_metadata` is a hidden pseudo-column (like a system column) available on
# MAGIC any file-based read — it must be selected explicitly to materialise.
# MAGIC That's where per-row provenance comes from.

# COMMAND ----------

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", BRONZE_SCHEMA_LOC)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("pathGlobFilter", "*.ndjson")
    .load(LANDING_ROOT)
    # select("*", ...) = keep every data column, append provenance columns.
    # The underscore prefix is a convention: "added by the pipeline, not the
    # source" — same idea as audit columns on an ETL staging table.
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified"),
        F.current_timestamp().alias("_ingested_at"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The write side
# MAGIC
# MAGIC `trigger(availableNow=True)` — process everything new, then STOP. The
# MAGIC streaming API used as an incremental batch job. Not a style choice on
# MAGIC Free Edition: serverless compute rejects always-on triggers
# MAGIC (`ProcessingTime`, the default, throws
# MAGIC `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`) — and a job-board pipeline
# MAGIC has no business paying for a 24/7 stream anyway.
# MAGIC
# MAGIC `checkpointLocation` — the stream's durable memory: which files are
# MAGIC done, transactionally paired with the data commits. This is what makes
# MAGIC re-running safe. **The checkpoint and the target table are a unit** —
# MAGIC see the reset cell at the bottom before ever deleting either.

# COMMAND ----------

query = (
    raw.writeStream
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(BRONZE_POSTINGS)   # creates the UC managed table on first run
)
# A streaming query runs asynchronously; block until this drain completes.
query.awaitTermination()

ingested = sum(p["numInputRows"] for p in query.recentProgress)
print(f"Rows ingested this run: {ingested:,}")
print("(Re-run this cell right now — it will ingest 0. That's the checkpoint working.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: one table, three source schemas, visibly ragged
# MAGIC Every board landed in the same table; columns another generation doesn't
# MAGIC have are simply NULL for its rows. `schema_gen` (stamped by the export)
# MAGIC makes the raggedness queryable.

# COMMAND ----------

display(spark.sql(f"""
    SELECT board, schema_gen,
           COUNT(*)                                   AS rows,
           COUNT(EmploymentType)                      AS has_int_enum,   -- gen A only
           COUNT(JobType)                             AS has_str_enum,   -- gen B/C
           COUNT(City)                                AS has_parsed_geo, -- gen A/B only
           COUNT(SalaryMin)                           AS has_salary,
           COUNT(_rescued_data)                       AS rescued
    FROM {BRONZE_POSTINGS}
    GROUP BY board, schema_gen
    ORDER BY rows DESC
"""))

# COMMAND ----------

# The mess this project exists for, in one query: same concept ("full-time
# employment"), three encodings — an int ordinal on coffee, a free string with
# 120+ variants on computer, sparser strings on the clones. Silver conforms it.
display(spark.sql(f"""
    SELECT schema_gen,
           COALESCE(JobType, CAST(EmploymentType AS STRING)) AS employment_raw,
           COUNT(*) AS n
    FROM {BRONZE_POSTINGS}
    GROUP BY 1, 2
    ORDER BY n DESC
    LIMIT 25
"""))

# COMMAND ----------

# Provenance: which file did each row come from, and when?
display(spark.sql(f"""
    SELECT regexp_extract(_source_file, '[^/]+/[^/]+$', 0) AS file,
           COUNT(*) AS rows,
           MIN(_ingested_at) AS ingested_at
    FROM {BRONZE_POSTINGS}
    GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the documentation
# MAGIC Table + column comments land in Unity Catalog, where Catalog Explorer,
# MAGIC `DESCRIBE`, and (honestly) future-you will read them. Comments are
# MAGIC applied only to columns that exist, so this cell stays valid as schema
# MAGIC evolution adds columns.

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {BRONZE_POSTINGS} IS
    'Raw job postings from the seven career.* boards, exactly as exported (all values kept as strings; typing happens in silver). One row per posting per export file. Three source schema generations share this table — gen_a (coffee: int enums, parsed geo), gen_b (computer: string enums, parsed geo, tags), gen_c (five clone boards: string enums, free-text location only). Append-only; provenance in _source_file/_ingested_at; schema mismatches preserved in _rescued_data.'
""")

comments = {
    "board": "Which career.* board exported this row (stamped by the export tool, present on every row).",
    "schema_gen": "Source schema generation: gen_a (coffee original), gen_b (computer), gen_c (clone boards). Drives silver conformance mapping.",
    "exported_at": "UTC timestamp of the database export that produced this row (extraction metadata).",
    "Id": "Posting id in the SOURCE board database. Only unique per board — cross-board identity is silver''s dedupe problem.",
    "Title": "Raw posting title as scraped. Mixed languages; may embed seniority; line separators scrubbed at export.",
    "CompanyName": "Company display name from the source board''s Companies table.",
    "CompanyType": "gen_a only: company classification as an int ordinal (decode map in src/coffee_enums.py).",
    "Category": "gen_a: int JobCategory ordinal. gen_b: free-text category string. Absent in gen_c.",
    "RoleType": "gen_a only: int CoffeeRoleType ordinal (21 values; decode map in src/coffee_enums.py).",
    "EmploymentType": "gen_a only: int EmploymentType ordinal (decode map in src/coffee_enums.py).",
    "JobType": "gen_b/gen_c only: employment type as RAW scraped text — 120+ variants incl. CDI, Ausbildung, 正社員, Full-time/FullTime/Full Time. Conformed in silver.",
    "ExperienceLevel": "gen_b/gen_c only: raw seniority text (70+ variants, mostly null). Conformed in silver.",
    "Location": "Free-text location as scraped. For gen_c boards this is the ONLY location signal — silver parses it.",
    "City": "gen_a/gen_b only: source-parsed city. Not trusted blindly — silver reconciles with Location.",
    "State": "gen_a/gen_b only: source-parsed state/region.",
    "Country": "gen_a/gen_b only: source-parsed country (mixed codes and names).",
    "IsRemote": "Source remote flag (JSON boolean, stored as string in bronze).",
    "IsHybrid": "gen_a only: source hybrid flag.",
    "RemoteType": "gen_b only: remote sub-type (mostly null; ''FullyRemote'' when set).",
    "SalaryMin": "Lower salary bound as published. Present on ~9 percent of rows — salary coverage is itself a data-quality metric.",
    "SalaryMax": "Upper salary bound as published.",
    "SalaryCurrency": "ISO currency code — CAUTION: gen_a defaults this to USD even without amounts, so it lies on ~12k rows. Silver''s trust rules handle it.",
    "SalaryPeriod": "Pay period (yearly/monthly/weekly/daily/hourly). Nullable on gen_b/c even when amounts exist — silver infers with an explicit confidence flag.",
    "RequiredSkills": "gen_a only: JSON array serialised INTO A STRING by the source ORM (e.g. ''[\"latte art\"]''). Silver double-parses.",
    "RequiredCertifications": "gen_a only: JSON-in-string array of certification enum ordinals.",
    "MinYearsExperience": "gen_a only: minimum years of experience when stated.",
    "Tags": "gen_b/gen_c: skill/topic tags from the board''s tag tables, folded into a JSON array at export. Rich on computer/solar, empty elsewhere.",
    "SourceUrl": "Public URL of the original posting at the source ATS. Strongest cross-board duplicate signal (303 exact computer<->solar matches).",
    "SourceHash": "gen_b/gen_c: source-system dedupe hash of the scraped posting.",
    "Status": "Posting status — int ordinal on gen_a (1=Active,4=Expired,5=Closed), text on gen_b/c (Active/Closed). Conformed in silver.",
    "CreatedAt": "When the source board first stored the posting.",
    "UpdatedAt": "gen_a/gen_b: last source-side update.",
    "PostedAt": "gen_b/gen_c: posting date as scraped (midnight-truncated dates are a known artifact).",
    "LastSeenAt": "gen_a only: last time the scraper re-confirmed the posting live.",
    "ExpiresAt": "Source-side expiry timestamp.",
    "Vertical": "gen_a only: platform vertical tag (always ''coffee'' today).",
    "_rescued_data": "Auto Loader''s escape hatch: values that didn''t fit the expected schema, preserved as JSON. NULL means the row fit cleanly.",
    "_source_file": "Full landing-volume path of the file this row was ingested from (from the _metadata pseudo-column).",
    "_source_file_modified": "Modification time of that file.",
    "_ingested_at": "When Auto Loader committed this row to bronze.",
}

existing = {f.name for f in spark.table(BRONZE_POSTINGS).schema.fields}
for col, txt in comments.items():
    if col in existing:
        spark.sql(f"ALTER TABLE {BRONZE_POSTINGS} ALTER COLUMN `{col}` COMMENT '{txt}'")
print(f"Commented {len(existing & set(comments))} of {len(existing)} columns.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Proof of exactly-once, via the transaction log
# MAGIC `DESCRIBE HISTORY` reads the Delta log (`LEARNING.md §1`): one
# MAGIC `STREAMING UPDATE` commit per drain that had data. Re-run the write cell
# MAGIC above: no new files → no new commit, row count unchanged. Then load more
# MAGIC files (notebook 01, `subset=all`) and re-run: exactly one new commit,
# MAGIC containing only the new files' rows.

# COMMAND ----------

display(spark.sql(f"""
    SELECT version, timestamp, operation,
           operationMetrics.numOutputRows AS rows_written
    FROM (DESCRIBE HISTORY {BRONZE_POSTINGS})
    ORDER BY version DESC
"""))

# COMMAND ----------

# Time travel taste: the table as of its first commit. Free until VACUUM
# physically removes old files.
display(spark.sql(
    f"SELECT COUNT(*) AS rows_at_version_0 FROM {BRONZE_POSTINGS} VERSION AS OF 0"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gotchas worth the scar tissue
# MAGIC
# MAGIC 1. **Checkpoint ↔ table are a pair.** Drop the table but keep the
# MAGIC    checkpoint → the next run recreates an EMPTY table and ingests
# MAGIC    nothing ("those files are done"). Delete both or neither — that's
# MAGIC    what the reset cell below does.
# MAGIC 2. **Schema evolution fails first, succeeds second.** With
# MAGIC    `addNewColumns`, the first file carrying a new column stops the run
# MAGIC    with `UnknownFieldException` — after recording the widened schema.
# MAGIC    The re-run succeeds. In a scheduled job you'd let retries absorb
# MAGIC    this; interactively, just run the write cell again. Loud > silent.
# MAGIC 3. **New data = new file names.** Auto Loader tracks paths; overwriting
# MAGIC    a known path is invisible by default (`allowOverwrites=false`).
# MAGIC 4. **Don't reshape here.** Every cast or rename you're tempted to add is
# MAGIC    Silver's job, where it's declarative, tested by expectations, and
# MAGIC    replayable.

# COMMAND ----------

# ---- FULL RESET (bronze only) -------------------------------------------
# Deletes the bronze table AND its checkpoint/schema state together, so the
# next run of this notebook re-ingests everything in landing from scratch.
# Landing files are untouched. Flip to True, run once, flip back.
RESET_BRONZE = False

if RESET_BRONZE:
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_POSTINGS}")
    dbutils.fs.rm(BRONZE_CHECKPOINT, recurse=True)
    dbutils.fs.rm(BRONZE_SCHEMA_LOC, recurse=True)
    print("Bronze table, checkpoint and schema history deleted. Re-run from the top.")
else:
    print("RESET_BRONZE is False — nothing touched.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Screenshots to take now (docs/screenshots/README.md)
# MAGIC * `01-landing-volume.png` — Catalog Explorer: landing volume with per-board files
# MAGIC * `02-bronze-run.png` — the write cell output with the ingested row count
# MAGIC * `03-bronze-table.png` — `postings_raw` in Catalog Explorer, comments visible
# MAGIC * `04-bronze-rerun-noop.png` — the history cell after a no-op re-run
# MAGIC
# MAGIC **Next stage (Silver):** a Lakeflow Declarative Pipeline reads this table
# MAGIC as a stream and turns "raw and ragged" into "typed and true" — with
# MAGIC expectations enforcing the contract.
