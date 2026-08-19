# LEARNING.md — what this project actually teaches

Study notes, not marketing. Each section is a concept this repo exercises,
explained the way I'd want it explained coming from 20 years of SQL Server /
.NET: what the thing *is* mechanically, what problem it solves, and what the
nearest concept in the Microsoft world is (and where that analogy breaks).

Sections marked *(stage N)* get filled in when that stage is built — the code
that exercises them doesn't exist yet, and writing the notes without the
scars would be exactly the marketing-speak this file exists to avoid.

---

## 1. Delta Lake vs plain Parquet

A Delta table is a directory of ordinary Parquet files **plus an ordered
transaction log** (`_delta_log/`, JSON files numbered 000...000.json,
000...001.json, …). That log is the entire trick. Each commit file lists the
data files added and removed by one atomic operation, plus per-file column
statistics (min/max/null counts).

What the log buys you that a bare Parquet folder cannot give:

- **Atomic commits.** A reader resolves table state by reading the log, not by
  listing files. A half-finished write is invisible until its commit lands —
  the same reason SQL Server readers never see a torn page: the write-ahead
  log defines truth, the data files are just where bytes live. With plain
  Parquet, a failed job leaves orphan files that the next read happily
  includes.
- **Serialisable writes.** Concurrent writers do optimistic concurrency
  against the log (try to commit version N+1; if someone beat you, re-check
  for conflicts and retry). No locks, no lock manager — closer to
  `UPDATE ... WHERE rowversion = @expected` than to sch-M locks.
- **Time travel.** Old versions are just earlier prefixes of the log, so
  `VERSION AS OF` / `TIMESTAMP AS OF` are free reads until `VACUUM` physically
  removes unreferenced files. Think temporal tables, except you get it without
  designing for it.
- **Schema enforcement + controlled evolution.** The log stores the schema;
  writes that don't match fail instead of silently writing a divergent file.
  Evolution (`mergeSchema`, Auto Loader's `addNewColumns`) is an explicit,
  logged schema change — DDL as data.
- **File skipping.** Those per-file min/max stats work like a coarse,
  automatic version of partition elimination: a filter on `board =
  'career.pet'` skips files whose stats prove they can't match. It's not an
  index — closer to SQL Server's segment elimination on columnstore.
- **Streaming source/sink.** Because commits are ordered, a stream can tail a
  Delta table like a change feed. This is what lets stage-2's pipeline read
  bronze incrementally instead of rescanning it.

Where the analogy breaks: there is no buffer pool, no page latching, no
row-level locking. Delta gives you *table-level* transactional appends,
merges and deletes over immutable files — OLAP transactionality, not OLTP.

## 2. Why medallion (bronze / silver / gold)

Three zones, each with a contract:

- **Bronze — facts as received.** Append-only, schema-on-read, no
  interpretation. Its job is to make everything downstream *reproducible*:
  if a silver rule turns out wrong (and one will — see the salary currency
  bug this dataset actually contains), you fix the rule and replay from
  bronze. Without bronze, a bad transform means re-scraping the world.
- **Silver — facts as understood.** Typed, deduplicated, conformed across the
  seven source schemas, one row = one real-world posting. This is where
  disagreement between sources gets resolved and documented.
- **Gold — facts as consumed.** Aggregates shaped for a specific audience
  (the dashboard). Denormalised, cheap to query, rebuilt from silver at will.

The SQL Server mapping that mostly works: bronze ≈ a staging schema you never
truncate; silver ≈ the cleansed ODS; gold ≈ the data marts. The difference in
kind: staging tables in a warehouse are traditionally *transient* because
storage was expensive; bronze is *permanent* because object storage is cheap
and replayability is the whole point. The layer boundary is also a blast
radius: a broken gold aggregate can't corrupt silver; a source schema change
stops at silver's conformance step instead of rippling into the dashboard.

It's not dogma. Three layers is the number this project needs, not a law —
add a layer when a new *contract* appears, not because a diagram said so.

## 3. Auto Loader — what problem it actually solves

The naive way to ingest a folder of files is `spark.read.json(dir)` on a
schedule. It rescans and re-reads everything, every run; you get duplicates
unless you dedupe, and "which files are new" becomes your problem, solved
badly with mtime comparisons — I've written that SSIS package, everyone has.

Auto Loader (`format("cloudFiles")`) makes file-state tracking the engine's
problem: it discovers new files, records every file it has committed into a
RocksDB store inside the stream **checkpoint**, and guarantees each file is
ingested exactly once even across crashes, because the file list and the data
commit move together. It's the high-water-mark pattern, except the mark is
transactional and per-file, and you don't maintain it.

The pieces it needs:
- `cloudFiles.format` — what's inside the files (json here; the boards export
  NDJSON, which the json reader consumes line-per-record by default).
- `cloudFiles.schemaLocation` — where inferred schema history lives. First
  run samples files and writes schema v0; later runs compare.
- **Schema evolution**: with `addNewColumns` (our setting), a file containing
  an unseen column *fails the stream on purpose*, records the widened schema,
  and the next run proceeds with the new column — new fields become visible,
  loudly, instead of silently vanishing. The alternative modes (`rescue`,
  `none`) trade loudness for stability.
- `_rescued_data` — the escape hatch column. Data that doesn't fit the
  expected type/schema for a row lands there as JSON instead of being
  dropped. Bronze's promise of "nothing lost" is literally this column.

Why not `COPY INTO`? Same exactly-once idea, SQL syntax, but state lives with
the target table and it's batch-only; Auto Loader scales to file-notification
mode and composes with pipelines. At this project's size either would work —
Auto Loader is used because it's what scales past toy size and it's the one
worth learning.

## 4. Streaming as incremental batch (`Trigger.AvailableNow`)

The bronze notebook uses the *streaming* API but is not a always-on stream:

```python
.writeStream.trigger(availableNow=True)
```

means "process everything that has arrived since the last checkpoint, then
stop." It is a batch job that borrows streaming's bookkeeping. This inversion
is worth internalising: in Spark, *streaming is the general case* and batch
is a stream you choose to drain in one gulp. The checkpoint directory — file
offsets, RocksDB file registry, transactional write markers — is what turns
"run this notebook again" into "continue from where we stopped" rather than
"start over."

Two practical consequences:
- **Re-running is safe.** Second run with no new files = zero rows written,
  not a duplicate load. The demo in notebook 02 proves it.
- **Checkpoint and table are a pair.** Drop the table but keep the
  checkpoint and the stream will smugly ingest nothing (files are "already
  done"). Delete both or neither — the reset cell in notebook 02 exists
  because everyone hits this exactly once.

On Free Edition this isn't just style: serverless compute rejects continuous
triggers outright (`ProcessingTime`, the default, throws
`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`). Scheduled `availableNow` runs
are the platform-sanctioned way to be "continuous enough" — and for job
postings, minutes-fresh would be pure vanity anyway.

## 5. Unity Catalog — the part that's actually load-bearing

Unity Catalog is the answer to "where do names resolve and where do
permissions live." Every object is `catalog.schema.object`, one metastore per
region governs all workspaces, and GRANTs/ownership/lineage/comments hang off
that tree. The AD analogy: it's the directory + ACL layer for data objects,
so security stops being per-cluster configuration and becomes a property of
the object itself.

What this project uses it for concretely:
- **Three-level naming** — `workspace.jobs_bronze.postings_raw`; schemas as
  layer boundaries (see §2).
- **Volumes** — governed *file* storage. `/Volumes/workspace/jobs_bronze/landing`
  is FUSE-mounted so Python sees a directory, but reads/writes go through UC
  permissions. It replaces "random blob container with an SAS token" — files
  get the same governance surface as tables. Landing zone (files in) and ops
  zone (checkpoints out) are separate volumes on purpose.
- **Comments everywhere** — table and column comments are queryable metadata
  (`information_schema`), which is what makes Catalog Explorer a data
  dictionary someone else can navigate. In a solo Free Edition account the
  GRANT machinery is idle; comments and lineage are the parts that pay rent
  immediately.
- **Managed tables** — we never specify storage paths for tables; UC owns the
  layout. The days of "the ETL job knows the S3 prefix" are over, and that's
  a governance feature, not a convenience.

## 6. Lakeflow Declarative Pipelines vs a chain of notebooks *(stage 2)*

*To be written when the pipeline exists. The short preview, to be tested
against reality: you declare tables as functions-returning-DataFrames and the
framework owns ordering, retries, incrementality and observability — the
difference between writing MSBuild targets and writing a batch script that
calls csc.exe in the right order. Also to cover: why the whole medallion flow
sits in ONE pipeline here (Free Edition allows one active pipeline per type —
and one DAG is the honest design anyway), and what the migration from
`import dlt` to the open-sourced Spark Declarative Pipelines API means.*

## 7. Expectations — declarative data quality *(stage 2)*

*To be written alongside the first real expectation set: warn vs drop vs
fail semantics, where each belongs (fail = structural invariants that mean
the pipeline itself is broken; drop = per-row garbage you've decided is
survivable and want quarantined-by-metric; warn = things you want measured
before you decide), and how expectation metrics land in the pipeline event
log as queryable data.*

## 8. Jobs — orchestration *(stage 4)*

*To be written when the job exists: tasks/DAG, why the ingest notebook and
the pipeline update are two tasks of one job rather than two schedules, and
what the 5-concurrent-task Free Edition cap forces you to think about.*

## 9. DBSQL warehouses and dashboards *(stage 5)*

*To be written when the dashboard exists: what a SQL warehouse is (compute
sized in T-shirt units, photon, result caching), why it exists separately
from notebook compute, and what "2X-Small and auto-stop" means for cost on
a quota-capped account.*
