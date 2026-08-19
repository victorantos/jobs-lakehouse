# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Unity Catalog setup
# MAGIC
# MAGIC Creates every governed object the pipeline needs: three schemas (one per
# MAGIC medallion layer) and two volumes (landing + ops). Everything gets a
# MAGIC `COMMENT` — in Unity Catalog, comments are queryable metadata
# MAGIC (`information_schema`), which is what turns Catalog Explorer into a data
# MAGIC dictionary a stranger can navigate. See `LEARNING.md §5` for what UC is
# MAGIC actually for.
# MAGIC
# MAGIC **Idempotent** — safe to re-run; everything is `IF NOT EXISTS`.
# MAGIC
# MAGIC Object model refresher (SQL Server analogy in brackets):
# MAGIC ```
# MAGIC metastore                 [the instance]
# MAGIC └── catalog: workspace    [a database — Free Edition provisions this one]
# MAGIC     ├── schema: jobs_bronze   [a schema]
# MAGIC     │   ├── volume: landing   [a governed file share — files IN]
# MAGIC     │   ├── volume: ops       [engine state — checkpoints OUT]
# MAGIC     │   └── table: postings_raw
# MAGIC     ├── schema: jobs_silver
# MAGIC     └── schema: jobs_gold
# MAGIC ```

# COMMAND ----------

# Notebooks in a Databricks Git folder get the repo root on sys.path, so repo
# modules import directly. The append is defensive (harmless if already there).
import sys, os

repo_root = os.path.dirname(os.getcwd())  # notebooks/ -> repo root
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import (
    CATALOG, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD,
    VOLUME_LANDING, VOLUME_OPS,
)

print(f"Target catalog: {CATALOG}")
# If this fails with CATALOG_NOT_FOUND, your workspace's default catalog has a
# different name — run `SHOW CATALOGS` and edit src/config.py accordingly.
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas — one per layer
# MAGIC Schema-per-layer makes the layer boundary a *governance* boundary: in a
# MAGIC team setup you'd GRANT analysts SELECT on `jobs_gold` only, and nobody
# MAGIC accidentally dashboards off bronze. Solo on Free Edition the GRANTs are
# MAGIC idle, but the discoverability and blast-radius benefits stand.

# COMMAND ----------

# f-strings into DDL are fine here: every value is a constant from
# src/config.py, not user input. (You'd parameterise with IDENTIFIER(:name)
# if names ever came from outside.)
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}
    COMMENT 'Bronze layer: job postings exactly as exported from the seven career.* boards, plus ingestion metadata. Append-only, schema-on-read, never edited. Replay source for everything downstream.'
""")
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}
    COMMENT 'Silver layer: typed, deduplicated, conformed postings. One row = one real-world posting; salaries normalised to annual USD; locations parsed; skills exploded. Built by the Lakeflow pipeline.'
""")
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}
    COMMENT 'Gold layer: aggregates for the dashboard — weekly posting volume, salary bands, skill trends, cross-board duplication. Built by the Lakeflow pipeline.'
""")
print("Schemas created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumes — governed file storage
# MAGIC Two volumes with strictly separated roles:
# MAGIC
# MAGIC | volume | direction | contents |
# MAGIC |---|---|---|
# MAGIC | `landing` | consumed by the pipeline | per-board NDJSON export files |
# MAGIC | `ops` | produced by the engine | Auto Loader checkpoints + schema history |
# MAGIC
# MAGIC Separate on purpose: pointing Auto Loader at a directory that contains its
# MAGIC own bookkeeping files is a classic self-inflicted wound. A volume is
# MAGIC FUSE-mounted at `/Volumes/<catalog>/<schema>/<name>/` — plain `open()`
# MAGIC works — but every access goes through UC permissions, unlike a raw bucket.

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.{VOLUME_LANDING}
    COMMENT 'Landing zone for NDJSON export files from the seven career.* job boards. One subdirectory per board. Auto Loader consumes this path; files are never modified after arrival.'
""")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.{VOLUME_OPS}
    COMMENT 'Engine state, not data: Auto Loader stream checkpoints and inferred-schema history. Deleting a checkpoint resets that stream''s notion of which files were already ingested — see notebook 02 before touching.'
""")
print("Volumes created.")

# COMMAND ----------

# Verify — and meet information_schema, which is where COMMENTs live as data.
display(spark.sql(f"""
    SELECT schema_name, comment
    FROM {CATALOG}.information_schema.schemata
    WHERE schema_name LIKE 'jobs\\_%'
    ORDER BY schema_name
"""))

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA_BRONZE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done — take the tour
# MAGIC Open **Catalog** in the left nav → `workspace` → `jobs_bronze`. The
# MAGIC schema and volume comments you just wrote are rendered there. That page
# MAGIC is what "data catalog" means in practice: names, comments, lineage and
# MAGIC permissions in the place people actually look.
# MAGIC
# MAGIC **Next:** `01_load_landing` puts data into the landing volume.
