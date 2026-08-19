# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Load export files into the landing volume
# MAGIC
# MAGIC Copies the committed sample exports (`data/samples/*.ndjson`, ~3.2k real
# MAGIC postings) from this Git folder into the `landing` volume, one
# MAGIC subdirectory per board — simulating what the boards' export job would
# MAGIC drop there on a schedule.
# MAGIC
# MAGIC Free Edition restricts outbound network access, so the workspace can't
# MAGIC pull from the boards' APIs/databases itself; extraction runs outside
# MAGIC (`tools/export_postings.py`, data-owner only) and files arrive here.
# MAGIC That split is normal architecture, not a workaround: landing zones
# MAGIC decouple *producing* data from *processing* it.
# MAGIC
# MAGIC **The `subset` widget (top of the notebook) is the incremental-ingest
# MAGIC demo.** Leave it on `first-half`, run this + notebook 02, then set it to
# MAGIC `all` and run both again — Auto Loader will ingest only the three new
# MAGIC files. No flag, no high-water-mark column, no bookkeeping of yours.
# MAGIC
# MAGIC **Full dataset instead of samples?** Run the export on your machine and
# MAGIC upload `data/exports/<board>/*.ndjson` into the same
# MAGIC `landing/<board>/` folders (Catalog → volume → Upload). Same pipeline,
# MAGIC ~109k rows instead of ~3.2k.

# COMMAND ----------

# Widgets are Databricks' notebook parameters (think SSRS report parameters);
# they also become job parameters when a notebook runs as a job task.
dbutils.widgets.dropdown("subset", "first-half", ["first-half", "all"],
                         "Which boards to load")

# COMMAND ----------

import os
import shutil
import sys

repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import BOARDS, LANDING_ROOT

FIRST_HALF = BOARDS[:4]  # coffee, computer, solar, pet
selected = BOARDS if dbutils.widgets.get("subset") == "all" else FIRST_HALF
print(f"Loading {len(selected)} board(s): {', '.join(selected)}")

# COMMAND ----------

# A UC volume is FUSE-mounted: /Volumes/... behaves like a local directory,
# so plain shutil works — but reads/writes go through Unity Catalog grants.
# Copies are skipped if the target exists: Auto Loader tracks files BY PATH,
# so re-copying the same path would do nothing anyway (and with the default
# cloudFiles.allowOverwrites=false, even a changed file at a known path is
# ignored). New data must arrive as NEW file names — that's the contract.
samples_dir = os.path.join(repo_root, "data", "samples")

for board in selected:
    src = os.path.join(samples_dir, f"{board}.ndjson")
    dst_dir = os.path.join(LANDING_ROOT, board)
    dst = os.path.join(dst_dir, f"sample_{board}.ndjson")
    os.makedirs(dst_dir, exist_ok=True)
    if os.path.exists(dst):
        print(f"  skip (already landed): {board}")
    else:
        shutil.copy(src, dst)
        print(f"  landed: {dst}")

# COMMAND ----------

# What's in the landing zone now? dbutils.fs is the other door to the same
# volume (Spark-native path scheme). Both views are governed by UC.
total = 0
for board in BOARDS:
    path = f"{LANDING_ROOT}/{board}"
    try:
        files = dbutils.fs.ls(path)
    except Exception:
        continue
    for f in files:
        total += 1
        print(f"{f.path.split('/Volumes/')[-1]:70s} {f.size:>10,} bytes")
print(f"\n{total} file(s) in landing.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** `02_bronze_autoloader` ingests whatever is in `landing/` that
# MAGIC it hasn't seen before.
