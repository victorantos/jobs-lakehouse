"""Single source of truth for names, paths and constants used across notebooks
and (later) the Lakeflow pipeline.

Why a plain module and not widgets/JSON/env vars: notebooks in a Databricks Git
folder can import repo modules directly (the repo root is on sys.path), so a
.py constants module gives you IDE support, one place to change names, and a
diff-able history — the same reasons you'd centralise config in a .NET project
instead of scattering appsettings keys.
"""

# ---------------------------------------------------------------------------
# Unity Catalog object names
#
# Free Edition provisions one workspace with a default catalog named
# "workspace". Creating additional catalogs is possible but pointless at this
# scale — catalog = environment/team boundary, schema = subject-area boundary.
# We use one schema per medallion layer, which keeps GRANTs, discovery and
# cleanup per-layer trivial (think: one SQL Server schema per zone, not one
# database per zone).
# ---------------------------------------------------------------------------
CATALOG = "workspace"           # Free Edition default; change if yours differs

SCHEMA_BRONZE = "jobs_bronze"
SCHEMA_SILVER = "jobs_silver"
SCHEMA_GOLD = "jobs_gold"

# Volumes live inside a schema. Two volumes with distinct roles:
#   landing — files the pipeline CONSUMES (the scraper exports land here)
#   ops     — files the engine PRODUCES (stream checkpoints, schema history,
#             export manifests). Kept separate so nobody ever points Auto
#             Loader at a directory that contains its own bookkeeping files.
VOLUME_LANDING = "landing"
VOLUME_OPS = "ops"

# Fully-qualified table names (three-level namespace: catalog.schema.table)
BRONZE_POSTINGS = f"{CATALOG}.{SCHEMA_BRONZE}.postings_raw"

# ---------------------------------------------------------------------------
# Paths. UC volumes are FUSE-mounted: to any Python/Spark code they are just
# directories under /Volumes/<catalog>/<schema>/<volume>/ — but access is
# governed by Unity Catalog grants, unlike a raw cloud bucket.
# ---------------------------------------------------------------------------
LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/{VOLUME_LANDING}"
OPS_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/{VOLUME_OPS}"

# Auto Loader needs two pieces of durable state, both under ops/:
#   checkpoint — which files have already been ingested (exactly-once)
#   schema     — the inferred schema history (enables controlled evolution)
BRONZE_CHECKPOINT = f"{OPS_ROOT}/checkpoints/bronze_postings"
BRONZE_SCHEMA_LOC = f"{OPS_ROOT}/schemas/bronze_postings"

# ---------------------------------------------------------------------------
# The seven boards in scope. Each is a separate production system with its own
# database — the point of this project is integrating them despite that.
# (career.energy / .repair / .estate exist too; adding one = adding a line
# here and a line in tools/export_postings.py.)
# ---------------------------------------------------------------------------
BOARDS = [
    "career.coffee",
    "career.computer",
    "career.solar",
    "career.pet",
    "career.delivery",
    "career.church",
    "career.dental",
]

# ---------------------------------------------------------------------------
# Salary normalisation targets (used by Silver, stage 2).
#
# Target: annual USD. Rates are PINNED, not live: the exercise must be
# deterministic and re-runnable, and for trend analysis a stable conversion
# basis is more honest than mixing each week's spot rate into the salary
# signal. Production design would join a daily rates dimension instead.
# Rates ≈ mid-2026 interbank, 2 dp is plenty for salary bands.
# ---------------------------------------------------------------------------
TARGET_CURRENCY = "USD"
FX_TO_USD = {
    "USD": 1.00,
    "EUR": 1.10,
    "GBP": 1.28,
    "CHF": 1.25,
    "CAD": 0.74,
    "AUD": 0.67,
    "PLN": 0.26,
    "SEK": 0.10,
    "DKK": 0.15,
    "NOK": 0.10,
}

# Period → annual multipliers. Assumptions documented on the Silver table
# comment as well: 40h weeks, 52 paid weeks, 260 weekdays. Salary strings
# like "yearly"/"hourly" come from the boards' SalaryPeriod column.
PERIOD_TO_ANNUAL = {
    "yearly": 1.0,
    "monthly": 12.0,
    "weekly": 52.0,
    "daily": 260.0,
    "hourly": 2080.0,  # 40 h/week × 52
}
