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

# Silver tables — all created by the Lakeflow pipeline (pipelines/silver.py).
# The pipeline's default schema is jobs_silver, but we always use fully
# qualified names anyway: with the default publishing mode a single pipeline
# can publish to any schema, which is how gold joins the same DAG in stage 3.
SILVER_POSTINGS_TYPED = f"{CATALOG}.{SCHEMA_SILVER}.postings_typed"
SILVER_POSTINGS_CURRENT = f"{CATALOG}.{SCHEMA_SILVER}.postings_current"
SILVER_POSTINGS = f"{CATALOG}.{SCHEMA_SILVER}.postings"
SILVER_POSTING_SKILLS = f"{CATALOG}.{SCHEMA_SILVER}.posting_skills"
SILVER_SALARY_FACTS = f"{CATALOG}.{SCHEMA_SILVER}.salary_facts"
SILVER_SALARY_QUARANTINE = f"{CATALOG}.{SCHEMA_SILVER}.salary_quarantine"
SILVER_REF_FX = f"{CATALOG}.{SCHEMA_SILVER}.ref_fx_rates"

# The pipeline event log, published into UC so it is queryable by name
# (see pipelines/pipeline-settings.json and notebook 03).
EVENT_LOG_TABLE = f"{CATALOG}.{SCHEMA_SILVER}._pipeline_event_log"

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
# Every currency observed in the seven boards' data (profiling 2026-08-19
# found 37 on coffee alone). A currency missing here is DELIBERATELY not
# silently defaulted: salary_facts drops such rows via a referential
# expectation against ref_fx_rates, and they surface in salary_quarantine.
FX_TO_USD = {
    "USD": 1.00,   "EUR": 1.10,    "GBP": 1.28,    "CHF": 1.25,
    "CAD": 0.74,   "AUD": 0.67,    "NZD": 0.61,    "JPY": 0.0068,
    "CNY": 0.14,   "KRW": 0.00074, "TWD": 0.033,   "HKD": 0.128,
    "SGD": 0.75,   "MYR": 0.22,    "THB": 0.028,   "VND": 0.000039,
    "PHP": 0.0177, "IDR": 0.000062,"INR": 0.0117,  "AED": 0.272,
    "ILS": 0.27,   "TRY": 0.028,   "RUB": 0.011,   "UAH": 0.024,
    "PLN": 0.26,   "CZK": 0.044,   "HUF": 0.0028,  "RON": 0.22,
    "SEK": 0.10,   "DKK": 0.148,   "NOK": 0.095,   "BRL": 0.185,
    "MXN": 0.055,  "PEN": 0.27,    "COP": 0.00024, "CLP": 0.0011,
    "ZAR": 0.055,  "BHD": 2.65,    "AMD": 0.0026,  "BYN": 0.31,
}

# Plausibility bounds for a normalised ANNUAL USD salary. Outside this range
# the row is a data error (SalaryMin=2, an hourly rate labelled yearly, ...)
# and salary_facts drops it into salary_quarantine.
SALARY_ANNUAL_USD_MIN = 1_000
SALARY_ANNUAL_USD_MAX = 5_000_000

# When SalaryPeriod is missing but amounts exist (609 rows on computer
# alone), infer the period from the magnitude of the mid value converted to
# USD. Thresholds chosen against the observed data; every inferred period is
# flagged (salary_period_confidence = 'inferred_magnitude').
PERIOD_INFER_HOURLY_BELOW_USD = 150
PERIOD_INFER_MONTHLY_BELOW_USD = 15_000

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
