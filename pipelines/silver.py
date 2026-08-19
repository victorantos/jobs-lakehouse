"""Silver layer — Lakeflow Declarative Pipeline source.

One pipeline holds the whole medallion DAG from bronze onward (Free Edition
allows one active pipeline per type — and one DAG is the honest design
anyway; gold joins this same pipeline in stage 3 via fully qualified names,
which the default publishing mode resolves across schemas).

This file is deliberately THIN: it declares tables, comments, expectations
and CDC wiring. The actual logic lives in src/silver_transforms.py as pure
DataFrame functions — the same code this pipeline runs is smoke-testable on
a laptop against the committed samples (tools/smoke_test_silver_logic.py),
so tested logic and deployed logic cannot drift.

The silver flow, three explicit steps, each a real table in the DAG:

  bronze.postings_raw ──stream──▶ postings_typed        (row-local: type,
      conform, trust rules; expectations incl. the FAIL contract)
  postings_typed ──auto CDC──▶ postings_current         (latest-wins per
      (board, source_id), sequenced by exported_at — declarative MERGE)
  postings_current ──▶ postings                         (cross-row: duplicate
      groups, canonical flags — an MV because it must see ALL rows)
      ├─▶ posting_skills
      └─(+ ref_fx_rates)─▶ salary_facts / salary_quarantine

Why the split: each step is a different KIND of computation (row-local
streaming / keyed upsert / global windows), and materialising the
boundaries makes the pipeline graph show the architecture.

API note: `from pyspark import pipelines as dp` is the current module (the
`dlt` import still works but is legacy). `spark` is injected by the runtime.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from src.config import (
    BRONZE_POSTINGS,
    FX_TO_USD,
    SALARY_ANNUAL_USD_MAX,
    SALARY_ANNUAL_USD_MIN,
    SILVER_POSTINGS,
    SILVER_POSTINGS_CURRENT,
    SILVER_POSTINGS_TYPED,
    SILVER_POSTING_SKILLS,
    SILVER_REF_FX,
    SILVER_SALARY_FACTS,
    SILVER_SALARY_QUARANTINE,
)
from src.silver_transforms import (
    enrich_postings,
    salary_base_from,
    skills_from,
    typed_postings,
)

FX_AS_OF = "2026-08 (pinned — see src/config.py)"


# ==========================================================================
# Reference: FX rates as a real governed table
# ==========================================================================
# The same numbers exist as an expression map (for the period-inference
# heuristic) — this table is the AUTHORITATIVE join target, so an unknown
# currency fails a visible referential expectation instead of silently
# nulling out mid-arithmetic.
@dp.materialized_view(
    name=SILVER_REF_FX,
    comment="Pinned currency->USD conversion rates used to normalise salaries. "
            "Pinned (not live) on purpose: deterministic re-runs, and a stable "
            "conversion basis keeps FX noise out of salary trend lines. "
            "Production would swap this for a daily rates dimension.",
    table_properties={"quality": "reference"},
    schema="""
        currency STRING COMMENT 'ISO 4217 currency code',
        usd_rate DOUBLE COMMENT 'Multiply an amount in this currency by this rate to get USD',
        as_of STRING COMMENT 'When these rates were pinned'
    """,
)
# FAIL expectations as structural guards: if the reference data itself is
# broken, nothing downstream can be trusted — stop the world.
@dp.expect_all_or_fail({
    "positive_rate": "usd_rate > 0",
    "iso_4217_shape": "length(currency) = 3",
})
def ref_fx_rates():
    rows = [(c, float(r), FX_AS_OF) for c, r in sorted(FX_TO_USD.items())]
    return spark.createDataFrame(rows, "currency string, usd_rate double, as_of string")


# ==========================================================================
# Step 1 — postings_typed: streaming, row-local conformance
# ==========================================================================
@dp.table(
    name=SILVER_POSTINGS_TYPED,
    comment="Internal conformance stage: bronze rows typed and mapped to the "
            "canonical vocabulary (logic: src/silver_transforms.typed_postings). "
            "Row-local only (safe under streaming); may hold multiple versions "
            "of one posting — postings_current collapses them. Query "
            "jobs_silver.postings instead of this table.",
    table_properties={"quality": "silver-internal"},
)
# THE FAIL EXPECTATION (the contract): every bronze row must identify itself
# (board + source id) and carry the export sequence timestamp. A violation
# means the export tool broke its contract — continuing would write postings
# that can never be deduplicated or updated, so the correct behaviour is to
# stop the update and roll back. Demo + recovery: notebook 03.
@dp.expect_all_or_fail({
    "source_identity_present":
        "board IS NOT NULL AND source_id IS NOT NULL AND exported_at IS NOT NULL",
})
# Warn-level expectations are free metrics: they gate nothing, but the event
# log records pass/fail counts per run — data-quality trendlines for free.
@dp.expect_all({
    "title_present": "title IS NOT NULL",
    "company_present": "company_name IS NOT NULL",
})
def postings_typed():
    # Read bronze AS A STREAM: only new bronze rows are processed per
    # update. Bronze is append-only (its contract) — that's what makes
    # streaming from a Delta table legal.
    return typed_postings(spark.readStream.table(BRONZE_POSTINGS))


# ==========================================================================
# Step 2 — postings_current: declarative latest-wins (auto CDC)
# ==========================================================================
# Re-exports put the same (board, source_id) into bronze repeatedly; this
# keeps exactly the newest version per key. create_auto_cdc_flow is the
# current name of the API (formerly apply_changes): the MERGE you'd
# hand-write, plus ordering and out-of-order handling, as a declaration.
# SCD type 1 = overwrite; bronze already preserves history.
dp.create_streaming_table(
    name=SILVER_POSTINGS_CURRENT,
    comment="Internal CDC stage: exactly one row per (board, source_id) — "
            "the newest exported version of each posting (SCD1, sequenced "
            "by exported_at). Cross-row enrichment happens in "
            "jobs_silver.postings; query that instead of this table.",
    table_properties={"quality": "silver-internal"},
    expect_all={"posting_key_present": "posting_key IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target=SILVER_POSTINGS_CURRENT,
    source=SILVER_POSTINGS_TYPED,
    keys=["board", "source_id"],
    sequence_by=F.col("exported_at"),
    stored_as_scd_type=1,
)


# ==========================================================================
# Step 3 — postings: the consumer-facing silver table
# ==========================================================================
@dp.materialized_view(
    name=SILVER_POSTINGS,
    comment="One row per live source posting across all seven boards, typed "
            "and conformed, with cross-board duplicate groups resolved. "
            "Rows sharing a dup_key describe the same real-world posting "
            "seen on multiple boards; is_canonical marks the group "
            "representative (earliest posted). Filter is_canonical for "
            "deduplicated analytics; keep all rows to study duplication "
            "itself. Annualised USD salaries live in salary_facts.",
    table_properties={"quality": "silver"},
    schema="""
        posting_key STRING COMMENT 'board:source_id — stable unique key',
        board STRING COMMENT 'Which career.* board exported the row',
        source_id BIGINT COMMENT 'Posting id in the source board database (unique per board only)',
        schema_gen STRING COMMENT 'Source schema generation (gen_a coffee / gen_b computer / gen_c clones)',
        title STRING COMMENT 'Posting title as scraped (mixed languages)',
        company_name STRING COMMENT 'Company display name from the source board',
        employment_type STRING COMMENT 'Canonical employment type (full_time/part_time/contract/temporary/internship/apprenticeship/freelance/seasonal/volunteer/other/unknown) conformed from 120+ raw spellings',
        employment_type_raw STRING COMMENT 'The raw source value (JobType string, or gen_a enum ordinal as text)',
        seniority STRING COMMENT 'Canonical seniority (internship/entry/mid/senior/lead/manager/director/executive/unknown)',
        seniority_source STRING COMMENT 'Where seniority came from: published field, title_parsed regex, or none',
        status STRING COMMENT 'Canonical posting status (active/paused/filled/expired/closed/draft/unknown)',
        is_remote BOOLEAN COMMENT 'Remote flag: source boolean OR remote keywords in the location text OR RemoteType=FullyRemote',
        is_hybrid BOOLEAN COMMENT 'Hybrid flag (gen_a publishes it; false elsewhere)',
        city STRING COMMENT 'City — source column where present (gen_a/b), else parsed from the free-text location',
        region STRING COMMENT 'State/region — source column or parsed (US state codes recognised)',
        country STRING COMMENT 'ISO2 country — source column conformed, else parsed; NULL means unknown, never guessed',
        location_raw STRING COMMENT 'The free-text location exactly as scraped',
        location_quality STRING COMMENT 'Parser verdict: parsed_full/parsed_partial/unparsed/empty — coverage is measurable, not assumed',
        salary_min_src DECIMAL(18,2) COMMENT 'Published lower salary bound in source currency (NULL unless > 0)',
        salary_max_src DECIMAL(18,2) COMMENT 'Published upper salary bound in source currency',
        salary_currency STRING COMMENT 'Effective currency after trust rules (gen_a USD default overridden by geography; believed only alongside amounts)',
        salary_currency_confidence STRING COMMENT 'published / inferred_geo / inferred_geo_over_default — how the currency was decided',
        salary_period STRING COMMENT 'Effective pay period (yearly/monthly/weekly/daily/hourly), published or magnitude-inferred',
        salary_period_confidence STRING COMMENT 'published / inferred_magnitude',
        source_url STRING COMMENT 'Public URL of the original posting at the source ATS',
        source_hash STRING COMMENT 'Source-system scrape hash (gen_b/c)',
        dup_key STRING COMMENT 'Duplicate-group key: normalised SourceUrl when available, else company+title+geo fingerprint',
        dup_group_size BIGINT COMMENT 'Number of postings sharing this dup_key (1 = no duplicate found)',
        is_canonical BOOLEAN COMMENT 'True for the group representative (earliest posted, board name as tiebreak)',
        is_cross_board_duplicate BOOLEAN COMMENT 'True when the dup group spans more than one board',
        posted_at TIMESTAMP COMMENT 'When the posting appeared (PostedAt, else CreatedAt)',
        updated_at TIMESTAMP COMMENT 'Last source-side update where tracked',
        last_seen_at TIMESTAMP COMMENT 'Last scraper re-confirmation (gen_a only)',
        expires_at TIMESTAMP COMMENT 'Source-side expiry',
        exported_at TIMESTAMP COMMENT 'Export batch timestamp this version came from'
    """,
)
@dp.expect_all({
    # Warn-level = coverage metrics with history in the event log.
    "posted_at_reasonable":
        "posted_at IS NULL OR (posted_at >= '2015-01-01' AND posted_at <= current_timestamp() + INTERVAL 7 DAYS)",
    "country_resolved": "country IS NOT NULL",
    "employment_conformed": "employment_type NOT IN ('other', 'unknown')",
})
def postings():
    return enrich_postings(spark.read.table(SILVER_POSTINGS_CURRENT))


# ==========================================================================
# posting_skills: three skill encodings -> one queryable table
# ==========================================================================
@dp.materialized_view(
    name=SILVER_POSTING_SKILLS,
    comment="One row per (posting, skill). Sources: gen_a stores a JSON "
            "array serialised INTO A STRING by its ORM (double-parsed "
            "here); gen_b/c tag tables were folded to real arrays at "
            "export; pet/delivery/church publish no skills at all — "
            "absence is honest, not imputed.",
    table_properties={"quality": "silver"},
    schema="""
        posting_key STRING COMMENT 'FK to jobs_silver.postings',
        board STRING COMMENT 'Board the posting came from',
        skill STRING COMMENT 'Normalised skill (lowercased, whitespace-collapsed) — the analytical key',
        skill_raw STRING COMMENT 'Skill exactly as published',
        skill_source STRING COMMENT 'required_skills_json (gen_a) or tags (gen_b/c)'
    """,
)
@dp.expect_all_or_drop({
    "skill_nonempty": "skill IS NOT NULL AND skill != ''",
    "posting_key_present": "posting_key IS NOT NULL",
})
@dp.expect_all({"skill_length_sane": "length(skill) <= 80"})
def posting_skills():
    return skills_from(spark.read.table(SILVER_POSTINGS_CURRENT))


# ==========================================================================
# salary_facts + salary_quarantine: annual USD with a visible reject path
# ==========================================================================
# One shared computation (a temporary view — a named subquery in the DAG,
# never stored), consumed twice: salary_facts keeps what survives its
# expectations, salary_quarantine MATERIALISES what they reject, with a
# reason. Drop-expectations give you counts; the quarantine gives you the
# actual rows to debug. Both, not either.
@dp.temporary_view(comment="Shared salary computation for facts + quarantine")
def salary_base():
    return salary_base_from(spark.read.table(SILVER_POSTINGS),
                            spark.read.table(SILVER_REF_FX))


_SALARY_COLS_DDL = """
    posting_key STRING COMMENT 'FK to jobs_silver.postings',
    board STRING COMMENT 'Board the posting came from',
    country STRING COMMENT 'ISO2 country of the posting (denormalised for banding)',
    seniority STRING COMMENT 'Canonical seniority (denormalised for banding)',
    employment_type STRING COMMENT 'Canonical employment type (denormalised)',
    is_canonical BOOLEAN COMMENT 'False for non-representative duplicate-group members — filter true to avoid double counting',
    salary_currency STRING COMMENT 'Effective source currency after trust rules',
    salary_currency_confidence STRING COMMENT 'published / inferred_geo / inferred_geo_over_default',
    salary_period STRING COMMENT 'Effective pay period',
    salary_period_confidence STRING COMMENT 'published / inferred_magnitude',
    salary_min_src DECIMAL(18,2) COMMENT 'Published lower bound, source currency',
    salary_max_src DECIMAL(18,2) COMMENT 'Published upper bound, source currency',
    usd_rate DOUBLE COMMENT 'Pinned FX rate applied (from ref_fx_rates)',
    salary_min_annual_usd DOUBLE COMMENT 'Lower bound annualised to USD',
    salary_max_annual_usd DOUBLE COMMENT 'Upper bound annualised to USD',
    salary_mid_annual_usd DOUBLE COMMENT 'Midpoint annualised to USD — the banding value',
    posted_at TIMESTAMP COMMENT 'Posting date (for salary trend lines)'
"""


@dp.materialized_view(
    name=SILVER_SALARY_FACTS,
    comment="One row per posting with a USABLE published salary, annualised "
            "to USD via the pinned FX table. Rows failing the plausibility "
            "or referential gates are dropped here and materialised in "
            "salary_quarantine with a reason. Only ~9% of postings publish "
            "amounts — salary coverage is itself a data-quality finding, "
            "reported in gold.",
    table_properties={"quality": "silver"},
    schema=_SALARY_COLS_DDL,
)
# THE DROP EXPECTATIONS: per-row garbage that must not poison the marts but
# must not stop the pipeline either. fx_rate_known is the REFERENTIAL check
# (the row's currency must exist in ref_fx_rates — enforced via the left
# join: no match, no rate). Every dropped row is visible in
# salary_quarantine.
@dp.expect_all_or_drop({
    "fx_rate_known": "usd_rate IS NOT NULL",
    "period_known": "salary_period IS NOT NULL",
    "annual_in_bounds":
        f"salary_mid_annual_usd BETWEEN {SALARY_ANNUAL_USD_MIN} AND {SALARY_ANNUAL_USD_MAX}",
})
@dp.expect_all({
    "bounds_ordered":
        "salary_min_annual_usd IS NULL OR salary_max_annual_usd IS NULL "
        "OR salary_min_annual_usd <= salary_max_annual_usd",
})
def salary_facts():
    return spark.read.table("salary_base").drop("reject_reason")


@dp.materialized_view(
    name=SILVER_SALARY_QUARANTINE,
    comment="The rows salary_facts rejected, kept queryable with a reason — "
            "the other half of the drop-expectation pattern. Drops without "
            "a quarantine are counts; drops with one are debuggable.",
    table_properties={"quality": "silver"},
    schema=_SALARY_COLS_DDL + ", reject_reason STRING COMMENT 'Why salary_facts refused this row'",
)
@dp.expect_all({"reason_present": "reject_reason IS NOT NULL"})
def salary_quarantine():
    return (spark.read.table("salary_base")
            .filter(F.col("reject_reason").isNotNull()))
